# Implementation Plan: Bi-directional SQL ↔ Parquet Transfer with Web UI

**Date**: 2025-10-02
**Status**: Planning
**Estimated Effort**: 10-13 hours

## Executive Summary

Extend the DataTransfer tool to support bi-directional transfers between SQL Server and Parquet files, and add a web-based user interface for configuration and execution.

**Current Capabilities:**
- SQL Server → Parquet → SQL Server (Parquet as intermediate storage)

**New Capabilities:**
- SQL Server → Parquet (export only)
- Parquet → SQL Server (import only)
- Web UI for all transfer types

## Current Architecture Analysis

### Existing Components

**Core Interfaces** (`src/DataTransfer.Core/Interfaces/`):
```csharp
// Extracts from SQL table to JSON stream
public interface ITableExtractor {
    Task<ExtractionResult> ExtractAsync(
        TableConfiguration tableConfig,
        string connectionString,
        Stream outputStream,
        CancellationToken cancellationToken = default);
}

// Loads JSON stream to SQL table
public interface IDataLoader {
    Task<LoadResult> LoadAsync(
        TableConfiguration tableConfig,
        string connectionString,
        Stream inputStream,
        CancellationToken cancellationToken = default);
}

// Read/Write Parquet files
public interface IParquetStorage {
    Task WriteAsync(Stream dataStream, string filePath, DateTime partitionDate, CancellationToken cancellationToken = default);
    Task<Stream> ReadAsync(string filePath, CancellationToken cancellationToken = default);
}
```

**Current Flow** (`src/DataTransfer.Pipeline/DataTransferOrchestrator.cs`):
```
1. Extract from SQL → JSON stream (ITableExtractor)
2. Write JSON → Parquet file (IParquetStorage.WriteAsync)
3. Read Parquet → JSON stream (IParquetStorage.ReadAsync)
4. Load JSON → SQL (IDataLoader)
```

**Key Limitation**: Both source and destination are assumed to be SQL tables. Parquet is only intermediate storage.

## Phase 1: Core Abstractions & Interfaces

### 1.1 New Interfaces

Create in `src/DataTransfer.Core/Interfaces/`:

**IParquetExtractor.cs**
```csharp
using DataTransfer.Core.Models;

namespace DataTransfer.Core.Interfaces;

/// <summary>
/// Extracts data from Parquet files to JSON stream
/// </summary>
public interface IParquetExtractor
{
    /// <summary>
    /// Extract data from a Parquet file to JSON format
    /// </summary>
    /// <param name="parquetPath">Relative or absolute path to Parquet file</param>
    /// <param name="outputStream">Stream to write JSON data</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Extraction result with row count</returns>
    Task<ExtractionResult> ExtractFromParquetAsync(
        string parquetPath,
        Stream outputStream,
        CancellationToken cancellationToken = default);
}
```

**IParquetWriter.cs**
```csharp
using DataTransfer.Core.Models;

namespace DataTransfer.Core.Interfaces;

/// <summary>
/// Writes data directly to Parquet files (simplified wrapper)
/// </summary>
public interface IParquetWriter
{
    /// <summary>
    /// Write JSON stream directly to Parquet file
    /// </summary>
    /// <param name="dataStream">JSON data stream</param>
    /// <param name="outputPath">Output file path</param>
    /// <param name="partitionDate">Optional partition date (null = no partitioning)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Number of rows written</returns>
    Task<int> WriteToParquetAsync(
        Stream dataStream,
        string outputPath,
        DateTime? partitionDate = null,
        CancellationToken cancellationToken = default);
}
```

### 1.2 Enhanced Models

Create in `src/DataTransfer.Core/Models/`:

**TransferType.cs**
```csharp
namespace DataTransfer.Core.Models;

public enum TransferType
{
    SqlToSql,        // Existing: SQL → Parquet → SQL
    SqlToParquet,    // New: SQL → Parquet (export)
    ParquetToSql     // New: Parquet → SQL (import)
}
```

**SourceType.cs & DestinationType.cs**
```csharp
namespace DataTransfer.Core.Models;

public enum SourceType
{
    SqlServer,
    Parquet
}

public enum DestinationType
{
    SqlServer,
    Parquet
}
```

**TransferConfiguration.cs** (enhanced)
```csharp
namespace DataTransfer.Core.Models;

public class TransferConfiguration
{
    public TransferType TransferType { get; set; }
    public SourceConfiguration Source { get; set; } = new();
    public DestinationConfiguration Destination { get; set; } = new();
    public PartitioningConfiguration? Partitioning { get; set; }
}

public class SourceConfiguration
{
    public SourceType Type { get; set; }

    // For SQL Server sources
    public string? ConnectionString { get; set; }
    public TableIdentifier? Table { get; set; }

    // For Parquet sources
    public string? ParquetPath { get; set; }
}

public class DestinationConfiguration
{
    public DestinationType Type { get; set; }

    // For SQL Server destinations
    public string? ConnectionString { get; set; }
    public TableIdentifier? Table { get; set; }

    // For Parquet destinations
    public string? ParquetPath { get; set; }
    public string? Compression { get; set; } = "Snappy";
}
```

## Phase 2: Implementation Classes

### 2.1 Parquet Extractor

Create `src/DataTransfer.Parquet/ParquetExtractor.cs`:

```csharp
using System.Text.Json;
using DataTransfer.Core.Interfaces;
using DataTransfer.Core.Models;
using Parquet;
using Parquet.Data;

namespace DataTransfer.Parquet;

public class ParquetExtractor : IParquetExtractor
{
    private readonly string _basePath;

    public ParquetExtractor(string basePath)
    {
        _basePath = basePath ?? throw new ArgumentNullException(nameof(basePath));
    }

    public async Task<ExtractionResult> ExtractFromParquetAsync(
        string parquetPath,
        Stream outputStream,
        CancellationToken cancellationToken = default)
    {
        var fullPath = Path.IsPathRooted(parquetPath)
            ? parquetPath
            : Path.Combine(_basePath, parquetPath);

        if (!File.Exists(fullPath))
        {
            throw new FileNotFoundException($"Parquet file not found: {fullPath}");
        }

        await using var fileStream = File.OpenRead(fullPath);
        using var parquetReader = await ParquetReader.CreateAsync(fileStream, cancellationToken: cancellationToken);

        var jsonRows = new List<Dictionary<string, object?>>();

        for (int groupIndex = 0; groupIndex < parquetReader.RowGroupCount; groupIndex++)
        {
            using var rowGroupReader = parquetReader.OpenRowGroupReader(groupIndex);
            var rowCount = (int)rowGroupReader.RowCount;

            var schema = parquetReader.Schema;
            var dataFields = schema.GetDataFields();

            // Read all columns
            var columnData = new Dictionary<string, Array>();
            foreach (var field in dataFields)
            {
                var column = await rowGroupReader.ReadColumnAsync(field, cancellationToken);
                columnData[field.Name] = column.Data;
            }

            // Convert to JSON rows
            for (int i = 0; i < rowCount; i++)
            {
                var row = new Dictionary<string, object?>();
                foreach (var field in dataFields)
                {
                    var data = columnData[field.Name];
                    row[field.Name] = data.GetValue(i);
                }
                jsonRows.Add(row);
            }
        }

        // Write JSON array to output stream
        await using var writer = new Utf8JsonWriter(outputStream, new JsonWriterOptions { Indented = false });
        writer.WriteStartArray();
        foreach (var row in jsonRows)
        {
            writer.WriteStartObject();
            foreach (var kvp in row)
            {
                writer.WritePropertyName(kvp.Key);
                JsonSerializer.Serialize(writer, kvp.Value);
            }
            writer.WriteEndObject();
        }
        writer.WriteEndArray();
        await writer.FlushAsync(cancellationToken);

        return new ExtractionResult
        {
            RowsExtracted = jsonRows.Count,
            Success = true
        };
    }
}
```

### 2.2 Parquet Writer (Simplified Wrapper)

Create `src/DataTransfer.Parquet/ParquetWriter.cs`:

```csharp
using DataTransfer.Core.Interfaces;

namespace DataTransfer.Parquet;

public class ParquetWriter : IParquetWriter
{
    private readonly IParquetStorage _storage;

    public ParquetWriter(IParquetStorage storage)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
    }

    public async Task<int> WriteToParquetAsync(
        Stream dataStream,
        string outputPath,
        DateTime? partitionDate = null,
        CancellationToken cancellationToken = default)
    {
        var fileName = Path.GetFileName(outputPath);
        var partition = partitionDate ?? DateTime.UtcNow;

        await _storage.WriteAsync(dataStream, fileName, partition, cancellationToken);

        // Read back to count rows (temporary solution)
        var partitionPath = $"year={partition.Year:D4}/month={partition.Month:D2}/day={partition.Day:D2}";
        var fullPath = $"{partitionPath}/{fileName}";

        await using var readStream = await _storage.ReadAsync(fullPath, cancellationToken);
        using var jsonDoc = await JsonDocument.ParseAsync(readStream, cancellationToken: cancellationToken);

        return jsonDoc.RootElement.GetArrayLength();
    }
}
```

### 2.3 Unified Transfer Orchestrator

Create `src/DataTransfer.Pipeline/UnifiedTransferOrchestrator.cs`:

```csharp
using DataTransfer.Core.Interfaces;
using DataTransfer.Core.Models;
using DataTransfer.Core.Strategies;
using Microsoft.Extensions.Logging;

namespace DataTransfer.Pipeline;

public class UnifiedTransferOrchestrator
{
    private readonly ITableExtractor _sqlExtractor;
    private readonly IParquetExtractor _parquetExtractor;
    private readonly IDataLoader _sqlLoader;
    private readonly IParquetWriter _parquetWriter;
    private readonly ILogger<UnifiedTransferOrchestrator> _logger;

    public UnifiedTransferOrchestrator(
        ITableExtractor sqlExtractor,
        IParquetExtractor parquetExtractor,
        IDataLoader sqlLoader,
        IParquetWriter parquetWriter,
        ILogger<UnifiedTransferOrchestrator> logger)
    {
        _sqlExtractor = sqlExtractor ?? throw new ArgumentNullException(nameof(sqlExtractor));
        _parquetExtractor = parquetExtractor ?? throw new ArgumentNullException(nameof(parquetExtractor));
        _sqlLoader = sqlLoader ?? throw new ArgumentNullException(nameof(sqlLoader));
        _parquetWriter = parquetWriter ?? throw new ArgumentNullException(nameof(parquetWriter));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<TransferResult> ExecuteTransferAsync(
        TransferConfiguration config,
        CancellationToken cancellationToken = default)
    {
        var result = new TransferResult { StartTime = DateTime.UtcNow };

        try
        {
            switch (config.TransferType)
            {
                case TransferType.SqlToParquet:
                    await TransferSqlToParquetAsync(config, result, cancellationToken);
                    break;

                case TransferType.ParquetToSql:
                    await TransferParquetToSqlAsync(config, result, cancellationToken);
                    break;

                case TransferType.SqlToSql:
                    await TransferSqlToSqlAsync(config, result, cancellationToken);
                    break;

                default:
                    throw new NotSupportedException($"Transfer type {config.TransferType} not supported");
            }

            result.Success = true;
            result.EndTime = DateTime.UtcNow;
            _logger.LogInformation("Transfer completed in {Duration}ms", result.Duration.TotalMilliseconds);

            return result;
        }
        catch (Exception ex)
        {
            result.Success = false;
            result.EndTime = DateTime.UtcNow;
            result.ErrorMessage = ex.Message;
            _logger.LogError(ex, "Transfer failed: {Error}", ex.Message);
            throw;
        }
    }

    private async Task TransferSqlToParquetAsync(
        TransferConfiguration config,
        TransferResult result,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Extracting from SQL Server to Parquet");

        using var dataStream = new MemoryStream();

        // Extract from SQL
        var tableConfig = new TableConfiguration
        {
            Source = config.Source.Table!,
            Partitioning = config.Partitioning ?? new PartitioningConfiguration { Type = PartitionType.Static }
        };

        var extractResult = await _sqlExtractor.ExtractAsync(
            tableConfig,
            config.Source.ConnectionString!,
            dataStream,
            cancellationToken);

        result.RowsExtracted = extractResult.RowsExtracted;

        // Write to Parquet
        dataStream.Position = 0;
        var rowsWritten = await _parquetWriter.WriteToParquetAsync(
            dataStream,
            config.Destination.ParquetPath!,
            DateTime.UtcNow,
            cancellationToken);

        result.RowsLoaded = rowsWritten;
        result.ParquetFilePath = config.Destination.ParquetPath;
    }

    private async Task TransferParquetToSqlAsync(
        TransferConfiguration config,
        TransferResult result,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Loading from Parquet to SQL Server");

        using var dataStream = new MemoryStream();

        // Extract from Parquet
        var extractResult = await _parquetExtractor.ExtractFromParquetAsync(
            config.Source.ParquetPath!,
            dataStream,
            cancellationToken);

        result.RowsExtracted = extractResult.RowsExtracted;
        result.ParquetFilePath = config.Source.ParquetPath;

        // Load to SQL
        dataStream.Position = 0;
        var tableConfig = new TableConfiguration
        {
            Destination = config.Destination.Table!
        };

        var loadResult = await _sqlLoader.LoadAsync(
            tableConfig,
            config.Destination.ConnectionString!,
            dataStream,
            cancellationToken);

        result.RowsLoaded = loadResult.RowsLoaded;
    }

    private async Task TransferSqlToSqlAsync(
        TransferConfiguration config,
        TransferResult result,
        CancellationToken cancellationToken)
    {
        // Use existing DataTransferOrchestrator logic
        _logger.LogInformation("SQL to SQL transfer (via Parquet intermediate)");
        // Implementation similar to existing orchestrator
        throw new NotImplementedException("Use existing DataTransferOrchestrator for SQL→SQL");
    }
}
```

### 2.4 Configuration Validator Updates

Update `src/DataTransfer.Configuration/ConfigurationValidator.cs`:

```csharp
public ValidationResult ValidateTransferConfiguration(TransferConfiguration config)
{
    var errors = new List<string>();

    switch (config.TransferType)
    {
        case TransferType.SqlToParquet:
            if (config.Source.Type != SourceType.SqlServer)
                errors.Add("Source must be SqlServer for SqlToParquet transfer");
            if (string.IsNullOrWhiteSpace(config.Source.ConnectionString))
                errors.Add("Source connection string is required");
            if (config.Source.Table == null)
                errors.Add("Source table is required");
            if (config.Destination.Type != DestinationType.Parquet)
                errors.Add("Destination must be Parquet for SqlToParquet transfer");
            if (string.IsNullOrWhiteSpace(config.Destination.ParquetPath))
                errors.Add("Destination Parquet path is required");
            break;

        case TransferType.ParquetToSql:
            if (config.Source.Type != SourceType.Parquet)
                errors.Add("Source must be Parquet for ParquetToSql transfer");
            if (string.IsNullOrWhiteSpace(config.Source.ParquetPath))
                errors.Add("Source Parquet path is required");
            if (config.Destination.Type != DestinationType.SqlServer)
                errors.Add("Destination must be SqlServer for ParquetToSql transfer");
            if (string.IsNullOrWhiteSpace(config.Destination.ConnectionString))
                errors.Add("Destination connection string is required");
            if (config.Destination.Table == null)
                errors.Add("Destination table is required");
            break;
    }

    return new ValidationResult
    {
        IsValid = errors.Count == 0,
        Errors = errors
    };
}
```

## Phase 3: Web UI (Blazor Server)

### 3.1 Create Web Project

```bash
cd src
dotnet new blazor -o DataTransfer.Web -f net8.0
cd DataTransfer.Web
dotnet add reference ../DataTransfer.Core
dotnet add reference ../DataTransfer.Configuration
dotnet add reference ../DataTransfer.Pipeline
dotnet add reference ../DataTransfer.SqlServer
dotnet add reference ../DataTransfer.Parquet
```

### 3.2 Project Structure

```
src/DataTransfer.Web/
├── Program.cs                    # Application entry point
├── Components/
│   ├── Layout/
│   │   ├── MainLayout.razor
│   │   └── NavMenu.razor
│   ├── Pages/
│   │   ├── Home.razor           # Dashboard
│   │   ├── NewTransfer.razor    # Transfer configuration form
│   │   ├── History.razor        # Transfer history list
│   │   └── Files.razor          # Parquet file browser
│   └── Shared/
│       ├── SourceSelector.razor
│       ├── DestinationSelector.razor
│       ├── TransferProgress.razor
│       └── ParquetFileCard.razor
├── Services/
│   ├── TransferExecutionService.cs
│   ├── TransferHistoryService.cs
│   └── ParquetMetadataService.cs
├── Models/
│   └── TransferHistoryEntry.cs
└── wwwroot/
    ├── css/
    └── js/
```

### 3.3 Program.cs Configuration

```csharp
using DataTransfer.Core.Interfaces;
using DataTransfer.Parquet;
using DataTransfer.Pipeline;
using DataTransfer.SqlServer;
using DataTransfer.Web.Services;

var builder = WebApplication.CreateBuilder(args);

// Add Blazor services
builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

// Add DataTransfer services
builder.Services.AddSingleton<SqlQueryBuilder>();
builder.Services.AddSingleton<ITableExtractor, SqlTableExtractor>();
builder.Services.AddSingleton<IDataLoader, SqlDataLoader>();
builder.Services.AddSingleton<IParquetStorage>(sp =>
    new ParquetStorage("./parquet-files"));
builder.Services.AddSingleton<IParquetExtractor>(sp =>
    new ParquetExtractor("./parquet-files"));
builder.Services.AddSingleton<IParquetWriter, ParquetWriter>();
builder.Services.AddSingleton<UnifiedTransferOrchestrator>();

// Web-specific services
builder.Services.AddSingleton<TransferExecutionService>();
builder.Services.AddSingleton<TransferHistoryService>();
builder.Services.AddSingleton<ParquetMetadataService>();

var app = builder.Build();

app.UseStaticFiles();
app.UseAntiforgery();

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();
```

### 3.4 Key Components

**NewTransfer.razor**
```razor
@page "/transfer/new"
@inject UnifiedTransferOrchestrator Orchestrator
@inject TransferExecutionService ExecutionService
@inject NavigationManager Navigation

<PageTitle>New Transfer</PageTitle>

<h3>Configure New Transfer</h3>

<EditForm Model="@_config" OnValidSubmit="@HandleSubmit">
    <DataAnnotationsValidator />
    <ValidationSummary />

    <div class="mb-3">
        <label>Transfer Type</label>
        <InputSelect @bind-Value="_config.TransferType" class="form-select">
            <option value="@TransferType.SqlToParquet">SQL Server → Parquet</option>
            <option value="@TransferType.ParquetToSql">Parquet → SQL Server</option>
            <option value="@TransferType.SqlToSql">SQL Server → SQL Server</option>
        </InputSelect>
    </div>

    <SourceSelector Config="@_config.Source" TransferType="@_config.TransferType" />
    <DestinationSelector Config="@_config.Destination" TransferType="@_config.TransferType" />

    @if (_config.TransferType == TransferType.SqlToParquet)
    {
        <div class="mb-3">
            <label>Partition Strategy</label>
            <InputSelect @bind-Value="_partitionType" class="form-select">
                <option value="@PartitionType.Static">No Partitioning</option>
                <option value="@PartitionType.Date">Date</option>
                <option value="@PartitionType.IntDate">Integer Date</option>
            </InputSelect>
        </div>
    }

    <button type="submit" class="btn btn-primary" disabled="@_isProcessing">
        @if (_isProcessing)
        {
            <span class="spinner-border spinner-border-sm me-2"></span>
        }
        Execute Transfer
    </button>
</EditForm>

@if (_isProcessing)
{
    <TransferProgress TransferId="@_currentTransferId" />
}

@code {
    private TransferConfiguration _config = new();
    private PartitionType _partitionType = PartitionType.Static;
    private bool _isProcessing;
    private string? _currentTransferId;

    private async Task HandleSubmit()
    {
        _isProcessing = true;
        _currentTransferId = Guid.NewGuid().ToString();

        try
        {
            _config.Partitioning = new PartitioningConfiguration { Type = _partitionType };

            var result = await ExecutionService.ExecuteAsync(_config, _currentTransferId);

            if (result.Success)
            {
                Navigation.NavigateTo("/history");
            }
        }
        finally
        {
            _isProcessing = false;
        }
    }
}
```

**SourceSelector.razor**
```razor
@if (TransferType == TransferType.SqlToParquet || TransferType == TransferType.SqlToSql)
{
    <div class="card mb-3">
        <div class="card-header">SQL Server Source</div>
        <div class="card-body">
            <div class="mb-3">
                <label>Connection String</label>
                <InputText @bind-Value="Config.ConnectionString" class="form-control" />
            </div>
            <div class="row">
                <div class="col-md-4">
                    <label>Database</label>
                    <InputText @bind-Value="Config.Table.Database" class="form-control" />
                </div>
                <div class="col-md-4">
                    <label>Schema</label>
                    <InputText @bind-Value="Config.Table.Schema" class="form-control" />
                </div>
                <div class="col-md-4">
                    <label>Table</label>
                    <InputText @bind-Value="Config.Table.Table" class="form-control" />
                </div>
            </div>
        </div>
    </div>
}
else if (TransferType == TransferType.ParquetToSql)
{
    <div class="card mb-3">
        <div class="card-header">Parquet Source</div>
        <div class="card-body">
            <div class="mb-3">
                <label>Parquet File Path</label>
                <InputText @bind-Value="Config.ParquetPath" class="form-control"
                    placeholder="e.g., ./exports/data.parquet" />
            </div>
            <button type="button" class="btn btn-secondary btn-sm" @onclick="BrowseFiles">
                Browse Files
            </button>
        </div>
    </div>
}

@code {
    [Parameter] public SourceConfiguration Config { get; set; } = new();
    [Parameter] public TransferType TransferType { get; set; }

    private void BrowseFiles()
    {
        // Open file browser modal
    }
}
```

### 3.5 Services

**TransferExecutionService.cs**
```csharp
public class TransferExecutionService
{
    private readonly UnifiedTransferOrchestrator _orchestrator;
    private readonly TransferHistoryService _history;
    private readonly ILogger<TransferExecutionService> _logger;

    public async Task<TransferResult> ExecuteAsync(
        TransferConfiguration config,
        string transferId,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Starting transfer {TransferId}", transferId);

        var result = await _orchestrator.ExecuteTransferAsync(config, cancellationToken);

        await _history.AddAsync(new TransferHistoryEntry
        {
            Id = transferId,
            TransferType = config.TransferType,
            StartTime = result.StartTime,
            EndTime = result.EndTime,
            RowsTransferred = result.RowsLoaded,
            Success = result.Success,
            ErrorMessage = result.ErrorMessage
        });

        return result;
    }
}
```

**TransferHistoryService.cs**
```csharp
public class TransferHistoryService
{
    private readonly List<TransferHistoryEntry> _history = new();
    private readonly SemaphoreSlim _lock = new(1, 1);

    public async Task AddAsync(TransferHistoryEntry entry)
    {
        await _lock.WaitAsync();
        try
        {
            _history.Add(entry);
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task<List<TransferHistoryEntry>> GetAllAsync()
    {
        await _lock.WaitAsync();
        try
        {
            return _history.OrderByDescending(h => h.StartTime).ToList();
        }
        finally
        {
            _lock.Release();
        }
    }
}

public class TransferHistoryEntry
{
    public string Id { get; set; } = string.Empty;
    public TransferType TransferType { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime? EndTime { get; set; }
    public long RowsTransferred { get; set; }
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
    public TimeSpan Duration => EndTime.HasValue ? EndTime.Value - StartTime : TimeSpan.Zero;
}
```

## Phase 4: Testing Strategy

### 4.1 Unit Tests

**ParquetExtractorTests.cs**
```csharp
[Fact]
public async Task ExtractFromParquetAsync_ValidFile_ReturnsJsonStream()
{
    // Arrange
    var testParquetPath = "./test-data/sample.parquet";
    var extractor = new ParquetExtractor("./");
    var outputStream = new MemoryStream();

    // Act
    var result = await extractor.ExtractFromParquetAsync(testParquetPath, outputStream);

    // Assert
    Assert.True(result.Success);
    Assert.True(result.RowsExtracted > 0);

    outputStream.Position = 0;
    var json = await JsonDocument.ParseAsync(outputStream);
    Assert.Equal(JsonValueKind.Array, json.RootElement.ValueKind);
}

[Fact]
public async Task ExtractFromParquetAsync_FileNotFound_ThrowsException()
{
    // Arrange
    var extractor = new ParquetExtractor("./");
    var outputStream = new MemoryStream();

    // Act & Assert
    await Assert.ThrowsAsync<FileNotFoundException>(
        () => extractor.ExtractFromParquetAsync("nonexistent.parquet", outputStream));
}
```

**UnifiedOrchestratorTests.cs**
```csharp
[Fact]
public async Task ExecuteTransfer_SqlToParquet_Success()
{
    // Arrange
    var config = new TransferConfiguration
    {
        TransferType = TransferType.SqlToParquet,
        Source = new SourceConfiguration
        {
            Type = SourceType.SqlServer,
            ConnectionString = _testConnectionString,
            Table = new TableIdentifier { Database = "Test", Schema = "dbo", Table = "Orders" }
        },
        Destination = new DestinationConfiguration
        {
            Type = DestinationType.Parquet,
            ParquetPath = "./test-output.parquet"
        }
    };

    // Act
    var result = await _orchestrator.ExecuteTransferAsync(config);

    // Assert
    Assert.True(result.Success);
    Assert.True(File.Exists("./test-output.parquet"));
}
```

### 4.2 Integration Tests

Create `tests/DataTransfer.Integration.Tests/BidirectionalTransferTests.cs`:

```csharp
public class BidirectionalTransferTests : IAsyncLifetime
{
    private MsSqlContainer _sqlContainer;
    private string _connectionString;

    [Fact]
    public async Task RoundTrip_SqlToParquetToSql_DataIntegrity()
    {
        // 1. Create source table with data
        await CreateTestTableAsync("SourceTable", 100);

        // 2. SQL → Parquet
        var sqlToParquetConfig = new TransferConfiguration
        {
            TransferType = TransferType.SqlToParquet,
            Source = new SourceConfiguration
            {
                Type = SourceType.SqlServer,
                ConnectionString = _connectionString,
                Table = new TableIdentifier { Database = "master", Schema = "dbo", Table = "SourceTable" }
            },
            Destination = new DestinationConfiguration
            {
                Type = DestinationType.Parquet,
                ParquetPath = "./roundtrip.parquet"
            }
        };

        var exportResult = await _orchestrator.ExecuteTransferAsync(sqlToParquetConfig);
        Assert.True(exportResult.Success);
        Assert.Equal(100, exportResult.RowsExtracted);

        // 3. Parquet → SQL
        await CreateTestTableAsync("DestinationTable", 0);

        var parquetToSqlConfig = new TransferConfiguration
        {
            TransferType = TransferType.ParquetToSql,
            Source = new SourceConfiguration
            {
                Type = SourceType.Parquet,
                ParquetPath = "./roundtrip.parquet"
            },
            Destination = new DestinationConfiguration
            {
                Type = DestinationType.SqlServer,
                ConnectionString = _connectionString,
                Table = new TableIdentifier { Database = "master", Schema = "dbo", Table = "DestinationTable" }
            }
        };

        var importResult = await _orchestrator.ExecuteTransferAsync(parquetToSqlConfig);
        Assert.True(importResult.Success);
        Assert.Equal(100, importResult.RowsLoaded);

        // 4. Verify data integrity
        var destCount = await GetRowCountAsync("DestinationTable");
        Assert.Equal(100, destCount);
    }
}
```

## Phase 5: Configuration Examples

### SQL to Parquet Export
```json
{
  "TransferType": "SqlToParquet",
  "Source": {
    "Type": "SqlServer",
    "ConnectionString": "Server=localhost;Database=Sales;User Id=sa;Password=***;TrustServerCertificate=true",
    "Table": {
      "Database": "Sales",
      "Schema": "dbo",
      "Table": "Orders"
    }
  },
  "Destination": {
    "Type": "Parquet",
    "ParquetPath": "./exports/orders_2024.parquet",
    "Compression": "Snappy"
  },
  "Partitioning": {
    "Type": "Date",
    "Column": "OrderDate"
  }
}
```

### Parquet to SQL Import
```json
{
  "TransferType": "ParquetToSql",
  "Source": {
    "Type": "Parquet",
    "ParquetPath": "./exports/orders_2024.parquet"
  },
  "Destination": {
    "Type": "SqlServer",
    "ConnectionString": "Server=localhost;Database=Archive;User Id=sa;Password=***;TrustServerCertificate=true",
    "Table": {
      "Database": "Archive",
      "Schema": "history",
      "Table": "OrdersArchive"
    }
  }
}
```

## Phase 6: Documentation Updates

### README.md additions

```markdown
## Bi-directional Transfer Support

The tool now supports three transfer types:

1. **SQL Server → Parquet** - Export SQL data to Parquet files
2. **Parquet → SQL Server** - Import Parquet files to SQL tables
3. **SQL Server → SQL Server** - Migrate data between servers (via Parquet)

### Web UI

A web-based UI is available for configuring and executing transfers:

```bash
cd src/DataTransfer.Web
dotnet run
```

Navigate to `http://localhost:5000` to access the UI.

### CLI Usage

Export to Parquet:
```bash
dotnet run --project src/DataTransfer.Console -- \
  --config export-config.json \
  --type SqlToParquet
```

Import from Parquet:
```bash
dotnet run --project src/DataTransfer.Console -- \
  --config import-config.json \
  --type ParquetToSql
```
```

## Implementation Checklist

### Phase 1: Core (TDD)
- [ ] Create `IParquetExtractor` interface with tests
- [ ] Create `IParquetWriter` interface with tests
- [ ] Add `TransferType` enum
- [ ] Create enhanced `TransferConfiguration` model
- [ ] Update configuration validator with tests

### Phase 2: Implementation (TDD)
- [ ] Implement `ParquetExtractor` (RED → GREEN → REFACTOR)
- [ ] Implement `ParquetWriter` (RED → GREEN → REFACTOR)
- [ ] Implement `UnifiedTransferOrchestrator` (RED → GREEN → REFACTOR)
- [ ] Add unit tests for all new classes

### Phase 3: Web UI
- [ ] Create Blazor project structure
- [ ] Implement `NewTransfer.razor` page
- [ ] Implement `SourceSelector` component
- [ ] Implement `DestinationSelector` component
- [ ] Implement `History.razor` page
- [ ] Implement `Files.razor` (Parquet browser)
- [ ] Create `TransferExecutionService`
- [ ] Create `TransferHistoryService`
- [ ] Create `ParquetMetadataService`

### Phase 4: Testing
- [ ] Unit tests for `ParquetExtractor`
- [ ] Unit tests for `UnifiedOrchestrator`
- [ ] Integration test: SQL → Parquet
- [ ] Integration test: Parquet → SQL
- [ ] Integration test: Round-trip (SQL → Parquet → SQL)
- [ ] Web UI manual testing

### Phase 5: Documentation
- [ ] Update README.md with bi-directional examples
- [ ] Create WEB_UI_GUIDE.md
- [ ] Update ARCHITECTURE.md
- [ ] Add configuration examples
- [ ] Update demo with Parquet samples

### Phase 6: Deployment
- [ ] Update Docker configuration for web UI
- [ ] Add docker-compose.yml for full stack
- [ ] Update CI/CD pipeline
- [ ] Create release notes

## Technology Stack Summary

- **.NET 8** - Framework
- **Blazor Server** - Web UI
- **Parquet.NET** - Parquet file handling
- **Bootstrap 5** - UI styling
- **xUnit** - Testing
- **Testcontainers** - Integration tests
- **Serilog** - Logging

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Schema inference errors | High | Extensive testing with various Parquet schemas |
| Large file handling | Medium | Implement streaming, add progress reporting |
| Type mapping SQL↔Parquet | High | Create comprehensive type mapping table |
| Web UI security | High | Add authentication in next phase |
| State management | Medium | Start with in-memory, plan SQLite persistence |

## Success Criteria

- [ ] Can export SQL table to Parquet file
- [ ] Can import Parquet file to SQL table
- [ ] Round-trip maintains data integrity
- [ ] Web UI allows configuration without coding
- [ ] All tests pass (unit + integration)
- [ ] Documentation is complete
- [ ] Demo shows all transfer types

## Next Steps After Implementation

1. Add authentication to Web UI
2. Implement real-time progress with SignalR
3. Add support for multiple Parquet file formats
4. Cloud storage integration (Azure Blob, S3)
5. Batch processing multiple files
6. Scheduling and automation features
