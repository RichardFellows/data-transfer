using DataTransfer.Configuration;
using DataTransfer.Configuration.Models;
using DataTransfer.Configuration.Services;
using DataTransfer.Core.Interfaces;
using DataTransfer.Core.Models;
using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.ChangeDetection;
using DataTransfer.Iceberg.Integration;
using DataTransfer.Iceberg.Readers;
using DataTransfer.Iceberg.Watermarks;
using DataTransfer.Iceberg.Writers;
using DataTransfer.Parquet;
using DataTransfer.Pipeline;
using DataTransfer.SqlServer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;

// Configure Serilog
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .WriteTo.Console()
    .WriteTo.File("logs/datatransfer-.txt", rollingInterval: RollingInterval.Day)
    .CreateLogger();

try
{
    Log.Information("DataTransfer Console Application Starting");

    var host = CreateHost(args);
    var logger = host.Services.GetRequiredService<ILogger<Program>>();

    // Determine mode based on arguments
    if (args.Length == 0)
    {
        // Interactive mode
        logger.LogInformation("Starting interactive mode");
        return await RunInteractiveModeAsync(host.Services);
    }
    else
    {
        // Command-line mode
        logger.LogInformation("Starting command-line mode");
        return await RunCommandLineModeAsync(args, host.Services);
    }
}
catch (Exception ex)
{
    Log.Fatal(ex, "Fatal error: {Message}", ex.Message);
    return 1;
}
finally
{
    Log.CloseAndFlush();
}

static IHost CreateHost(string[] args)
{
    var builder = Host.CreateDefaultBuilder(args);

    builder.ConfigureServices((context, services) =>
    {
        services.AddLogging(loggingBuilder =>
            loggingBuilder.AddSerilog(dispose: true));

        // Core services
        services.AddSingleton<SqlQueryBuilder>();
        services.AddSingleton<ITableExtractor, SqlTableExtractor>();
        services.AddSingleton<IDataLoader, SqlDataLoader>();
        services.AddSingleton<IParquetStorage>(sp =>
            new ParquetStorage("parquet-output"));
        services.AddSingleton<IParquetExtractor>(sp =>
            new ParquetExtractor("parquet-output"));
        services.AddSingleton<IParquetWriter, ParquetWriter>();

        // Orchestrators (both old and new)
        services.AddSingleton<DataTransferOrchestrator>();
        services.AddSingleton<UnifiedTransferOrchestrator>();

        // Iceberg services
        var icebergWarehousePath = context.Configuration["Iceberg:WarehousePath"] ?? "./iceberg-warehouse";
        services.AddSingleton<FilesystemCatalog>(sp =>
            new FilesystemCatalog(
                icebergWarehousePath,
                sp.GetRequiredService<ILogger<FilesystemCatalog>>()));

        services.AddSingleton<SqlServerToIcebergExporter>();
        services.AddSingleton<IcebergTableWriter>();
        services.AddSingleton<IcebergParquetWriter>();
        services.AddSingleton<IcebergReader>();
        services.AddSingleton<IcebergAppender>();
        services.AddSingleton<SqlServerImporter>();

        // Incremental sync services
        services.AddSingleton<IWatermarkStore>(sp =>
            new FileWatermarkStore(Path.Combine(icebergWarehousePath, ".watermarks")));
        services.AddSingleton<IChangeDetectionStrategy, TimestampChangeDetection>();
        services.AddSingleton<IncrementalSyncCoordinator>();

        // Configuration services
        services.AddSingleton<ConfigurationLoader>();
        services.AddSingleton<ConfigurationValidator>();

        // Profile service
        var profilesDir = context.Configuration["Profiles:StorageDirectory"] ?? "./profiles";
        services.AddSingleton<TransferProfileService>(sp =>
            new TransferProfileService(
                sp.GetRequiredService<ILogger<TransferProfileService>>(),
                profilesDir));
    });

    return builder.Build();
}

static async Task<int> RunInteractiveModeAsync(IServiceProvider services)
{
    var profileService = services.GetRequiredService<TransferProfileService>();
    var orchestrator = services.GetRequiredService<UnifiedTransferOrchestrator>();
    var logger = services.GetRequiredService<ILogger<Program>>();

    while (true)
    {
        Console.WriteLine();
        Console.WriteLine("╔════════════════════════════════════════════╗");
        Console.WriteLine("║   🔄 DataTransfer Console                 ║");
        Console.WriteLine("╚════════════════════════════════════════════╝");
        Console.WriteLine();
        Console.WriteLine("1. Run from config file (legacy)");
        Console.WriteLine("2. Load saved profile");
        Console.WriteLine("3. List all profiles");
        Console.WriteLine("4. Exit");
        Console.WriteLine();
        Console.Write("Select option (1-4): ");

        var choice = Console.ReadLine();

        switch (choice)
        {
            case "1":
                await RunFromConfigFileAsync(services);
                break;
            case "2":
                await RunFromProfileAsync(profileService, orchestrator, logger);
                break;
            case "3":
                await ListProfilesAsync(profileService);
                break;
            case "4":
                Console.WriteLine("Exiting...");
                return 0;
            default:
                Console.WriteLine("Invalid option. Please try again.");
                break;
        }

        Console.WriteLine();
        Console.WriteLine("Press any key to continue...");
        Console.ReadKey();
    }
}

static async Task RunFromConfigFileAsync(IServiceProvider services)
{
    var configLoader = services.GetRequiredService<ConfigurationLoader>();
    var validator = services.GetRequiredService<ConfigurationValidator>();
    var orchestrator = services.GetRequiredService<DataTransferOrchestrator>();
    var logger = services.GetRequiredService<ILogger<Program>>();

    Console.Write("Enter config file path (default: config/appsettings.json): ");
    var configPath = Console.ReadLine();
    if (string.IsNullOrWhiteSpace(configPath))
    {
        configPath = "config/appsettings.json";
    }

    try
    {
        logger.LogInformation("Loading configuration from {ConfigPath}", configPath);
        var config = await configLoader.LoadAsync(configPath);

        // Validate configuration
        var validationResult = validator.Validate(config);
        if (!validationResult.IsValid)
        {
            Console.WriteLine("❌ Configuration validation failed:");
            foreach (var error in validationResult.Errors)
            {
                Console.WriteLine($"   - {error}");
            }
            return;
        }

        Console.WriteLine($"✓ Configuration loaded: {config.Tables.Count} tables");
        Console.WriteLine("Press ENTER to execute or Ctrl+C to cancel...");
        Console.ReadLine();

        int successCount = 0;
        int failureCount = 0;
        long totalRowsExtracted = 0;
        long totalRowsLoaded = 0;

        foreach (var tableConfig in config.Tables)
        {
            try
            {
                Console.WriteLine($"Processing {tableConfig.Source.FullyQualifiedName}...");

                var result = await orchestrator.TransferTableAsync(
                    tableConfig,
                    config.Connections.Source,
                    config.Connections.Destination);

                if (result.Success)
                {
                    successCount++;
                    totalRowsExtracted += result.RowsExtracted;
                    totalRowsLoaded += result.RowsLoaded;
                    Console.WriteLine($"✓ Completed: {result.RowsLoaded:N0} rows in {result.Duration.TotalSeconds:F2}s");
                }
                else
                {
                    failureCount++;
                    Console.WriteLine($"✗ Failed: {result.ErrorMessage}");
                }
            }
            catch (Exception ex)
            {
                failureCount++;
                Console.WriteLine($"✗ Exception: {ex.Message}");
            }
        }

        Console.WriteLine();
        Console.WriteLine("=====================================");
        Console.WriteLine("Transfer Summary:");
        Console.WriteLine($"  Success: {successCount}");
        Console.WriteLine($"  Failed: {failureCount}");
        Console.WriteLine($"  Total Rows: {totalRowsLoaded:N0}");
        Console.WriteLine("=====================================");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"❌ Error: {ex.Message}");
        logger.LogError(ex, "Failed to process config file");
    }
}

static async Task RunFromProfileAsync(
    TransferProfileService profileService,
    UnifiedTransferOrchestrator orchestrator,
    Microsoft.Extensions.Logging.ILogger logger)
{
    var profiles = await profileService.GetAllProfilesAsync();

    if (!profiles.Any())
    {
        Console.WriteLine("❌ No profiles found. Create one using the Web UI first.");
        return;
    }

    Console.WriteLine();
    Console.WriteLine("Available Profiles:");
    Console.WriteLine("===================");
    for (int i = 0; i < profiles.Count; i++)
    {
        var p = profiles[i];
        Console.WriteLine($"{i + 1}. {p.ProfileName}");
        Console.WriteLine($"   Type: {p.Configuration.TransferType}");
        if (!string.IsNullOrEmpty(p.Description))
        {
            Console.WriteLine($"   Description: {p.Description}");
        }
        if (p.Tags.Any())
        {
            Console.WriteLine($"   Tags: {string.Join(", ", p.Tags)}");
        }
        Console.WriteLine();
    }

    Console.Write($"Select profile (1-{profiles.Count}): ");
    if (int.TryParse(Console.ReadLine(), out int selection) &&
        selection > 0 && selection <= profiles.Count)
    {
        var profile = profiles[selection - 1];
        Console.WriteLine();
        Console.WriteLine($"✓ Loaded: {profile.ProfileName}");
        Console.WriteLine($"  Type: {profile.Configuration.TransferType}");

        // Show configuration details
        if (profile.Configuration.TransferType == TransferType.SqlToParquet)
        {
            var table = profile.Configuration.Source.Table;
            Console.WriteLine($"  Source: {table?.Database}.{table?.Schema}.{table?.Table}");
            Console.WriteLine($"  Destination: {profile.Configuration.Destination.ParquetPath}");
        }
        else if (profile.Configuration.TransferType == TransferType.ParquetToSql)
        {
            var table = profile.Configuration.Destination.Table;
            Console.WriteLine($"  Source: {profile.Configuration.Source.ParquetPath}");
            Console.WriteLine($"  Destination: {table?.Database}.{table?.Schema}.{table?.Table}");
        }

        Console.WriteLine();
        Console.WriteLine("Press ENTER to execute or Ctrl+C to cancel...");
        Console.ReadLine();

        try
        {
            var transferId = $"console-{Guid.NewGuid():N}";
            logger.LogInformation("Executing profile: {ProfileName} ({TransferId})", profile.ProfileName, transferId);

            var result = await orchestrator.ExecuteTransferAsync(
                profile.Configuration,
                CancellationToken.None);

            if (result.Success)
            {
                Console.WriteLine();
                Console.WriteLine("✓ Transfer completed successfully!");
                Console.WriteLine($"  Rows transferred: {result.RowsLoaded:N0}");
                Console.WriteLine($"  Duration: {result.Duration.TotalSeconds:F2} seconds");
                Console.WriteLine($"  Throughput: {result.RowsLoaded / result.Duration.TotalSeconds:N0} rows/sec");
            }
            else
            {
                Console.WriteLine();
                Console.WriteLine($"✗ Transfer failed: {result.ErrorMessage}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Error executing transfer: {ex.Message}");
            logger.LogError(ex, "Failed to execute profile: {ProfileName}", profile.ProfileName);
        }
    }
    else
    {
        Console.WriteLine("Invalid selection.");
    }
}

static async Task ListProfilesAsync(TransferProfileService profileService)
{
    var profiles = await profileService.GetAllProfilesAsync();

    if (!profiles.Any())
    {
        Console.WriteLine("No profiles found.");
        return;
    }

    Console.WriteLine();
    Console.WriteLine("Saved Profiles:");
    Console.WriteLine("===============");
    foreach (var profile in profiles)
    {
        Console.WriteLine();
        Console.WriteLine($"📋 {profile.ProfileName}");
        Console.WriteLine($"   ID: {profile.ProfileId}");
        Console.WriteLine($"   Type: {profile.Configuration.TransferType}");
        if (!string.IsNullOrEmpty(profile.Description))
        {
            Console.WriteLine($"   Description: {profile.Description}");
        }
        Console.WriteLine($"   Created: {profile.CreatedDate:yyyy-MM-dd HH:mm} by {profile.CreatedBy}");
        if (profile.Tags.Any())
        {
            Console.WriteLine($"   Tags: {string.Join(", ", profile.Tags)}");
        }
    }
}

static async Task<int> RunCommandLineModeAsync(string[] args, IServiceProvider services)
{
    var logger = services.GetRequiredService<ILogger<Program>>();
    var profileService = services.GetRequiredService<TransferProfileService>();
    var orchestrator = services.GetRequiredService<UnifiedTransferOrchestrator>();

    // Parse arguments
    string? profileName = null;
    string? configPath = null;
    string? environmentName = null;
    string? discoverConnectionString = null;
    string? discoverTable = null;
    bool listProfiles = false;

    // Iceberg command arguments
    string? exportIcebergConn = null;
    string? exportIcebergTable = null;
    string? exportIcebergName = null;
    string? importIcebergTable = null;
    string? importIcebergConn = null;
    string? importIcebergDestTable = null;
    string? syncSourceConn = null;
    string? syncSourceTable = null;
    string? syncIcebergTable = null;
    string? syncTargetConn = null;
    string? syncTargetTable = null;
    string? syncPrimaryKey = null;
    string? syncWatermark = null;
    string? syncMergeStrategy = "upsert";

    for (int i = 0; i < args.Length; i++)
    {
        switch (args[i])
        {
            case "--profile":
                if (i + 1 < args.Length)
                {
                    profileName = args[i + 1];
                    i++;
                }
                break;
            case "--config":
                if (i + 1 < args.Length)
                {
                    configPath = args[i + 1];
                    i++;
                }
                break;
            case "--environment":
                if (i + 1 < args.Length)
                {
                    environmentName = args[i + 1];
                    i++;
                }
                break;
            case "--discover":
                if (i + 1 < args.Length)
                {
                    discoverConnectionString = args[i + 1];
                    i++;
                }
                break;
            case "--table":
                if (i + 1 < args.Length)
                {
                    discoverTable = args[i + 1];
                    i++;
                }
                break;
            case "--list-profiles":
                listProfiles = true;
                break;
            case "--export-iceberg":
                if (i + 2 < args.Length)
                {
                    exportIcebergConn = args[i + 1];
                    exportIcebergTable = args[i + 2];
                    i += 2;
                }
                break;
            case "--iceberg-name":
                if (i + 1 < args.Length)
                {
                    exportIcebergName = args[i + 1];
                    i++;
                }
                break;
            case "--import-iceberg":
                if (i + 3 < args.Length)
                {
                    importIcebergTable = args[i + 1];
                    importIcebergConn = args[i + 2];
                    importIcebergDestTable = args[i + 3];
                    i += 3;
                }
                break;
            case "--sync-iceberg":
                if (i + 5 < args.Length)
                {
                    syncSourceConn = args[i + 1];
                    syncSourceTable = args[i + 2];
                    syncIcebergTable = args[i + 3];
                    syncTargetConn = args[i + 4];
                    syncTargetTable = args[i + 5];
                    i += 5;
                }
                break;
            case "--primary-key":
                if (i + 1 < args.Length)
                {
                    syncPrimaryKey = args[i + 1];
                    i++;
                }
                break;
            case "--watermark":
                if (i + 1 < args.Length)
                {
                    syncWatermark = args[i + 1];
                    i++;
                }
                break;
            case "--merge-strategy":
                if (i + 1 < args.Length)
                {
                    syncMergeStrategy = args[i + 1];
                    i++;
                }
                break;
            case "--help":
                ShowHelp();
                return 0;
        }
    }

    if (listProfiles)
    {
        await ListProfilesAsync(profileService);
        return 0;
    }

    if (!string.IsNullOrEmpty(discoverConnectionString))
    {
        return await RunSchemaDiscoveryAsync(discoverConnectionString, discoverTable, logger);
    }

    // Iceberg commands
    if (!string.IsNullOrEmpty(exportIcebergConn) && !string.IsNullOrEmpty(exportIcebergTable))
    {
        return await RunExportIcebergAsync(exportIcebergConn, exportIcebergTable, exportIcebergName, orchestrator, logger);
    }

    if (!string.IsNullOrEmpty(importIcebergTable) && !string.IsNullOrEmpty(importIcebergConn) && !string.IsNullOrEmpty(importIcebergDestTable))
    {
        return await RunImportIcebergAsync(importIcebergTable, importIcebergConn, importIcebergDestTable, orchestrator, logger);
    }

    if (!string.IsNullOrEmpty(syncSourceConn) && !string.IsNullOrEmpty(syncSourceTable) &&
        !string.IsNullOrEmpty(syncIcebergTable) && !string.IsNullOrEmpty(syncTargetConn) &&
        !string.IsNullOrEmpty(syncTargetTable))
    {
        if (string.IsNullOrEmpty(syncPrimaryKey) || string.IsNullOrEmpty(syncWatermark))
        {
            logger.LogError("--sync-iceberg requires --primary-key and --watermark parameters");
            return 1;
        }
        return await RunSyncIcebergAsync(
            syncSourceConn, syncSourceTable, syncIcebergTable, syncTargetConn, syncTargetTable,
            syncPrimaryKey, syncWatermark, syncMergeStrategy ?? "upsert", orchestrator, logger);
    }

    if (!string.IsNullOrEmpty(profileName))
    {
        // Run from profile
        var profiles = await profileService.GetAllProfilesAsync();
        var profile = profiles.FirstOrDefault(p => p.ProfileName == profileName);

        if (profile == null)
        {
            logger.LogError("Profile '{ProfileName}' not found", profileName);
            return 1;
        }

        logger.LogInformation("Executing profile: {ProfileName}", profileName);

        // Apply environment settings if specified
        var envManager = await LoadEnvironmentAsync(environmentName, logger);
        if (envManager != null && !string.IsNullOrEmpty(environmentName))
        {
            var environment = envManager.GetEnvironment(environmentName);
            ApplyEnvironmentToConfiguration(profile.Configuration, environment, envManager);
        }

        var result = await orchestrator.ExecuteTransferAsync(profile.Configuration, CancellationToken.None);

        if (result.Success)
        {
            logger.LogInformation("✓ Transfer completed: {Rows} rows in {Duration}s",
                result.RowsLoaded, result.Duration.TotalSeconds);
            return 0;
        }
        else
        {
            logger.LogError("✗ Transfer failed: {Error}", result.ErrorMessage);
            return 1;
        }
    }
    else if (!string.IsNullOrEmpty(configPath))
    {
        // Run from config file (legacy mode)
        var configLoader = services.GetRequiredService<ConfigurationLoader>();
        var validator = services.GetRequiredService<ConfigurationValidator>();
        var legacyOrchestrator = services.GetRequiredService<DataTransferOrchestrator>();

        logger.LogInformation("Loading configuration from {ConfigPath}", configPath);
        var config = await configLoader.LoadAsync(configPath);

        var validationResult = validator.Validate(config);
        if (!validationResult.IsValid)
        {
            logger.LogError("Configuration validation failed:");
            foreach (var error in validationResult.Errors)
            {
                logger.LogError("  - {Error}", error);
            }
            return 1;
        }

        int successCount = 0;
        int failureCount = 0;

        foreach (var tableConfig in config.Tables)
        {
            try
            {
                var result = await legacyOrchestrator.TransferTableAsync(
                    tableConfig,
                    config.Connections.Source,
                    config.Connections.Destination);

                if (result.Success)
                {
                    successCount++;
                    logger.LogInformation("✓ {Table}: {Rows} rows",
                        tableConfig.Source.FullyQualifiedName, result.RowsLoaded);
                }
                else
                {
                    failureCount++;
                    logger.LogError("✗ {Table}: {Error}",
                        tableConfig.Source.FullyQualifiedName, result.ErrorMessage);
                }
            }
            catch (Exception ex)
            {
                failureCount++;
                logger.LogError(ex, "✗ {Table}: Exception", tableConfig.Source.FullyQualifiedName);
            }
        }

        logger.LogInformation("Summary: {Success} succeeded, {Failed} failed", successCount, failureCount);
        return failureCount == 0 ? 0 : 1;
    }
    else
    {
        ShowHelp();
        return 1;
    }
}

static async Task<int> RunSchemaDiscoveryAsync(string connectionString, string? tableFilter, Microsoft.Extensions.Logging.ILogger logger)
{
    try
    {
        logger.LogInformation("Discovering database schema...");
        Console.WriteLine();
        Console.WriteLine("╔════════════════════════════════════════════╗");
        Console.WriteLine("║   🔍 Database Schema Discovery            ║");
        Console.WriteLine("╚════════════════════════════════════════════╝");
        Console.WriteLine();

        var discovery = new SqlSchemaDiscovery(connectionString);

        // Test connection first
        Console.Write("Testing connection... ");
        if (!await discovery.TestConnectionAsync())
        {
            Console.WriteLine("❌ Failed");
            logger.LogError("Failed to connect to database");
            return 1;
        }
        Console.WriteLine("✓ Connected");
        Console.WriteLine();

        if (!string.IsNullOrEmpty(tableFilter))
        {
            // Discover specific table
            var parts = tableFilter.Split('.');
            if (parts.Length != 2)
            {
                Console.WriteLine("❌ Table filter must be in format: schema.tablename (e.g., dbo.Orders)");
                return 1;
            }

            var schema = parts[0];
            var tableName = parts[1];

            Console.WriteLine($"Discovering table: {schema}.{tableName}");
            Console.WriteLine();

            var table = await discovery.DiscoverTableAsync(schema, tableName);
            if (table == null)
            {
                Console.WriteLine($"❌ Table {schema}.{tableName} not found");

                // Try to suggest similar tables
                var dbInfo = await discovery.DiscoverDatabaseAsync();
                var suggestions = dbInfo.GetTableSuggestions(tableName);
                if (suggestions.Any())
                {
                    Console.WriteLine();
                    Console.WriteLine("Did you mean one of these?");
                    foreach (var suggestion in suggestions)
                    {
                        Console.WriteLine($"  - {suggestion}");
                    }
                }
                return 1;
            }

            DisplayTableDetails(table);
            return 0;
        }
        else
        {
            // Discover entire database
            var dbInfo = await discovery.DiscoverDatabaseAsync();

            Console.WriteLine($"Database: {dbInfo.DatabaseName}");
            Console.WriteLine($"Server: {dbInfo.ServerVersion}");
            Console.WriteLine($"Tables: {dbInfo.TotalTables:N0}");
            Console.WriteLine($"Total Rows: {dbInfo.TotalRows:N0}");
            Console.WriteLine();
            Console.WriteLine("═══════════════════════════════════════════════════════════════");
            Console.WriteLine();

            // Group by schema
            var schemas = dbInfo.Tables.GroupBy(t => t.Schema).OrderBy(g => g.Key);

            foreach (var schemaGroup in schemas)
            {
                Console.WriteLine($"Schema: {schemaGroup.Key}");
                Console.WriteLine("───────────────────────────────────────────────────────────────");
                Console.WriteLine();

                foreach (var table in schemaGroup.OrderBy(t => t.TableName))
                {
                    DisplayTableSummary(table);
                }
                Console.WriteLine();
            }

            Console.WriteLine("═══════════════════════════════════════════════════════════════");
            Console.WriteLine();
            Console.WriteLine("💡 Tip: Use --table schema.tablename to see detailed information");
            Console.WriteLine("   Example: --discover \"...\" --table dbo.Orders");
            Console.WriteLine();

            return 0;
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"❌ Error: {ex.Message}");
        logger.LogError(ex, "Schema discovery failed");
        return 1;
    }
}

static void DisplayTableSummary(DataTransfer.SqlServer.Models.TableInfo table)
{
    Console.WriteLine($"📊 {table.FullName}");
    Console.WriteLine($"   Rows: {table.RowCount:N0}");
    Console.WriteLine($"   Columns: {table.Columns.Count}");

    var suggestion = table.GetBestPartitionSuggestion();
    if (suggestion != null)
    {
        Console.WriteLine($"   Suggested Partition: {suggestion.PartitionType}");
        if (!string.IsNullOrEmpty(suggestion.ColumnName))
        {
            Console.WriteLine($"   Column: {suggestion.ColumnName}");
        }
        if (!string.IsNullOrEmpty(suggestion.EffectiveDateColumn))
        {
            Console.WriteLine($"   Effective: {suggestion.EffectiveDateColumn}, Expiration: {suggestion.ExpirationDateColumn}");
        }
        Console.WriteLine($"   Confidence: {suggestion.Confidence:P0}");
    }
    Console.WriteLine();
}

static void DisplayTableDetails(DataTransfer.SqlServer.Models.TableInfo table)
{
    Console.WriteLine($"Table: {table.FullName}");
    Console.WriteLine($"Row Count: {table.RowCount:N0}");
    Console.WriteLine();
    Console.WriteLine("Columns:");
    Console.WriteLine("───────────────────────────────────────────────────────────────");

    foreach (var column in table.Columns)
    {
        var nullable = column.IsNullable ? "NULL" : "NOT NULL";
        var typeInfo = column.DataType;

        if (column.MaxLength > 0)
        {
            typeInfo += $"({column.MaxLength})";
        }
        else if (column.MaxLength == -1)
        {
            typeInfo += "(MAX)";
        }
        else if (column.Precision > 0)
        {
            typeInfo += $"({column.Precision},{column.Scale})";
        }

        Console.WriteLine($"  {column.ColumnName,-30} {typeInfo,-20} {nullable}");

        var colSuggestion = column.GetPartitionSuggestion();
        if (colSuggestion != null)
        {
            Console.WriteLine($"     💡 Can be used for {colSuggestion.PartitionType} partitioning");
        }
    }

    Console.WriteLine();
    Console.WriteLine("═══════════════════════════════════════════════════════════════");
    Console.WriteLine();

    var suggestion = table.GetBestPartitionSuggestion();
    if (suggestion != null)
    {
        Console.WriteLine("Recommended Partition Strategy:");
        Console.WriteLine("───────────────────────────────────────────────────────────────");
        Console.WriteLine();
        Console.WriteLine($"Type: {suggestion.PartitionType}");
        Console.WriteLine($"Reason: {suggestion.Reason}");
        Console.WriteLine($"Confidence: {suggestion.Confidence:P0}");
        Console.WriteLine();
        Console.WriteLine("Sample Configuration:");
        Console.WriteLine(suggestion.ToConfigurationJson());
        Console.WriteLine();
    }
}

static async Task<int> RunExportIcebergAsync(
    string connectionString,
    string sourceTable,
    string? icebergTableName,
    UnifiedTransferOrchestrator orchestrator,
    Microsoft.Extensions.Logging.ILogger logger)
{
    logger.LogInformation("Exporting SQL Server table {Table} to Iceberg", sourceTable);

    // Parse table name (expect schema.table format)
    var parts = sourceTable.Split('.');
    if (parts.Length != 2)
    {
        logger.LogError("Table name must be in format schema.table (e.g., dbo.Customers)");
        return 1;
    }

    var config = new TransferConfiguration
    {
        TransferType = TransferType.SqlToIceberg,
        Source = new SourceConfiguration
        {
            Type = SourceType.SqlServer,
            ConnectionString = connectionString,
            Table = new TableIdentifier
            {
                Schema = parts[0],
                Table = parts[1]
            }
        },
        Destination = new DestinationConfiguration
        {
            Type = DestinationType.Iceberg,
            IcebergTable = new IcebergTransferConfiguration
            {
                TableName = icebergTableName ?? parts[1].ToLower()
            }
        }
    };

    try
    {
        var result = await orchestrator.ExecuteTransferAsync(config, CancellationToken.None);

        if (result.Success)
        {
            logger.LogInformation("✓ Successfully exported {Rows} rows to Iceberg table '{IcebergTable}' in {Duration}s",
                result.RowsLoaded, config.Destination.IcebergTable.TableName, result.Duration.TotalSeconds);
            logger.LogInformation("  Iceberg table path: {Path}", result.ParquetFilePath);
            return 0;
        }
        else
        {
            logger.LogError("✗ Export failed: {Error}", result.ErrorMessage);
            return 1;
        }
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Export to Iceberg failed");
        return 1;
    }
}

static async Task<int> RunImportIcebergAsync(
    string icebergTableName,
    string connectionString,
    string destinationTable,
    UnifiedTransferOrchestrator orchestrator,
    Microsoft.Extensions.Logging.ILogger logger)
{
    logger.LogInformation("Importing Iceberg table {IcebergTable} to SQL Server table {Table}",
        icebergTableName, destinationTable);

    // Parse destination table name (expect schema.table format)
    var parts = destinationTable.Split('.');
    if (parts.Length != 2)
    {
        logger.LogError("Destination table name must be in format schema.table (e.g., dbo.Customers)");
        return 1;
    }

    var config = new TransferConfiguration
    {
        TransferType = TransferType.IcebergToSql,
        Source = new SourceConfiguration
        {
            Type = SourceType.Iceberg,
            IcebergTable = new IcebergTransferConfiguration
            {
                TableName = icebergTableName
            }
        },
        Destination = new DestinationConfiguration
        {
            Type = DestinationType.SqlServer,
            ConnectionString = connectionString,
            Table = new TableIdentifier
            {
                Schema = parts[0],
                Table = parts[1]
            }
        }
    };

    try
    {
        var result = await orchestrator.ExecuteTransferAsync(config, CancellationToken.None);

        if (result.Success)
        {
            logger.LogInformation("✓ Successfully imported {Rows} rows from Iceberg to SQL Server in {Duration}s",
                result.RowsLoaded, result.Duration.TotalSeconds);
            return 0;
        }
        else
        {
            logger.LogError("✗ Import failed: {Error}", result.ErrorMessage);
            return 1;
        }
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Import from Iceberg failed");
        return 1;
    }
}

static async Task<int> RunSyncIcebergAsync(
    string sourceConnectionString,
    string sourceTable,
    string icebergTableName,
    string targetConnectionString,
    string targetTable,
    string primaryKeyColumn,
    string watermarkColumn,
    string mergeStrategy,
    UnifiedTransferOrchestrator orchestrator,
    Microsoft.Extensions.Logging.ILogger logger)
{
    logger.LogInformation("Starting incremental sync: {SourceTable} → {IcebergTable} → {TargetTable}",
        sourceTable, icebergTableName, targetTable);

    // Parse source table
    var sourceParts = sourceTable.Split('.');
    if (sourceParts.Length != 2)
    {
        logger.LogError("Source table name must be in format schema.table");
        return 1;
    }

    // Parse target table
    var targetParts = targetTable.Split('.');
    if (targetParts.Length != 2)
    {
        logger.LogError("Target table name must be in format schema.table");
        return 1;
    }

    var config = new TransferConfiguration
    {
        TransferType = TransferType.SqlToIcebergIncremental,
        Source = new SourceConfiguration
        {
            Type = SourceType.SqlServer,
            ConnectionString = sourceConnectionString,
            Table = new TableIdentifier
            {
                Schema = sourceParts[0],
                Table = sourceParts[1]
            }
        },
        Destination = new DestinationConfiguration
        {
            Type = DestinationType.Iceberg,
            ConnectionString = targetConnectionString,
            Table = new TableIdentifier
            {
                Schema = targetParts[0],
                Table = targetParts[1]
            },
            IcebergTable = new IcebergTransferConfiguration
            {
                TableName = icebergTableName,
                IncrementalSync = new IncrementalSyncOptions
                {
                    PrimaryKeyColumn = primaryKeyColumn,
                    WatermarkColumn = watermarkColumn,
                    MergeStrategy = mergeStrategy,
                    WatermarkType = "timestamp"
                }
            }
        }
    };

    try
    {
        var result = await orchestrator.ExecuteTransferAsync(config, CancellationToken.None);

        if (result.Success)
        {
            logger.LogInformation("✓ Incremental sync completed: {Extracted} rows extracted, {Loaded} rows loaded in {Duration}s",
                result.RowsExtracted, result.RowsLoaded, result.Duration.TotalSeconds);
            return 0;
        }
        else
        {
            logger.LogError("✗ Sync failed: {Error}", result.ErrorMessage);
            return 1;
        }
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Incremental sync failed");
        return 1;
    }
}

static async Task<EnvironmentManager?> LoadEnvironmentAsync(string? environmentName, Microsoft.Extensions.Logging.ILogger logger)
{
    if (string.IsNullOrEmpty(environmentName))
    {
        return null;
    }

    try
    {
        var envFilePath = "config/environments.json";
        if (!File.Exists(envFilePath))
        {
            logger.LogWarning("Environment file not found: {Path}. Continuing without environment substitution.", envFilePath);
            return null;
        }

        var json = await File.ReadAllTextAsync(envFilePath);
        var settings = System.Text.Json.JsonSerializer.Deserialize<EnvironmentSettings>(json);

        if (settings == null || settings.Environments.Count == 0)
        {
            logger.LogWarning("No environments found in {Path}", envFilePath);
            return null;
        }

        var manager = new EnvironmentManager(settings);
        var env = manager.GetEnvironment(environmentName);
        logger.LogInformation("Loaded environment: {EnvironmentName}", environmentName);
        return manager;
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Failed to load environment: {EnvironmentName}", environmentName);
        throw;
    }
}

static void ApplyEnvironmentToConfiguration(TransferConfiguration config, EnvironmentConfiguration environment, EnvironmentManager manager)
{
    // Apply token replacement to connection strings
    if (!string.IsNullOrEmpty(config.Source.ConnectionString))
    {
        config.Source.ConnectionString = manager.ReplaceTokens(config.Source.ConnectionString, environment);
    }

    if (!string.IsNullOrEmpty(config.Destination.ConnectionString))
    {
        config.Destination.ConnectionString = manager.ReplaceTokens(config.Destination.ConnectionString, environment);
    }

    // Apply token replacement to file paths
    if (!string.IsNullOrEmpty(config.Source.ParquetPath))
    {
        config.Source.ParquetPath = manager.ReplaceTokens(config.Source.ParquetPath, environment);
    }

    if (!string.IsNullOrEmpty(config.Destination.ParquetPath))
    {
        config.Destination.ParquetPath = manager.ReplaceTokens(config.Destination.ParquetPath, environment);
    }
}

static void ShowHelp()
{
    Console.WriteLine();
    Console.WriteLine("DataTransfer Console - Usage:");
    Console.WriteLine("=============================");
    Console.WriteLine();
    Console.WriteLine("Interactive mode (no arguments):");
    Console.WriteLine("  dotnet run --project src/DataTransfer.Console");
    Console.WriteLine();
    Console.WriteLine("Command-line mode:");
    Console.WriteLine("  --profile <name>      Run transfer from saved profile");
    Console.WriteLine("  --config <path>       Run transfer from config file (legacy)");
    Console.WriteLine("  --environment <name>  Use environment-specific settings (dev, staging, prod)");
    Console.WriteLine("  --list-profiles       List all saved profiles");
    Console.WriteLine("  --discover <connstr>  Discover database schema and suggest partitions");
    Console.WriteLine("  --table <schema.name> Discover specific table (use with --discover)");
    Console.WriteLine();
    Console.WriteLine("Iceberg commands:");
    Console.WriteLine("  --export-iceberg <conn> <schema.table>");
    Console.WriteLine("                        Export SQL Server table to Iceberg format");
    Console.WriteLine("    --iceberg-name <name>  Optional: Iceberg table name (default: table name)");
    Console.WriteLine();
    Console.WriteLine("  --import-iceberg <iceberg-table> <conn> <schema.table>");
    Console.WriteLine("                        Import Iceberg table to SQL Server");
    Console.WriteLine();
    Console.WriteLine("  --sync-iceberg <src-conn> <src-table> <iceberg-table> <tgt-conn> <tgt-table>");
    Console.WriteLine("                        Incremental sync with watermark tracking");
    Console.WriteLine("    --primary-key <column>   Primary key column for merge (required)");
    Console.WriteLine("    --watermark <column>     Watermark column for change detection (required)");
    Console.WriteLine("    --merge-strategy <type>  Merge strategy: upsert or append (default: upsert)");
    Console.WriteLine();
    Console.WriteLine("  --help                Show this help message");
    Console.WriteLine();
    Console.WriteLine("Examples:");
    Console.WriteLine("  # Run saved profile");
    Console.WriteLine("  dotnet run --project src/DataTransfer.Console -- --profile \"Daily Orders Extract\"");
    Console.WriteLine();
    Console.WriteLine("  # Run profile with environment-specific settings");
    Console.WriteLine("  dotnet run --project src/DataTransfer.Console -- --profile \"Daily Orders Extract\" --environment prod");
    Console.WriteLine();
    Console.WriteLine("  # Export to Iceberg");
    Console.WriteLine("  dotnet run --project src/DataTransfer.Console -- --export-iceberg \\");
    Console.WriteLine("    \"Server=localhost;Database=Sales;...\" dbo.Orders");
    Console.WriteLine();
    Console.WriteLine("  # Import from Iceberg");
    Console.WriteLine("  dotnet run --project src/DataTransfer.Console -- --import-iceberg \\");
    Console.WriteLine("    orders \"Server=localhost;Database=Warehouse;...\" dbo.Orders");
    Console.WriteLine();
    Console.WriteLine("  # Incremental sync");
    Console.WriteLine("  dotnet run --project src/DataTransfer.Console -- --sync-iceberg \\");
    Console.WriteLine("    \"Server=source;...\" dbo.Customers customers \\");
    Console.WriteLine("    \"Server=target;...\" dbo.Customers \\");
    Console.WriteLine("    --primary-key CustomerId --watermark LastModifiedDate");
    Console.WriteLine();
    Console.WriteLine("  # Discover schema");
    Console.WriteLine("  dotnet run --project src/DataTransfer.Console -- --discover \"Server=localhost;Database=MyDB;...\"");
    Console.WriteLine("  dotnet run --project src/DataTransfer.Console -- --discover \"...\" --table dbo.Orders");
    Console.WriteLine();
}

// Make Program class accessible to tests
public partial class Program { }
