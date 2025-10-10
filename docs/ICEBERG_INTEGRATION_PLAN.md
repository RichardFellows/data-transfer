# Apache Iceberg Integration Plan

## Project Overview

Integration of Apache Iceberg table format into the DataTransfer application, enabling SQL Server data to be exported to Iceberg tables backed by Parquet files with full ACID transaction support.

## Executive Summary

**Goal**: Add support for exporting SQL Server data to Apache Iceberg table format with filesystem catalog support.

**Key Technologies**:
- ParquetSharp (replaces Parquet.Net for field-id support)
- Apache.Avro (for manifest/manifest-list generation)
- Custom Avro wrappers (to preserve Iceberg-specific schema attributes)

**Success Criteria**:
1. Generate Iceberg tables readable by PyIceberg
2. Generate Iceberg tables queryable by DuckDB/Spark
3. Maintain existing test coverage (80%+)
4. Support existing partition strategies
5. Atomic commits via filesystem catalog

---

## Architecture Overview

### Iceberg Table Structure

Iceberg uses a three-tiered metadata hierarchy:

```
warehouse/
├── my_table/
│   ├── metadata/
│   │   ├── v1.metadata.json          # Table metadata (root)
│   │   ├── snap-123.avro             # Manifest list
│   │   ├── manifest-abc.avro         # Manifest file
│   │   └── version-hint.txt          # Current version pointer
│   └── data/
│       ├── year=2025/
│       │   └── month=01/
│       │       └── data-001.parquet  # Actual data files
```

**Key Concepts**:
- **Table Metadata**: Root JSON file containing schema, snapshots, partition specs
- **Manifest List**: Avro file listing all manifests for a snapshot
- **Manifest File**: Avro file listing all data files with statistics
- **Data Files**: Parquet files with embedded field-id metadata
- **Atomic Commits**: Version hint file updated atomically via filesystem move

---

## Implementation Phases

### Phase 0: Assessment & Dependencies

**New NuGet Packages Required**:
```xml
<!-- DataTransfer.Iceberg.csproj -->
<PackageReference Include="ParquetSharp" Version="14.0.1" />
<PackageReference Include="Apache.Avro" Version="1.11.3" />
```

**Project Structure**:
```
src/
├── DataTransfer.Iceberg/           # NEW - Iceberg-specific implementations
│   ├── Catalog/
│   │   └── FilesystemCatalog.cs
│   ├── Metadata/
│   │   ├── AvroSchemaWrapper.cs
│   │   ├── ManifestFileGenerator.cs
│   │   └── TableMetadataGenerator.cs
│   ├── Writers/
│   │   └── IcebergParquetWriter.cs
│   └── Models/
│       └── IcebergSchema.cs
├── DataTransfer.Core/              # MODIFY
│   ├── Models/Iceberg/             # NEW
│   └── Mapping/                    # NEW - Type mappers
└── DataTransfer.Parquet/           # REFACTOR - Adapter pattern
```

---

### Phase 1: Core Iceberg Infrastructure

#### 1.1 Domain Models

**File**: `src/DataTransfer.Core/Models/Iceberg/IcebergSchema.cs`

```csharp
namespace DataTransfer.Core.Models.Iceberg;

/// <summary>
/// Represents an Iceberg table schema with mandatory field IDs
/// </summary>
public class IcebergSchema
{
    [JsonPropertyName("type")]
    public string Type { get; set; } = "struct";

    [JsonPropertyName("schema-id")]
    public int SchemaId { get; set; }

    [JsonPropertyName("fields")]
    public List<IcebergField> Fields { get; set; } = new();
}

/// <summary>
/// Iceberg field with stable field-id for schema evolution
/// </summary>
public class IcebergField
{
    [JsonPropertyName("id")]
    public int Id { get; set; }  // CRITICAL: Enables schema evolution

    [JsonPropertyName("name")]
    public string Name { get; set; }

    [JsonPropertyName("required")]
    public bool Required { get; set; }

    [JsonPropertyName("type")]
    public object Type { get; set; }  // string for primitives, object for complex
}

/// <summary>
/// Root table metadata - the single source of truth
/// </summary>
public class IcebergTableMetadata
{
    [JsonPropertyName("format-version")]
    public int FormatVersion { get; set; } = 2;

    [JsonPropertyName("table-uuid")]
    public string TableUuid { get; set; }

    [JsonPropertyName("location")]
    public string Location { get; set; }

    [JsonPropertyName("last-updated-ms")]
    public long LastUpdatedMs { get; set; }

    [JsonPropertyName("last-column-id")]
    public int LastColumnId { get; set; }

    [JsonPropertyName("schemas")]
    public List<IcebergSchema> Schemas { get; set; } = new();

    [JsonPropertyName("current-schema-id")]
    public int CurrentSchemaId { get; set; }

    [JsonPropertyName("partition-specs")]
    public List<object> PartitionSpecs { get; set; } = new();

    [JsonPropertyName("default-spec-id")]
    public int DefaultSpecId { get; set; } = 0;

    [JsonPropertyName("last-partition-id")]
    public int LastPartitionId { get; set; } = 0;

    [JsonPropertyName("snapshots")]
    public List<IcebergSnapshot> Snapshots { get; set; } = new();

    [JsonPropertyName("current-snapshot-id")]
    public long? CurrentSnapshotId { get; set; }
}

/// <summary>
/// Snapshot representing table state at a point in time
/// </summary>
public class IcebergSnapshot
{
    [JsonPropertyName("snapshot-id")]
    public long SnapshotId { get; set; }

    [JsonPropertyName("timestamp-ms")]
    public long TimestampMs { get; set; }

    [JsonPropertyName("manifest-list")]
    public string ManifestList { get; set; }  // Relative path to manifest list file
}
```

**Tests**: `tests/DataTransfer.Core.Tests/Models/Iceberg/IcebergSchemaTests.cs`

```csharp
public class IcebergSchemaTests
{
    [Fact]
    public void Should_Generate_Sequential_Field_IDs()
    {
        // Test that field IDs are assigned monotonically starting at 1
    }

    [Fact]
    public void Should_Serialize_To_Valid_Iceberg_Json()
    {
        // Test JSON serialization includes all required fields
    }

    [Fact]
    public void Should_Map_Nullability_Correctly()
    {
        // Test Required property reflects SQL nullability
    }
}
```

#### 1.2 Type Mapping

**File**: `src/DataTransfer.Core/Mapping/SqlServerToIcebergTypeMapper.cs`

```csharp
namespace DataTransfer.Core.Mapping;

/// <summary>
/// Maps SQL Server data types to Iceberg primitive types
/// Reference: Gemini guide type mapping table
/// </summary>
public static class SqlServerToIcebergTypeMapper
{
    public static object MapType(SqlDbType sqlType, int? precision = null, int? scale = null)
    {
        return sqlType switch
        {
            // Integer types
            SqlDbType.BigInt => "long",
            SqlDbType.Int => "int",
            SqlDbType.SmallInt => "int",
            SqlDbType.TinyInt => "int",

            // Boolean
            SqlDbType.Bit => "boolean",

            // Floating point
            SqlDbType.Float => "double",
            SqlDbType.Real => "float",

            // Decimal - requires precision/scale object
            SqlDbType.Decimal or SqlDbType.Numeric or SqlDbType.Money or SqlDbType.SmallMoney
                => new { type = "decimal", precision = precision ?? 18, scale = scale ?? 0 },

            // Date/Time
            SqlDbType.Date => "date",
            SqlDbType.DateTime or SqlDbType.SmallDateTime or SqlDbType.DateTime2 => "timestamp",
            SqlDbType.DateTimeOffset => "timestamptz",

            // String types
            SqlDbType.Char or SqlDbType.NChar or SqlDbType.VarChar or
            SqlDbType.NVarChar or SqlDbType.Text or SqlDbType.NText => "string",

            // Binary
            SqlDbType.Binary or SqlDbType.VarBinary or SqlDbType.Image => "binary",

            // UUID
            SqlDbType.UniqueIdentifier => "uuid",

            _ => throw new NotSupportedException($"SQL type {sqlType} is not supported for Iceberg mapping")
        };
    }
}
```

**Tests**: `tests/DataTransfer.Core.Tests/Mapping/SqlServerToIcebergTypeMapperTests.cs`

```csharp
public class SqlServerToIcebergTypeMapperTests
{
    [Theory]
    [InlineData(SqlDbType.BigInt, "long")]
    [InlineData(SqlDbType.Int, "int")]
    [InlineData(SqlDbType.Bit, "boolean")]
    [InlineData(SqlDbType.DateTime2, "timestamp")]
    [InlineData(SqlDbType.UniqueIdentifier, "uuid")]
    public void Should_Map_Common_Types_Correctly(SqlDbType sqlType, string expectedIcebergType)
    {
        var result = SqlServerToIcebergTypeMapper.MapType(sqlType);
        result.Should().Be(expectedIcebergType);
    }

    [Fact]
    public void Should_Map_Decimal_With_Precision_And_Scale()
    {
        var result = SqlServerToIcebergTypeMapper.MapType(SqlDbType.Decimal, 18, 2);
        result.Should().BeEquivalentTo(new { type = "decimal", precision = 18, scale = 2 });
    }

    [Fact]
    public void Should_Throw_On_Unsupported_Type()
    {
        Action act = () => SqlServerToIcebergTypeMapper.MapType(SqlDbType.Timestamp);
        act.Should().Throw<NotSupportedException>();
    }
}
```

---

### Phase 2: ParquetSharp Integration

#### 2.1 Iceberg Parquet Writer

**File**: `src/DataTransfer.Iceberg/Writers/IcebergParquetWriter.cs`

```csharp
namespace DataTransfer.Iceberg.Writers;

/// <summary>
/// Writes Parquet files with Iceberg-compliant schema (embedded field-ids)
/// Uses ParquetSharp low-level GroupNode API
/// </summary>
public class IcebergParquetWriter : IDisposable
{
    private readonly ParquetFileWriter _writer;
    private readonly IcebergSchema _schema;
    private readonly string _filePath;

    public IcebergParquetWriter(string path, IcebergSchema schema)
    {
        _schema = schema;
        _filePath = path;

        // CRITICAL: Use GroupNode to embed field-id metadata
        var groupNode = BuildIcebergCompliantSchema(schema);
        _writer = new ParquetFileWriter(path, groupNode);
    }

    /// <summary>
    /// Builds Parquet schema with mandatory Iceberg field-id annotations
    /// This is the key to Iceberg compatibility
    /// </summary>
    private GroupNode BuildIcebergCompliantSchema(IcebergSchema schema)
    {
        var nodes = new List<Node>();

        foreach (var field in schema.Fields)
        {
            var parquetType = MapIcebergTypeToParquetType(field.Type);
            var repetition = field.Required ? Repetition.Required : Repetition.Optional;

            // CRITICAL: PrimitiveNode constructor that accepts fieldId parameter
            var primitiveNode = new PrimitiveNode(
                name: field.Name,
                repetition: repetition,
                type: parquetType,
                fieldId: field.Id  // ← This is what enables Iceberg schema evolution
            );

            nodes.Add(primitiveNode);
        }

        return new GroupNode("schema", Repetition.Required, nodes);
    }

    /// <summary>
    /// Maps Iceberg types to ParquetSharp physical types
    /// </summary>
    private ParquetType MapIcebergTypeToParquetType(object icebergType)
    {
        if (icebergType is string primitiveType)
        {
            return primitiveType switch
            {
                "boolean" => ParquetType.Boolean,
                "int" => ParquetType.Int32,
                "long" => ParquetType.Int64,
                "float" => ParquetType.Float,
                "double" => ParquetType.Double,
                "string" => ParquetType.ByteArray,
                "binary" => ParquetType.ByteArray,
                "uuid" => ParquetType.FixedLenByteArray(16),
                "date" => ParquetType.Int32,
                "timestamp" => ParquetType.Int64,
                "timestamptz" => ParquetType.Int64,
                _ => throw new NotSupportedException($"Iceberg type {primitiveType} not mapped")
            };
        }

        // Handle complex types like decimal
        // ... implementation for decimal, etc.

        throw new NotSupportedException($"Iceberg type {icebergType} not supported");
    }

    /// <summary>
    /// Writes a batch of data from SqlDataReader to Parquet
    /// Returns metadata for manifest generation
    /// </summary>
    public async Task<DataFileMetadata> WriteBatchAsync(
        IDataReader reader,
        int batchSize = 100000,
        CancellationToken ct = default)
    {
        long recordCount = 0;

        using var rowGroupWriter = _writer.AppendRowGroup();

        while (await reader.ReadAsync(ct))
        {
            // Write row data
            for (int i = 0; i < reader.FieldCount; i++)
            {
                var columnWriter = rowGroupWriter.NextColumn();
                var value = reader.GetValue(i);

                // Type-specific writing logic
                // ... implementation
            }

            recordCount++;

            if (recordCount % batchSize == 0)
            {
                rowGroupWriter.Close();
                rowGroupWriter = _writer.AppendRowGroup();
            }
        }

        _writer.Close();

        // Return file statistics for manifest
        var fileInfo = new FileInfo(_filePath);
        return new DataFileMetadata
        {
            FilePath = _filePath,
            FileSizeInBytes = fileInfo.Length,
            RecordCount = recordCount,
            // Add column statistics (min/max values, null counts)
        };
    }

    public void Dispose()
    {
        _writer?.Dispose();
    }
}

/// <summary>
/// Metadata about a written data file (for manifest generation)
/// </summary>
public class DataFileMetadata
{
    public string FilePath { get; set; }
    public long FileSizeInBytes { get; set; }
    public long RecordCount { get; set; }
    public Dictionary<string, object> ColumnStatistics { get; set; }
}
```

**Tests**: `tests/DataTransfer.Iceberg.Tests/Writers/IcebergParquetWriterTests.cs`

```csharp
public class IcebergParquetWriterTests
{
    [Fact]
    public void Should_Embed_Field_IDs_In_Parquet_Schema()
    {
        // Write a file, then read schema metadata with ParquetSharp
        // Verify field-id annotations are present
    }

    [Fact]
    public async Task Should_Write_Data_With_Correct_Types()
    {
        // Test that data types are correctly written
    }

    [Fact]
    public async Task Should_Generate_File_Statistics()
    {
        // Verify returned DataFileMetadata contains correct record count
    }
}
```

---

### Phase 3: Iceberg Metadata Generation

#### 3.1 Avro Schema Wrapper (Critical Workaround)

**File**: `src/DataTransfer.Iceberg/Metadata/AvroSchemaWrapper.cs`

```csharp
namespace DataTransfer.Iceberg.Metadata;

/// <summary>
/// Wrapper around Apache.Avro.Schema that preserves Iceberg-specific attributes
/// CRITICAL: Apache.Avro strips field-id, element-id, etc. during serialization
/// This wrapper intercepts ToString() to inject the original compliant schema
/// </summary>
public class AvroSchemaWrapper : Apache.Avro.Schema
{
    private readonly string _originalJsonSchema;

    public AvroSchemaWrapper(string compliantJsonSchema)
        : base(Apache.Avro.Schema.Parse(compliantJsonSchema))
    {
        _originalJsonSchema = compliantJsonSchema;
    }

    /// <summary>
    /// CRITICAL OVERRIDE: Returns original JSON instead of regenerated version
    /// This preserves Iceberg field-id, element-id, key-id, value-id attributes
    /// </summary>
    public override string ToString()
    {
        return _originalJsonSchema;
    }
}

/// <summary>
/// Wrapper around DatumWriter that uses AvroSchemaWrapper
/// Ensures Iceberg-compliant schema is written to Avro file header
/// </summary>
public class AvroDatumWriterWrapper<T> : DatumWriter<T>
{
    private readonly DatumWriter<T> _innerWriter;
    private readonly AvroSchemaWrapper _schemaWrapper;

    public AvroDatumWriterWrapper(DatumWriter<T> innerWriter, string compliantSchema)
    {
        _innerWriter = innerWriter;
        _schemaWrapper = new AvroSchemaWrapper(compliantSchema);
    }

    public override Schema Schema => _schemaWrapper;

    public override void Write(T datum, Encoder encoder)
    {
        _innerWriter.Write(datum, encoder);
    }
}
```

#### 3.2 Manifest File Generator

**File**: `src/DataTransfer.Iceberg/Metadata/ManifestFileGenerator.cs`

```csharp
namespace DataTransfer.Iceberg.Metadata;

/// <summary>
/// Generates Iceberg manifest files (Avro format)
/// Manifest files list data files with statistics for query planning
/// </summary>
public class ManifestFileGenerator
{
    // Iceberg manifest entry schema (from spec)
    private const string ManifestEntrySchema = @"
    {
      ""type"": ""record"",
      ""name"": ""manifest_entry"",
      ""fields"": [
        {
          ""name"": ""status"",
          ""type"": ""int"",
          ""field-id"": 0
        },
        {
          ""name"": ""snapshot_id"",
          ""type"": [""null"", ""long""],
          ""default"": null,
          ""field-id"": 1
        },
        {
          ""name"": ""data_file"",
          ""type"": {
            ""type"": ""record"",
            ""name"": ""r2"",
            ""fields"": [
              {""name"": ""file_path"", ""type"": ""string"", ""field-id"": 100},
              {""name"": ""file_format"", ""type"": ""string"", ""field-id"": 101},
              {""name"": ""partition"", ""type"": {""type"": ""map"", ""values"": ""string""}, ""field-id"": 102},
              {""name"": ""record_count"", ""type"": ""long"", ""field-id"": 103},
              {""name"": ""file_size_in_bytes"", ""type"": ""long"", ""field-id"": 104}
            ]
          },
          ""field-id"": 2
        }
      ]
    }";

    public string WriteManifest(
        List<DataFileMetadata> dataFiles,
        string outputPath,
        long snapshotId)
    {
        // Use wrapper to preserve field-id attributes
        var schema = Apache.Avro.Schema.Parse(ManifestEntrySchema);
        var writer = new GenericDatumWriter<GenericRecord>(schema);
        var wrappedWriter = new AvroDatumWriterWrapper<GenericRecord>(writer, ManifestEntrySchema);

        using var fileWriter = DataFileWriter<GenericRecord>.OpenWriter(wrappedWriter, outputPath);

        foreach (var dataFile in dataFiles)
        {
            var record = new GenericRecord((RecordSchema)schema);
            record.Add("status", 1);  // 1 = ADDED
            record.Add("snapshot_id", snapshotId);

            var dataFileRecord = CreateDataFileRecord(dataFile, schema);
            record.Add("data_file", dataFileRecord);

            fileWriter.Append(record);
        }

        return outputPath;
    }

    private GenericRecord CreateDataFileRecord(DataFileMetadata metadata, Schema schema)
    {
        var dataFileSchema = ((RecordSchema)schema)
            .Fields.First(f => f.Name == "data_file").Schema;

        var record = new GenericRecord((RecordSchema)dataFileSchema);
        record.Add("file_path", metadata.FilePath);
        record.Add("file_format", "PARQUET");
        record.Add("partition", new Dictionary<string, string>());
        record.Add("record_count", metadata.RecordCount);
        record.Add("file_size_in_bytes", metadata.FileSizeInBytes);

        return record;
    }
}
```

#### 3.3 Manifest List Generator

**File**: `src/DataTransfer.Iceberg/Metadata/ManifestListGenerator.cs`

```csharp
namespace DataTransfer.Iceberg.Metadata;

/// <summary>
/// Generates Iceberg manifest list files (Avro format)
/// Manifest lists index all manifests for a snapshot
/// </summary>
public class ManifestListGenerator
{
    // Iceberg manifest list schema (from spec)
    private const string ManifestListSchema = @"
    {
      ""type"": ""record"",
      ""name"": ""manifest_file"",
      ""fields"": [
        {""name"": ""manifest_path"", ""type"": ""string"", ""field-id"": 500},
        {""name"": ""manifest_length"", ""type"": ""long"", ""field-id"": 501},
        {""name"": ""partition_spec_id"", ""type"": ""int"", ""field-id"": 502},
        {""name"": ""added_files_count"", ""type"": [""null"", ""int""], ""default"": null, ""field-id"": 512},
        {""name"": ""existing_files_count"", ""type"": [""null"", ""int""], ""default"": null, ""field-id"": 513},
        {""name"": ""deleted_files_count"", ""type"": [""null"", ""int""], ""default"": null, ""field-id"": 514}
      ]
    }";

    public string WriteManifestList(
        string manifestPath,
        string outputPath,
        int addedFilesCount)
    {
        var schema = Apache.Avro.Schema.Parse(ManifestListSchema);
        var writer = new GenericDatumWriter<GenericRecord>(schema);
        var wrappedWriter = new AvroDatumWriterWrapper<GenericRecord>(writer, ManifestListSchema);

        using var fileWriter = DataFileWriter<GenericRecord>.OpenWriter(wrappedWriter, outputPath);

        var manifestInfo = new FileInfo(manifestPath);

        var record = new GenericRecord((RecordSchema)schema);
        record.Add("manifest_path", manifestPath);
        record.Add("manifest_length", manifestInfo.Length);
        record.Add("partition_spec_id", 0);
        record.Add("added_files_count", addedFilesCount);
        record.Add("existing_files_count", 0);
        record.Add("deleted_files_count", 0);

        fileWriter.Append(record);

        return outputPath;
    }
}
```

#### 3.4 Table Metadata Generator

**File**: `src/DataTransfer.Iceberg/Metadata/TableMetadataGenerator.cs`

```csharp
namespace DataTransfer.Iceberg.Metadata;

/// <summary>
/// Generates the root Iceberg table metadata JSON file
/// This is the single source of truth for the table
/// </summary>
public class TableMetadataGenerator
{
    public void WriteMetadata(IcebergTableMetadata metadata, string outputPath)
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = true,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        };

        var json = JsonSerializer.Serialize(metadata, options);
        File.WriteAllText(outputPath, json);
    }

    public IcebergTableMetadata CreateInitialMetadata(
        IcebergSchema schema,
        string tableLocation,
        string manifestListPath,
        long snapshotId)
    {
        return new IcebergTableMetadata
        {
            FormatVersion = 2,
            TableUuid = Guid.NewGuid().ToString(),
            Location = tableLocation,
            LastUpdatedMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            LastColumnId = schema.Fields.Max(f => f.Id),
            Schemas = new List<IcebergSchema> { schema },
            CurrentSchemaId = schema.SchemaId,
            PartitionSpecs = new List<object>(),
            DefaultSpecId = 0,
            LastPartitionId = 0,
            Snapshots = new List<IcebergSnapshot>
            {
                new IcebergSnapshot
                {
                    SnapshotId = snapshotId,
                    TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    ManifestList = manifestListPath
                }
            },
            CurrentSnapshotId = snapshotId
        };
    }
}
```

---

### Phase 4: Filesystem Catalog Implementation

#### 4.1 Filesystem Catalog

**File**: `src/DataTransfer.Iceberg/Catalog/FilesystemCatalog.cs`

```csharp
namespace DataTransfer.Iceberg.Catalog;

/// <summary>
/// Local filesystem-based Iceberg catalog
/// Manages table metadata and atomic commits
/// </summary>
public class FilesystemCatalog
{
    private readonly string _warehousePath;
    private readonly ILogger<FilesystemCatalog> _logger;

    public FilesystemCatalog(string warehousePath, ILogger<FilesystemCatalog> logger)
    {
        _warehousePath = warehousePath;
        _logger = logger;
    }

    /// <summary>
    /// Creates directory structure for a new Iceberg table
    /// </summary>
    public string InitializeTable(string tableName)
    {
        var tablePath = Path.Combine(_warehousePath, tableName);
        var metadataPath = Path.Combine(tablePath, "metadata");
        var dataPath = Path.Combine(tablePath, "data");

        Directory.CreateDirectory(metadataPath);
        Directory.CreateDirectory(dataPath);

        return tablePath;
    }

    /// <summary>
    /// Atomically commits a new snapshot to the catalog
    /// Uses version-hint.txt for atomic pointer swap
    /// </summary>
    public async Task<bool> CommitAsync(
        string tableName,
        IcebergTableMetadata metadata,
        CancellationToken ct = default)
    {
        var tablePath = Path.Combine(_warehousePath, tableName);
        var metadataDir = Path.Combine(tablePath, "metadata");

        try
        {
            // Determine next version
            var version = GetNextVersion(metadataDir);
            var metadataFile = Path.Combine(metadataDir, $"v{version}.metadata.json");

            _logger.LogInformation("Committing Iceberg snapshot version {Version} for table {Table}",
                version, tableName);

            // 1. Write metadata file
            var generator = new TableMetadataGenerator();
            generator.WriteMetadata(metadata, metadataFile);

            // 2. Atomic commit via version hint
            await AtomicVersionUpdate(metadataDir, version, ct);

            _logger.LogInformation("Successfully committed Iceberg table {Table} version {Version}",
                tableName, version);

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to commit Iceberg table {Table}", tableName);
            return false;
        }
    }

    /// <summary>
    /// Atomically updates version-hint.txt using filesystem move
    /// This provides ACID commit semantics
    /// </summary>
    private async Task AtomicVersionUpdate(string metadataDir, int version, CancellationToken ct)
    {
        var hintFile = Path.Combine(metadataDir, "version-hint.txt");
        var tempHint = Path.Combine(metadataDir, $"version-hint.txt.{Guid.NewGuid()}");

        // Write to temp file
        await File.WriteAllTextAsync(tempHint, version.ToString(), ct);

        // Atomic move (on same filesystem)
        File.Move(tempHint, hintFile, overwrite: true);
    }

    /// <summary>
    /// Gets the next version number by reading current version hint
    /// </summary>
    private int GetNextVersion(string metadataDir)
    {
        var hintFile = Path.Combine(metadataDir, "version-hint.txt");

        if (!File.Exists(hintFile))
            return 1;

        var currentVersion = int.Parse(File.ReadAllText(hintFile));
        return currentVersion + 1;
    }

    /// <summary>
    /// Reads current table metadata
    /// </summary>
    public IcebergTableMetadata? LoadTable(string tableName)
    {
        var tablePath = Path.Combine(_warehousePath, tableName);
        var metadataDir = Path.Combine(tablePath, "metadata");
        var hintFile = Path.Combine(metadataDir, "version-hint.txt");

        if (!File.Exists(hintFile))
            return null;

        var version = int.Parse(File.ReadAllText(hintFile));
        var metadataFile = Path.Combine(metadataDir, $"v{version}.metadata.json");

        var json = File.ReadAllText(metadataFile);
        return JsonSerializer.Deserialize<IcebergTableMetadata>(json);
    }
}
```

**Tests**: `tests/DataTransfer.Iceberg.Tests/Catalog/FilesystemCatalogTests.cs`

```csharp
public class FilesystemCatalogTests
{
    [Fact]
    public void Should_Create_Warehouse_Directory_Structure()
    {
        var catalog = new FilesystemCatalog(_tempPath, _logger);
        var tablePath = catalog.InitializeTable("test_table");

        Directory.Exists(Path.Combine(tablePath, "metadata")).Should().BeTrue();
        Directory.Exists(Path.Combine(tablePath, "data")).Should().BeTrue();
    }

    [Fact]
    public async Task Should_Atomically_Commit_New_Snapshot()
    {
        var catalog = new FilesystemCatalog(_tempPath, _logger);
        catalog.InitializeTable("test_table");

        var metadata = CreateTestMetadata();
        var result = await catalog.CommitAsync("test_table", metadata);

        result.Should().BeTrue();
        File.Exists(Path.Combine(_tempPath, "test_table/metadata/version-hint.txt")).Should().BeTrue();
    }

    [Fact]
    public async Task Should_Support_Concurrent_Reads_During_Write()
    {
        // Test that readers see consistent state during commit
    }
}
```

---

### Phase 5: Integration with Existing Pipeline

#### 5.1 Iceberg Table Strategy

**File**: `src/DataTransfer.Core/Strategies/IcebergTableStrategy.cs`

```csharp
namespace DataTransfer.Core.Strategies;

/// <summary>
/// Strategy for exporting SQL Server data to Iceberg table format
/// Orchestrates: schema inference → data writing → metadata generation → commit
/// </summary>
public class IcebergTableStrategy : PartitionStrategy
{
    private readonly FilesystemCatalog _catalog;
    private readonly string _warehousePath;
    private readonly ILogger<IcebergTableStrategy> _logger;

    public IcebergTableStrategy(
        FilesystemCatalog catalog,
        string warehousePath,
        ILogger<IcebergTableStrategy> logger)
    {
        _catalog = catalog;
        _warehousePath = warehousePath;
        _logger = logger;
    }

    public override async Task ExecuteAsync(
        ITableExtractor extractor,
        IDataLoader loader,
        CancellationToken ct = default)
    {
        var tableName = GetTableName(extractor);

        _logger.LogInformation("Starting Iceberg export for table {Table}", tableName);

        // 1. Initialize table structure
        var tablePath = _catalog.InitializeTable(tableName);

        // 2. Infer Iceberg schema from SQL reader
        var schema = await InferIcebergSchemaAsync(extractor, ct);

        // 3. Write Parquet data files
        var dataFiles = await WriteDataFilesAsync(extractor, schema, tablePath, ct);

        // 4. Generate snapshot ID
        var snapshotId = GenerateSnapshotId();

        // 5. Generate manifest file
        var manifestPath = GenerateManifest(dataFiles, tablePath, snapshotId);

        // 6. Generate manifest list
        var manifestListPath = GenerateManifestList(manifestPath, tablePath, dataFiles.Count);

        // 7. Generate table metadata
        var metadata = GenerateTableMetadata(schema, tablePath, manifestListPath, snapshotId);

        // 8. Atomic commit
        var success = await _catalog.CommitAsync(tableName, metadata, ct);

        if (!success)
            throw new InvalidOperationException($"Failed to commit Iceberg table {tableName}");

        _logger.LogInformation("Successfully created Iceberg table {Table} with {FileCount} data files",
            tableName, dataFiles.Count);
    }

    /// <summary>
    /// Infers Iceberg schema from SqlDataReader metadata
    /// Assigns sequential field IDs starting at 1
    /// </summary>
    private async Task<IcebergSchema> InferIcebergSchemaAsync(
        ITableExtractor extractor,
        CancellationToken ct)
    {
        using var reader = await extractor.ExtractAsync(ct);
        var schemaTable = reader.GetSchemaTable();

        var schema = new IcebergSchema { SchemaId = 0 };
        int fieldId = 1;

        foreach (DataRow row in schemaTable.Rows)
        {
            var columnName = row["ColumnName"].ToString();
            var dataType = (Type)row["DataType"];
            var allowNull = (bool)row["AllowDBNull"];

            var sqlType = GetSqlDbType(dataType);
            var icebergType = SqlServerToIcebergTypeMapper.MapType(sqlType);

            schema.Fields.Add(new IcebergField
            {
                Id = fieldId++,
                Name = columnName,
                Required = !allowNull,
                Type = icebergType
            });
        }

        return schema;
    }

    /// <summary>
    /// Writes data to Parquet files with Iceberg-compliant schema
    /// </summary>
    private async Task<List<DataFileMetadata>> WriteDataFilesAsync(
        ITableExtractor extractor,
        IcebergSchema schema,
        string tablePath,
        CancellationToken ct)
    {
        var dataFiles = new List<DataFileMetadata>();
        var dataDir = Path.Combine(tablePath, "data");
        var dataFilePath = Path.Combine(dataDir, $"data-{Guid.NewGuid()}.parquet");

        using var reader = await extractor.ExtractAsync(ct);
        using var writer = new IcebergParquetWriter(dataFilePath, schema);

        var metadata = await writer.WriteBatchAsync(reader, ct: ct);
        dataFiles.Add(metadata);

        return dataFiles;
    }

    private string GenerateManifest(
        List<DataFileMetadata> dataFiles,
        string tablePath,
        long snapshotId)
    {
        var generator = new ManifestFileGenerator();
        var manifestPath = Path.Combine(tablePath, "metadata", $"manifest-{Guid.NewGuid()}.avro");
        return generator.WriteManifest(dataFiles, manifestPath, snapshotId);
    }

    private string GenerateManifestList(
        string manifestPath,
        string tablePath,
        int addedFilesCount)
    {
        var generator = new ManifestListGenerator();
        var manifestListPath = Path.Combine(tablePath, "metadata", $"snap-{Guid.NewGuid()}.avro");
        return generator.WriteManifestList(manifestPath, manifestListPath, addedFilesCount);
    }

    private IcebergTableMetadata GenerateTableMetadata(
        IcebergSchema schema,
        string tablePath,
        string manifestListPath,
        long snapshotId)
    {
        var generator = new TableMetadataGenerator();
        return generator.CreateInitialMetadata(schema, tablePath, manifestListPath, snapshotId);
    }

    private long GenerateSnapshotId()
    {
        return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }
}
```

#### 5.2 Update Pipeline Orchestrator

**File**: `src/DataTransfer.Pipeline/UnifiedTransferOrchestrator.cs` (modifications)

```csharp
public async Task<TransferResult> ExecuteAsync(
    TransferConfiguration config,
    CancellationToken ct = default)
{
    var strategy = SelectStrategy(config);

    // ... existing execution logic
}

private PartitionStrategy SelectStrategy(TransferConfiguration config)
{
    // Check if Iceberg output is requested
    if (config.Destination.Format == OutputFormat.IcebergParquet)
    {
        var catalog = new FilesystemCatalog(
            config.Destination.WarehousePath,
            _logger
        );

        return new IcebergTableStrategy(
            catalog,
            config.Destination.WarehousePath,
            _loggerFactory.CreateLogger<IcebergTableStrategy>()
        );
    }

    // Existing strategy selection logic
    return config.PartitionType switch
    {
        PartitionType.Date => new DatePartitionStrategy(...),
        PartitionType.IntDate => new IntDatePartitionStrategy(...),
        // ... etc
    };
}
```

---

### Phase 6: Validation & Testing

#### 6.1 PyIceberg Validation Infrastructure

**File**: `tests/DataTransfer.Integration.Tests/Validation/PyIcebergValidator.cs`

```csharp
namespace DataTransfer.Integration.Tests.Validation;

/// <summary>
/// Validates Iceberg tables using PyIceberg (Python library)
/// This ensures generated tables are standards-compliant
/// </summary>
public class PyIcebergValidator
{
    public async Task<ValidationResult> ValidateTableAsync(
        string warehousePath,
        string tableName)
    {
        var pythonScript = $@"
import sys
from pyiceberg.catalog import load_catalog

try:
    # Load catalog
    catalog = load_catalog(
        'local',
        **{{
            'type': 'rest',
            'uri': 'http://localhost:8181',
            'warehouse': 'file://{warehousePath}'
        }}
    )

    # Load table
    table = catalog.load_table('{tableName}')

    # Read schema
    schema = table.schema()
    print(f'SCHEMA: {{schema}}')

    # Scan data
    df = table.scan().to_pandas()
    print(f'ROWS: {{len(df)}}')

    # Verify field IDs present
    for field in schema.fields:
        if not hasattr(field, 'field_id'):
            print('ERROR: Missing field_id')
            sys.exit(1)

    print('VALIDATION: SUCCESS')
    sys.exit(0)

except Exception as e:
    print(f'ERROR: {{e}}')
    sys.exit(1)
";

        var result = await Bash.ExecuteAsync("python3", "-c", pythonScript);

        return new ValidationResult
        {
            Success = result.ExitCode == 0,
            Output = result.Output,
            Errors = result.Error
        };
    }
}

public class ValidationResult
{
    public bool Success { get; set; }
    public string Output { get; set; }
    public string Errors { get; set; }
}
```

**Test**: `tests/DataTransfer.Integration.Tests/IcebergValidationTests.cs`

```csharp
[Collection("Integration")]
public class IcebergValidationTests : IClassFixture<IcebergTestFixture>
{
    private readonly IcebergTestFixture _fixture;
    private readonly PyIcebergValidator _validator;

    [Fact]
    public async Task Generated_Iceberg_Table_Should_Be_Readable_By_PyIceberg()
    {
        // Arrange
        var config = new TransferConfiguration
        {
            TransferType = TransferType.SqlToIcebergParquet,
            Source = _fixture.CreateSqlServerSource(),
            Destination = new DestinationConfiguration
            {
                Format = OutputFormat.IcebergParquet,
                WarehousePath = _fixture.WarehousePath
            }
        };

        // Act
        var result = await _fixture.Orchestrator.ExecuteAsync(config);

        // Assert
        result.Success.Should().BeTrue();

        var validation = await _validator.ValidateTableAsync(
            _fixture.WarehousePath,
            "test_table"
        );

        validation.Success.Should().BeTrue("PyIceberg should successfully read the table");
        validation.Output.Should().Contain("VALIDATION: SUCCESS");
    }

    [Fact]
    public async Task Generated_Table_Should_Preserve_Field_IDs()
    {
        // Test that field IDs are present in PyIceberg schema
    }

    [Fact]
    public async Task Generated_Table_Should_Match_Source_Row_Count()
    {
        // Validate data integrity
    }
}
```

#### 6.2 DuckDB Query Tests

**Test**: `tests/DataTransfer.Integration.Tests/IcebergDuckDBTests.cs`

```csharp
[Collection("Integration")]
public class IcebergDuckDBTests
{
    [Fact]
    public async Task Generated_Table_Should_Be_Queryable_Via_DuckDB()
    {
        // Arrange - Generate Iceberg table
        var tablePath = await GenerateTestIcebergTable();

        // Act - Query with DuckDB
        using var conn = new DuckDBConnection();
        await conn.OpenAsync();

        await conn.ExecuteAsync("INSTALL iceberg;");
        await conn.ExecuteAsync("LOAD iceberg;");

        var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM iceberg_scan('{tablePath}')";

        var count = (long)await cmd.ExecuteScalarAsync();

        // Assert
        count.Should().BeGreaterThan(0);
    }
}
```

---

### Phase 7: Configuration & UI Integration

#### 7.1 Configuration Models

**File**: `src/DataTransfer.Configuration/Models/DestinationConfiguration.cs` (modifications)

```csharp
public class DestinationConfiguration
{
    public DestinationType Type { get; set; }
    public OutputFormat Format { get; set; } = OutputFormat.Parquet;
    public string? WarehousePath { get; set; }
    public string? CatalogUri { get; set; }  // For future REST catalog

    // ... existing properties
}

public enum OutputFormat
{
    Parquet,
    IcebergParquet
}
```

#### 7.2 Web UI Updates

**File**: `src/DataTransfer.Web/Components/Pages/NewTransfer.razor` (modifications)

Add after the destination type selection:

```razor
@if (_config.TransferType == TransferType.SqlToParquet)
{
    <div class="mb-3">
        <label class="form-label">Output Format</label>
        <div class="form-check">
            <input class="form-check-input" type="radio" name="outputFormat"
                   id="formatParquet" value="@OutputFormat.Parquet"
                   @onchange="@(() => _config.Destination.Format = OutputFormat.Parquet)"
                   checked="@(_config.Destination.Format == OutputFormat.Parquet)" />
            <label class="form-check-label" for="formatParquet">
                Standard Parquet
                <small class="text-muted d-block">Single Parquet file with date partitioning</small>
            </label>
        </div>
        <div class="form-check mt-2">
            <input class="form-check-input" type="radio" name="outputFormat"
                   id="formatIceberg" value="@OutputFormat.IcebergParquet"
                   @onchange="@(() => _config.Destination.Format = OutputFormat.IcebergParquet)"
                   checked="@(_config.Destination.Format == OutputFormat.IcebergParquet)" />
            <label class="form-check-label" for="formatIceberg">
                Apache Iceberg Table
                <small class="text-muted d-block">
                    ACID-compliant table format with schema evolution support.
                    Queryable by Spark, Trino, DuckDB, and other engines.
                </small>
            </label>
        </div>
    </div>

    @if (_config.Destination.Format == OutputFormat.IcebergParquet)
    {
        <div class="mb-3">
            <label class="form-label">Warehouse Path</label>
            <input @bind="_config.Destination.WarehousePath" class="form-control"
                   placeholder="/path/to/warehouse"
                   title="Root directory for Iceberg tables" />
            <div class="form-text">
                Root directory where Iceberg table metadata and data will be stored
            </div>
        </div>
    }
}
```

---

## Implementation Timeline

### Sprint 1 (Week 1-2): Core Infrastructure
- ✅ Create DataTransfer.Iceberg project
- ✅ Iceberg domain models (IcebergSchema, IcebergTableMetadata)
- ✅ SQL Server → Iceberg type mapping
- ✅ Unit tests for models and mapping

### Sprint 2 (Week 3-4): ParquetSharp & Metadata
- ✅ IcebergParquetWriter with field-id support
- ✅ Avro schema wrappers (critical workaround)
- ✅ Manifest and manifest list generators
- ✅ Table metadata generator
- ✅ Unit tests for writers and generators

### Sprint 3 (Week 5): Catalog & Strategy
- ✅ FilesystemCatalog implementation
- ✅ IcebergTableStrategy
- ✅ Pipeline orchestrator integration
- ✅ Integration tests

### Sprint 4 (Week 6): Validation
- ✅ PyIceberg validation infrastructure
- ✅ DuckDB query tests
- ✅ E2E integration tests
- ✅ Fix any compatibility issues

### Sprint 5 (Week 7): UI & Polish
- ✅ Configuration updates
- ✅ Web UI integration
- ✅ Documentation
- ✅ User acceptance testing

---

## Testing Strategy

### Unit Tests (80%+ coverage)
- Type mapping correctness
- Schema generation with field IDs
- Avro wrapper behavior
- Catalog atomic commits

### Integration Tests
- Full SQL → Iceberg pipeline
- PyIceberg validation
- DuckDB queries
- Concurrent read/write

### E2E Tests
- Web UI workflow
- Profile saving/loading
- Error handling

---

## Key Implementation Notes

### Critical Success Factors

1. **Field-id Embedding**: Must use ParquetSharp GroupNode API
2. **Avro Schema Preservation**: Must use custom wrappers
3. **Atomic Commits**: Version hint file must use filesystem move
4. **External Validation**: Must validate with PyIceberg/DuckDB

### Common Pitfalls to Avoid

- ❌ Using high-level Parquet.Net API (can't embed field-id)
- ❌ Using standard Apache.Avro without wrappers (strips field-id)
- ❌ Non-atomic metadata updates (breaks consistency)
- ❌ Incorrect type mapping (causes data corruption)

### Technical Debt Considerations

- ParquetSharp requires native library deployment
- Avro wrapper is a workaround (may break on library updates)
- Filesystem catalog doesn't support distributed locking
- No partition evolution in initial implementation

---

## Future Enhancements

### Phase 2 Features (Post-MVP)
- Partition evolution support
- Schema evolution (add/rename columns)
- REST catalog integration (Tabular, AWS Glue)
- Incremental appends
- Time travel queries
- Table snapshots and rollback

### Phase 3 Features
- Merge-on-read (updates/deletes)
- Compaction operations
- Metadata-only operations
- Multi-table transactions

---

## References

- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [ParquetSharp Documentation](https://github.com/G-Research/ParquetSharp)
- [Apache Avro C# Documentation](https://avro.apache.org/docs/current/api/csharp/)
- [PyIceberg](https://py.iceberg.apache.org/)
- [Gemini Implementation Guide](gemini_-gemini-sql-to-iceberg-parquet-demo-plan_2025-10-05T21-05-25+0100.md)

---

## Appendix: SQL Server to Iceberg Type Mapping

| SQL Server Type | .NET Type | Iceberg Type | Parquet Type | Notes |
|----------------|-----------|--------------|--------------|-------|
| BigInt | long | long | INT64 | Direct mapping |
| Int | int | int | INT32 | Direct mapping |
| SmallInt | short | int | INT32 | Fits in int |
| TinyInt | byte | int | INT32 | Fits in int |
| Bit | bool | boolean | BOOLEAN | Direct mapping |
| Float | double | double | DOUBLE | Double precision |
| Real | float | float | FLOAT | Single precision |
| Decimal/Numeric | decimal | decimal(P,S) | FIXED_LEN_BYTE_ARRAY | Preserve P&S |
| Date | DateTime | date | INT32 | Days since epoch |
| DateTime2 | DateTime | timestamp | INT64 | Microseconds |
| DateTimeOffset | DateTimeOffset | timestamptz | INT64 | With timezone |
| NVarChar/VarChar | string | string | BYTE_ARRAY (UTF8) | All text types |
| VarBinary | byte[] | binary | BYTE_ARRAY | Binary data |
| UniqueIdentifier | Guid | uuid | FIXED_LEN_BYTE_ARRAY(16) | UUID type |

---

**Document Version**: 1.0
**Last Updated**: 2025-10-05
**Status**: Ready for Implementation
