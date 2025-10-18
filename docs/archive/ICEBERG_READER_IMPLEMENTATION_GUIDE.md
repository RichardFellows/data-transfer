# Iceberg Reader Implementation Guide

## Supplemental Documentation for Phase 2

This document provides detailed implementation guidance for reading data from Iceberg tables, filling in the gaps from the main implementation prompt.

---

## Table of Contents
1. [Avro Manifest Reading](#avro-manifest-reading)
2. [Parquet Data File Reading](#parquet-data-file-reading)
3. [Row Reconstruction Logic](#row-reconstruction-logic)
4. [Complete Implementation Example](#complete-implementation-example)

---

## 1. Avro Manifest Reading

### Understanding the Manifest Structure

Based on `ManifestFileGenerator.cs` (lines 16-68), Iceberg manifests use this Avro schema:

```json
{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    {
      "name": "status",
      "type": "int",
      "field-id": 0
    },
    {
      "name": "snapshot_id",
      "type": ["null", "long"],
      "default": null,
      "field-id": 1
    },
    {
      "name": "data_file",
      "type": {
        "type": "record",
        "name": "data_file",
        "fields": [
          {
            "name": "file_path",
            "type": "string",
            "field-id": 100
          },
          {
            "name": "file_format",
            "type": "string",
            "field-id": 101
          },
          {
            "name": "partition",
            "type": ["null", {"type": "map", "values": "string"}],
            "default": null,
            "field-id": 102
          },
          {
            "name": "record_count",
            "type": "long",
            "field-id": 103
          },
          {
            "name": "file_size_in_bytes",
            "type": "long",
            "field-id": 104
          }
        ]
      },
      "field-id": 2
    }
  ]
}
```

### Manifest List Schema

Based on `ManifestListGenerator.cs`, manifest lists have this structure:

```json
{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {
      "name": "manifest_path",
      "type": "string",
      "field-id": 500
    },
    {
      "name": "manifest_length",
      "type": "long",
      "field-id": 501
    },
    {
      "name": "partition_spec_id",
      "type": "int",
      "field-id": 502
    },
    {
      "name": "added_files_count",
      "type": "int",
      "field-id": 511
    }
  ]
}
```

### Reading Manifest List Files

```csharp
/// <summary>
/// Reads manifest list Avro file and returns paths to manifest files
/// </summary>
private List<string> ReadManifestList(string manifestListPath)
{
    var manifestPaths = new List<string>();

    // Use Apache.Avro's DataFileReader to read Avro files
    using (var reader = DataFileReader<GenericRecord>.OpenReader(manifestListPath))
    {
        while (reader.HasNext())
        {
            var record = reader.Next();

            // Extract manifest_path field
            var manifestPath = record["manifest_path"] as string;

            if (!string.IsNullOrEmpty(manifestPath))
            {
                manifestPaths.Add(manifestPath);
            }
        }
    }

    _logger.LogDebug("Read {Count} manifest files from manifest list {Path}",
        manifestPaths.Count, manifestListPath);

    return manifestPaths;
}
```

### Reading Manifest Files

```csharp
/// <summary>
/// Reads manifest Avro file and returns paths to data files
/// </summary>
private List<string> ReadManifest(string tablePath, string manifestRelativePath)
{
    var dataFilePaths = new List<string>();
    var manifestPath = Path.Combine(tablePath, manifestRelativePath);

    using (var reader = DataFileReader<GenericRecord>.OpenReader(manifestPath))
    {
        while (reader.HasNext())
        {
            var entry = reader.Next();

            // Extract the nested data_file record
            var dataFile = entry["data_file"] as GenericRecord;

            if (dataFile != null)
            {
                // Extract file_path from the nested record
                var filePath = dataFile["file_path"] as string;

                if (!string.IsNullOrEmpty(filePath))
                {
                    dataFilePaths.Add(filePath);
                }
            }
        }
    }

    _logger.LogDebug("Read {Count} data files from manifest {Path}",
        dataFilePaths.Count, manifestRelativePath);

    return dataFilePaths;
}
```

### Key Points for Avro Reading

1. **Use `DataFileReader<GenericRecord>`** from `Apache.Avro` library
2. **Access fields by name** using dictionary indexer: `record["field_name"]`
3. **Handle nested records** by casting to `GenericRecord`
4. **Paths in manifests are relative** - combine with table path to get absolute paths
5. **Always dispose readers** with `using` statements

---

## 2. Parquet Data File Reading

### Understanding Parquet Column Storage

Parquet stores data in columns, not rows. To reconstruct rows, you must:
1. Read all columns from a row group
2. Transpose the columnar data into rows
3. Map column values to field names

### Key Classes from ParquetSharp

- `ParquetFileReader` - Opens Parquet files
- `RowGroupReader` - Reads a row group
- `ColumnReader` - Reads a single column
- `LogicalReader<T>` - Typed reader for column values

### Reading Parquet Files with Row Reconstruction

```csharp
/// <summary>
/// Reads a Parquet data file and streams rows as dictionaries
/// </summary>
private async IAsyncEnumerable<Dictionary<string, object>> ReadParquetFile(
    string filePath,
    IcebergSchema schema,
    [EnumeratorCancellation] CancellationToken cancellationToken)
{
    using var reader = new ParquetFileReader(filePath);
    var rowGroupCount = reader.FileMetaData.NumRowGroups;

    _logger.LogDebug("Reading Parquet file {Path} with {RowGroups} row groups",
        filePath, rowGroupCount);

    for (int rg = 0; rg < rowGroupCount; rg++)
    {
        cancellationToken.ThrowIfCancellationRequested();

        using var rowGroupReader = reader.RowGroup(rg);
        var rowCount = rowGroupReader.MetaData.NumRows;

        // Read all columns into arrays
        var columnData = new List<(string FieldName, Array Values)>();

        for (int colIndex = 0; colIndex < schema.Fields.Count; colIndex++)
        {
            var field = schema.Fields[colIndex];
            using var columnReader = rowGroupReader.Column(colIndex);

            var values = ReadColumnValues(columnReader, field, rowCount);
            columnData.Add((field.Name, values));
        }

        // Reconstruct rows from columnar data
        for (long rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var row = new Dictionary<string, object>();

            foreach (var (fieldName, values) in columnData)
            {
                row[fieldName] = values.GetValue(rowIndex)!;
            }

            yield return row;
        }
    }
}
```

### Reading Column Values by Type

```csharp
/// <summary>
/// Reads all values from a column based on Iceberg field type
/// </summary>
private Array ReadColumnValues(ColumnReader columnReader, IcebergField field, long rowCount)
{
    var fieldType = field.Type;

    // Handle JsonElement from deserialization
    if (fieldType is System.Text.Json.JsonElement jsonElement &&
        jsonElement.ValueKind == System.Text.Json.JsonValueKind.String)
    {
        fieldType = jsonElement.GetString()!;
    }

    if (fieldType is not string primitiveType)
    {
        throw new NotSupportedException($"Complex type reading not yet supported: {fieldType}");
    }

    switch (primitiveType)
    {
        case "boolean":
            return field.Required
                ? columnReader.LogicalReader<bool>().ReadAll((int)rowCount)
                : columnReader.LogicalReader<bool?>().ReadAll((int)rowCount);

        case "int":
            return field.Required
                ? columnReader.LogicalReader<int>().ReadAll((int)rowCount)
                : columnReader.LogicalReader<int?>().ReadAll((int)rowCount);

        case "long":
            return field.Required
                ? columnReader.LogicalReader<long>().ReadAll((int)rowCount)
                : columnReader.LogicalReader<long?>().ReadAll((int)rowCount);

        case "float":
            return field.Required
                ? columnReader.LogicalReader<float>().ReadAll((int)rowCount)
                : columnReader.LogicalReader<float?>().ReadAll((int)rowCount);

        case "double":
            return field.Required
                ? columnReader.LogicalReader<double>().ReadAll((int)rowCount)
                : columnReader.LogicalReader<double?>().ReadAll((int)rowCount);

        case "string":
            // Strings are always nullable in Parquet (reference type)
            return columnReader.LogicalReader<string>().ReadAll((int)rowCount);

        case "date":
            // Iceberg dates are stored as int32 (days since epoch)
            return field.Required
                ? columnReader.LogicalReader<DateTime>().ReadAll((int)rowCount)
                : columnReader.LogicalReader<DateTime?>().ReadAll((int)rowCount);

        case "timestamp":
        case "timestamptz":
            // Iceberg timestamps are stored as int64 microseconds
            return field.Required
                ? columnReader.LogicalReader<DateTime>().ReadAll((int)rowCount)
                : columnReader.LogicalReader<DateTime?>().ReadAll((int)rowCount);

        case "binary":
            return columnReader.LogicalReader<byte[]>().ReadAll((int)rowCount);

        case "uuid":
            // UUIDs stored as FixedLenByteArray(16)
            var byteArrays = columnReader.LogicalReader<byte[]>().ReadAll((int)rowCount);
            var guids = new object[byteArrays.Length];
            for (int i = 0; i < byteArrays.Length; i++)
            {
                guids[i] = byteArrays[i] != null ? new Guid(byteArrays[i]) : (object)null!;
            }
            return guids;

        default:
            throw new NotSupportedException($"Iceberg type {primitiveType} is not supported for Parquet reading");
    }
}
```

### Key Points for Parquet Reading

1. **Parquet stores data column-wise** - read all columns first, then reconstruct rows
2. **Use `LogicalReader<T>`** for type-safe column reading
3. **Handle required vs optional fields** - use nullable types for optional fields
4. **`ReadAll()` is more efficient** than reading values one-by-one
5. **Dispose all readers** properly to avoid memory leaks

---

## 3. Row Reconstruction Logic

### Column-to-Row Transposition

```csharp
/// <summary>
/// Transpose columnar data into row dictionaries
/// This is the core of the Parquet reading logic
/// </summary>
private IEnumerable<Dictionary<string, object>> TransposeColumnsToRows(
    IcebergSchema schema,
    List<(string FieldName, Array Values)> columnData,
    long rowCount)
{
    for (long rowIndex = 0; rowIndex < rowCount; rowIndex++)
    {
        var row = new Dictionary<string, object>();

        foreach (var (fieldName, values) in columnData)
        {
            var value = values.GetValue(rowIndex);
            row[fieldName] = value!;
        }

        yield return row;
    }
}
```

### Preserving Field Order

To maintain field order as defined in the schema:

```csharp
/// <summary>
/// Creates row dictionary preserving schema field order
/// </summary>
private Dictionary<string, object> CreateOrderedRow(
    IcebergSchema schema,
    List<(string FieldName, Array Values)> columnData,
    long rowIndex)
{
    // Use regular Dictionary - insertion order is preserved in .NET Core 2.1+
    var row = new Dictionary<string, object>();

    // Iterate through schema fields to maintain order
    for (int i = 0; i < schema.Fields.Count; i++)
    {
        var fieldName = schema.Fields[i].Name;
        var values = columnData[i].Values;
        var value = values.GetValue(rowIndex);

        row[fieldName] = value!;
    }

    return row;
}
```

### Handling Null Values

```csharp
/// <summary>
/// Safely gets value from array, handling nulls
/// </summary>
private object? GetValueOrNull(Array values, long index)
{
    var value = values.GetValue(index);

    // Handle DBNull from Parquet (shouldn't happen with ParquetSharp, but be safe)
    if (value == DBNull.Value)
    {
        return null;
    }

    return value;
}
```

---

## 4. Complete Implementation Example

### IcebergParquetReader.cs (New File)

```csharp
using DataTransfer.Core.Models.Iceberg;
using Microsoft.Extensions.Logging;
using ParquetSharp;
using System.Runtime.CompilerServices;

namespace DataTransfer.Iceberg.Readers;

/// <summary>
/// Reads Parquet data files and reconstructs rows
/// </summary>
public class IcebergParquetReader
{
    private readonly ILogger<IcebergParquetReader> _logger;

    public IcebergParquetReader(ILogger<IcebergParquetReader> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Reads all rows from a Parquet file as dictionaries
    /// </summary>
    public async IAsyncEnumerable<Dictionary<string, object>> ReadAsync(
        string filePath,
        IcebergSchema schema,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var reader = new ParquetFileReader(filePath);
        var rowGroupCount = reader.FileMetaData.NumRowGroups;

        for (int rg = 0; rg < rowGroupCount; rg++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using var rowGroupReader = reader.RowGroup(rg);
            var rowCount = rowGroupReader.MetaData.NumRows;

            // Read all columns
            var columnData = new List<Array>();
            for (int colIndex = 0; colIndex < schema.Fields.Count; colIndex++)
            {
                var field = schema.Fields[colIndex];
                using var columnReader = rowGroupReader.Column(colIndex);
                var values = ReadColumnValues(columnReader, field, rowCount);
                columnData.Add(values);
            }

            // Reconstruct rows
            for (long rowIndex = 0; rowIndex < rowCount; rowIndex++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var row = new Dictionary<string, object>();
                for (int colIndex = 0; colIndex < schema.Fields.Count; colIndex++)
                {
                    var fieldName = schema.Fields[colIndex].Name;
                    var value = columnData[colIndex].GetValue(rowIndex);
                    row[fieldName] = value!;
                }

                yield return row;
            }
        }

        await Task.CompletedTask; // Satisfy async method signature
    }

    private Array ReadColumnValues(ColumnReader columnReader, IcebergField field, long rowCount)
    {
        var fieldType = field.Type;

        // Handle JsonElement from deserialization
        if (fieldType is System.Text.Json.JsonElement jsonElement &&
            jsonElement.ValueKind == System.Text.Json.JsonValueKind.String)
        {
            fieldType = jsonElement.GetString()!;
        }

        if (fieldType is not string primitiveType)
        {
            throw new NotSupportedException($"Complex types not yet supported: {fieldType}");
        }

        switch (primitiveType)
        {
            case "boolean":
                return field.Required
                    ? columnReader.LogicalReader<bool>().ReadAll((int)rowCount)
                    : columnReader.LogicalReader<bool?>().ReadAll((int)rowCount);

            case "int":
                return field.Required
                    ? columnReader.LogicalReader<int>().ReadAll((int)rowCount)
                    : columnReader.LogicalReader<int?>().ReadAll((int)rowCount);

            case "long":
                return field.Required
                    ? columnReader.LogicalReader<long>().ReadAll((int)rowCount)
                    : columnReader.LogicalReader<long?>().ReadAll((int)rowCount);

            case "float":
                return field.Required
                    ? columnReader.LogicalReader<float>().ReadAll((int)rowCount)
                    : columnReader.LogicalReader<float?>().ReadAll((int)rowCount);

            case "double":
                return field.Required
                    ? columnReader.LogicalReader<double>().ReadAll((int)rowCount)
                    : columnReader.LogicalReader<double?>().ReadAll((int)rowCount);

            case "string":
                return columnReader.LogicalReader<string>().ReadAll((int)rowCount);

            case "date":
            case "timestamp":
            case "timestamptz":
                return field.Required
                    ? columnReader.LogicalReader<DateTime>().ReadAll((int)rowCount)
                    : columnReader.LogicalReader<DateTime?>().ReadAll((int)rowCount);

            case "binary":
                return columnReader.LogicalReader<byte[]>().ReadAll((int)rowCount);

            default:
                throw new NotSupportedException($"Type {primitiveType} not supported for reading");
        }
    }
}
```

### Complete IcebergReader.cs

```csharp
using Avro.File;
using Avro.Generic;
using DataTransfer.Iceberg.Catalog;
using Microsoft.Extensions.Logging;
using System.Runtime.CompilerServices;

namespace DataTransfer.Iceberg.Readers;

public class IcebergReader
{
    private readonly FilesystemCatalog _catalog;
    private readonly ILogger<IcebergReader> _logger;

    public IcebergReader(FilesystemCatalog catalog, ILogger<IcebergReader> logger)
    {
        _catalog = catalog;
        _logger = logger;
    }

    public async IAsyncEnumerable<Dictionary<string, object>> ReadTableAsync(
        string tableName,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // 1. Load table metadata
        var metadata = _catalog.LoadTable(tableName);
        if (metadata == null)
        {
            throw new InvalidOperationException($"Table {tableName} does not exist");
        }

        // Handle empty table (no snapshots)
        if (metadata.CurrentSnapshotId == null)
        {
            _logger.LogDebug("Table {Table} has no data (no current snapshot)", tableName);
            yield break;
        }

        var currentSnapshot = metadata.Snapshots.First(s => s.SnapshotId == metadata.CurrentSnapshotId);
        var schema = metadata.Schemas.First(s => s.SchemaId == metadata.CurrentSchemaId);

        // 2. Read manifest list
        var tablePath = _catalog.GetTablePath(tableName);
        var manifestListPath = Path.Combine(tablePath, "metadata", currentSnapshot.ManifestList);
        var manifestPaths = ReadManifestList(manifestListPath);

        // 3. Read all manifests to get data files
        var dataFiles = new List<string>();
        foreach (var manifestPath in manifestPaths)
        {
            dataFiles.AddRange(ReadManifest(tablePath, manifestPath));
        }

        // 4. Read all Parquet data files
        var parquetReader = new IcebergParquetReader(
            LoggerFactory.Create(b => {}).CreateLogger<IcebergParquetReader>());

        foreach (var dataFile in dataFiles)
        {
            var dataFilePath = Path.Combine(tablePath, dataFile);
            await foreach (var row in parquetReader.ReadAsync(dataFilePath, schema, cancellationToken))
            {
                yield return row;
            }
        }
    }

    public async IAsyncEnumerable<Dictionary<string, object>> ReadSnapshotAsync(
        string tableName,
        long snapshotId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Similar to ReadTableAsync but use specific snapshot
        var metadata = _catalog.LoadTable(tableName);
        if (metadata == null)
        {
            throw new InvalidOperationException($"Table {tableName} does not exist");
        }

        var snapshot = metadata.Snapshots.FirstOrDefault(s => s.SnapshotId == snapshotId);
        if (snapshot == null)
        {
            throw new InvalidOperationException($"Snapshot {snapshotId} not found in table {tableName}");
        }

        var schema = metadata.Schemas.First(s => s.SchemaId == metadata.CurrentSchemaId);
        var tablePath = _catalog.GetTablePath(tableName);
        var manifestListPath = Path.Combine(tablePath, "metadata", snapshot.ManifestList);
        var manifestPaths = ReadManifestList(manifestListPath);

        var dataFiles = new List<string>();
        foreach (var manifestPath in manifestPaths)
        {
            dataFiles.AddRange(ReadManifest(tablePath, manifestPath));
        }

        var parquetReader = new IcebergParquetReader(
            LoggerFactory.Create(b => {}).CreateLogger<IcebergParquetReader>());

        foreach (var dataFile in dataFiles)
        {
            var dataFilePath = Path.Combine(tablePath, dataFile);
            await foreach (var row in parquetReader.ReadAsync(dataFilePath, schema, cancellationToken))
            {
                yield return row;
            }
        }
    }

    private List<string> ReadManifestList(string manifestListPath)
    {
        var manifestPaths = new List<string>();

        using (var reader = DataFileReader<GenericRecord>.OpenReader(manifestListPath))
        {
            while (reader.HasNext())
            {
                var record = reader.Next();
                var manifestPath = record["manifest_path"] as string;

                if (!string.IsNullOrEmpty(manifestPath))
                {
                    manifestPaths.Add(manifestPath);
                }
            }
        }

        return manifestPaths;
    }

    private List<string> ReadManifest(string tablePath, string manifestRelativePath)
    {
        var dataFilePaths = new List<string>();
        var manifestPath = Path.Combine(tablePath, manifestRelativePath);

        using (var reader = DataFileReader<GenericRecord>.OpenReader(manifestPath))
        {
            while (reader.HasNext())
            {
                var entry = reader.Next();
                var dataFile = entry["data_file"] as GenericRecord;

                if (dataFile != null)
                {
                    var filePath = dataFile["file_path"] as string;
                    if (!string.IsNullOrEmpty(filePath))
                    {
                        dataFilePaths.Add(filePath);
                    }
                }
            }
        }

        return dataFilePaths;
    }
}
```

---

## 5. Testing Strategy

### Unit Tests for Parquet Reader

```csharp
[Fact]
public async Task Should_Read_Parquet_File_With_Multiple_Types()
{
    // Arrange
    var schema = new IcebergSchema
    {
        SchemaId = 0,
        Type = "struct",
        Fields = new List<IcebergField>
        {
            new() { Id = 1, Name = "id", Required = true, Type = "int" },
            new() { Id = 2, Name = "name", Required = false, Type = "string" },
            new() { Id = 3, Name = "amount", Required = true, Type = "double" },
            new() { Id = 4, Name = "created_at", Required = true, Type = "timestamp" }
        }
    };

    // First write a file
    var data = new List<Dictionary<string, object>>
    {
        new() { ["id"] = 1, ["name"] = "Alice", ["amount"] = 100.50, ["created_at"] = DateTime.UtcNow },
        new() { ["id"] = 2, ["name"] = null!, ["amount"] = 200.75, ["created_at"] = DateTime.UtcNow }
    };

    var writer = new IcebergParquetWriter(_testFilePath, schema);
    foreach (var row in data)
    {
        var values = schema.Fields.Select(f => row[f.Name]).ToArray();
        writer.WriteRow(values);
    }
    writer.Close();

    // Act - Read it back
    var parquetReader = new IcebergParquetReader(NullLogger<IcebergParquetReader>.Instance);
    var rows = new List<Dictionary<string, object>>();

    await foreach (var row in parquetReader.ReadAsync(_testFilePath, schema))
    {
        rows.Add(row);
    }

    // Assert
    Assert.Equal(2, rows.Count);
    Assert.Equal(1, rows[0]["id"]);
    Assert.Equal("Alice", rows[0]["name"]);
    Assert.Equal(100.50, (double)rows[0]["amount"], precision: 2);
    Assert.Null(rows[1]["name"]);
}
```

### Integration Test for Full Read Path

```csharp
[Fact]
public async Task Should_Read_Through_Manifest_Chain()
{
    // Arrange - Create table with writer
    var schema = CreateSimpleSchema();
    var data = CreateSampleData(100);
    await _writer.WriteTableAsync("full_chain_test", schema, data);

    // Act - Read through IcebergReader
    var reader = new IcebergReader(_catalog, NullLogger<IcebergReader>.Instance);
    var rows = new List<Dictionary<string, object>>();

    await foreach (var row in reader.ReadTableAsync("full_chain_test"))
    {
        rows.Add(row);
    }

    // Assert
    Assert.Equal(100, rows.Count);

    // Verify first and last rows
    Assert.Equal(1, rows[0]["id"]);
    Assert.Equal("row_1", rows[0]["value"]);
    Assert.Equal(100, rows[99]["id"]);
    Assert.Equal("row_100", rows[99]["value"]);
}
```

---

## 6. Performance Considerations

### Batch Reading Strategy

```csharp
// Good: Read entire row group at once
var values = columnReader.LogicalReader<int>().ReadAll(rowCount);

// Bad: Read values one by one (slow)
for (int i = 0; i < rowCount; i++)
{
    var value = columnReader.LogicalReader<int>().Read();
}
```

### Memory Management

```csharp
// Always dispose readers
using var fileReader = new ParquetFileReader(path);
using var rowGroupReader = fileReader.RowGroup(0);
using var columnReader = rowGroupReader.Column(0);

// For large files, process row groups incrementally
for (int rg = 0; rg < rowGroupCount; rg++)
{
    using var rowGroup = fileReader.RowGroup(rg);
    // Process and yield rows
    // GC can collect previous row group data
}
```

### Streaming vs Buffering

```csharp
// Good: Stream rows one at a time (memory efficient)
await foreach (var row in ReadTableAsync(tableName))
{
    await ProcessRowAsync(row);
}

// Bad: Load all into memory first
var allRows = await ReadTableAsync(tableName).ToListAsync();
```

---

## 7. Error Handling

### Graceful Degradation

```csharp
try
{
    using var reader = DataFileReader<GenericRecord>.OpenReader(manifestPath);
    // Read manifest
}
catch (FileNotFoundException ex)
{
    _logger.LogError(ex, "Manifest file not found: {Path}", manifestPath);
    throw new InvalidOperationException($"Manifest file missing: {manifestPath}", ex);
}
catch (Avro.AvroException ex)
{
    _logger.LogError(ex, "Failed to parse manifest file: {Path}", manifestPath);
    throw new InvalidOperationException($"Corrupt manifest file: {manifestPath}", ex);
}
```

### Type Mismatch Handling

```csharp
private Array ReadColumnValues(ColumnReader columnReader, IcebergField field, long rowCount)
{
    try
    {
        // Read column logic
    }
    catch (InvalidCastException ex)
    {
        _logger.LogError(ex,
            "Type mismatch reading field {Field}: expected {Expected}",
            field.Name, field.Type);
        throw new InvalidOperationException(
            $"Schema mismatch for field {field.Name}", ex);
    }
}
```

---

## Summary

This guide provides the missing implementation details for Phase 2:

✅ **Avro Reading:**
- Use `DataFileReader<GenericRecord>` from Apache.Avro
- Access fields by name with dictionary syntax
- Handle nested records for data file entries

✅ **Parquet Reading:**
- Use `ParquetFileReader` and `LogicalReader<T>`
- Read columns in bulk with `ReadAll()`
- Transpose columnar data into rows

✅ **Row Reconstruction:**
- Maintain field order from schema
- Handle nullable fields correctly
- Stream data efficiently with `IAsyncEnumerable`

With this guide, Phase 2 implementation should be straightforward. The patterns mirror the existing write code, just in reverse.
