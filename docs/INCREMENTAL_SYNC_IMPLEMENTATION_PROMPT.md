# Implementation Prompt: Incremental Sync for SQL Server ↔ Iceberg

## Context

You are implementing **incremental synchronization** between SQL Server databases using Apache Iceberg as an intermediate storage layer. The system already has:

✅ **Existing Functionality:**
- Full table export from SQL Server to Iceberg (`SqlServerToIcebergExporter`)
- Create new Iceberg tables with Parquet data (`IcebergTableWriter`)
- Iceberg v2 format compliance with field-ID preservation
- Filesystem catalog with atomic commits (`FilesystemCatalog`)
- Parquet writing with ParquetSharp (`IcebergParquetWriter`)
- Avro manifest generation (`ManifestFileGenerator`, `ManifestListGenerator`)
- Table metadata management (`TableMetadataGenerator`)
- Validation scripts (PyIceberg, DuckDB)

❌ **Missing (to be implemented):**
- Append new data to existing Iceberg tables (new snapshots)
- Read data from Iceberg Parquet files
- Detect changes in source SQL Server (watermark-based)
- Import data from Iceberg to target SQL Server
- Merge/upsert logic for target database
- Watermark storage and management
- End-to-end incremental sync orchestration

## Objective

Implement a complete **bidirectional incremental sync** system that:

1. **Detects changes** in source SQL Server (new/updated rows since last sync)
2. **Appends** incremental data to existing Iceberg table (new snapshot)
3. **Reads** data from Iceberg (specific snapshot or full table)
4. **Imports** data into target SQL Server with merge logic
5. **Tracks sync state** using watermarks (timestamps, IDs, snapshot IDs)

## Architecture

```
Source SQL Server              Iceberg Warehouse           Target SQL Server
┌──────────────┐              ┌─────────────────┐         ┌──────────────┐
│ Customers    │  Extract     │ customers/      │  Load   │ Customers    │
│ (1100 rows)  │──────────>   │ - Snapshot 1    │──────>  │ (1100 rows)  │
│              │  (100 new)   │ - Snapshot 2 ←  │ (Merge) │              │
└──────────────┘              └─────────────────┘         └──────────────┘
      │                               │                          │
      ├─ Watermark: 2024-10-10       ├─ Append capability       ├─ MERGE logic
      └─ Query: WHERE date > ?       └─ Time-travel support     └─ Upsert
```

## Implementation Phases

### **Phase 1: Iceberg Append Capability**

**Goal:** Enable appending new data to existing Iceberg tables (create new snapshots)

**Files to Create:**
- `src/DataTransfer.Iceberg/Integration/IcebergAppender.cs`
- `src/DataTransfer.Iceberg/Models/AppendResult.cs`

**Files to Modify:**
- `src/DataTransfer.Iceberg/Catalog/FilesystemCatalog.cs`
  - Add `AppendSnapshotAsync()` method
  - Load existing metadata, add new snapshot, commit v{N+1}
- `src/DataTransfer.Iceberg/Metadata/TableMetadataGenerator.cs`
  - Add `UpdateMetadataWithNewSnapshot()` method
  - Preserve existing snapshots, add new one

**Key Implementation Details:**

`IcebergAppender.cs`:
```csharp
public class IcebergAppender
{
    private readonly FilesystemCatalog _catalog;
    private readonly ILogger<IcebergAppender> _logger;

    public async Task<AppendResult> AppendAsync(
        string tableName,
        List<Dictionary<string, object>> newData,
        CancellationToken cancellationToken = default)
    {
        // 1. Load existing table metadata
        var existingMetadata = _catalog.LoadTable(tableName);
        if (existingMetadata == null)
            throw new InvalidOperationException($"Table {tableName} does not exist");

        // 2. Get schema from existing metadata
        var schema = existingMetadata.Schemas[0];

        // 3. Generate new snapshot ID
        var newSnapshotId = GenerateSnapshotId();

        // 4. Write new Parquet data files
        var dataFiles = await WriteDataFiles(tableName, schema, newData);

        // 5. Generate new manifest
        var manifestPath = GenerateManifest(tableName, dataFiles, newSnapshotId);

        // 6. Generate new manifest list
        var manifestListPath = GenerateManifestList(tableName, manifestPath, dataFiles);

        // 7. Update metadata with new snapshot (preserving old ones)
        var updatedMetadata = UpdateMetadata(existingMetadata, newSnapshotId, manifestListPath);

        // 8. Commit as new version (v{N+1}.metadata.json)
        await _catalog.AppendSnapshotAsync(tableName, updatedMetadata, cancellationToken);

        return new AppendResult
        {
            Success = true,
            NewSnapshotId = newSnapshotId,
            RowsAppended = newData.Count,
            DataFileCount = dataFiles.Count
        };
    }
}
```

**Tests to Write:**
- `IcebergAppenderTests.cs`:
  - `Should_Append_Data_To_Existing_Table()`
  - `Should_Create_New_Snapshot_With_Incremented_Version()`
  - `Should_Preserve_Previous_Snapshots()`
  - `Should_Update_Current_Snapshot_Id()`
  - `Should_Increment_Last_Sequence_Number()`
  - `Should_Handle_Empty_Append()`
  - `Should_Fail_If_Table_Does_Not_Exist()`

---

### **Phase 2: Iceberg Reader**

**Goal:** Read data from existing Iceberg tables (Parquet files)

**Files to Create:**
- `src/DataTransfer.Iceberg/Readers/IcebergReader.cs`
- `src/DataTransfer.Iceberg/Readers/IcebergParquetReader.cs`
- `src/DataTransfer.Iceberg/Models/ReadOptions.cs`

**Key Implementation Details:**

`IcebergReader.cs`:
```csharp
public class IcebergReader
{
    private readonly FilesystemCatalog _catalog;

    public async IAsyncEnumerable<Dictionary<string, object>> ReadTableAsync(
        string tableName,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // 1. Load table metadata
        var metadata = _catalog.LoadTable(tableName);
        var currentSnapshot = metadata.Snapshots.First(s => s.SnapshotId == metadata.CurrentSnapshotId);

        // 2. Read manifest list to get manifest files
        var manifestListPath = Path.Combine(_catalog.GetTablePath(tableName), "metadata", currentSnapshot.ManifestList);
        var manifestFiles = ReadManifestList(manifestListPath);

        // 3. Read each manifest to get data files
        var dataFiles = new List<string>();
        foreach (var manifestFile in manifestFiles)
        {
            dataFiles.AddRange(ReadManifest(manifestFile));
        }

        // 4. Read each Parquet data file and stream rows
        foreach (var dataFile in dataFiles)
        {
            await foreach (var row in ReadParquetFile(dataFile, metadata.Schemas[0], cancellationToken))
            {
                yield return row;
            }
        }
    }

    private async IAsyncEnumerable<Dictionary<string, object>> ReadParquetFile(
        string filePath,
        IcebergSchema schema,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var reader = new ParquetFileReader(filePath);
        var rowGroupCount = reader.FileMetaData.NumRowGroups;

        for (int rg = 0; rg < rowGroupCount; rg++)
        {
            using var rowGroupReader = reader.RowGroup(rg);

            // Read each column and reconstruct rows
            var rows = ReadRowGroup(rowGroupReader, schema);
            foreach (var row in rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return row;
            }
        }
    }
}
```

**Tests to Write:**
- `IcebergReaderTests.cs`:
  - `Should_Read_All_Rows_From_Table()`
  - `Should_Read_Specific_Snapshot()`
  - `Should_Handle_Multiple_Data_Files()`
  - `Should_Reconstruct_Rows_Correctly()`
  - `Should_Handle_Nullable_Fields()`
  - `Should_Support_Cancellation()`

---

### **Phase 3: Change Detection**

**Goal:** Identify new/changed rows in source SQL Server

**Files to Create:**
- `src/DataTransfer.Iceberg/ChangeDetection/IChangeDetectionStrategy.cs`
- `src/DataTransfer.Iceberg/ChangeDetection/TimestampChangeDetection.cs`
- `src/DataTransfer.Iceberg/ChangeDetection/IdBasedChangeDetection.cs`
- `src/DataTransfer.Iceberg/Models/IncrementalQuery.cs`
- `src/DataTransfer.Iceberg/Models/Watermark.cs`

**Key Implementation Details:**

`IChangeDetectionStrategy.cs`:
```csharp
public interface IChangeDetectionStrategy
{
    Task<IncrementalQuery> BuildIncrementalQueryAsync(
        string tableName,
        Watermark? lastWatermark,
        SqlConnection connection);
}
```

`TimestampChangeDetection.cs`:
```csharp
public class TimestampChangeDetection : IChangeDetectionStrategy
{
    private readonly string _watermarkColumn;

    public async Task<IncrementalQuery> BuildIncrementalQueryAsync(
        string tableName,
        Watermark? lastWatermark,
        SqlConnection connection)
    {
        string query;
        if (lastWatermark == null || !lastWatermark.LastSyncTimestamp.HasValue)
        {
            // First sync - get all rows
            query = $"SELECT * FROM {tableName}";
        }
        else
        {
            // Incremental - get rows modified after watermark
            query = $"SELECT * FROM {tableName} WHERE {_watermarkColumn} > @WatermarkValue";
        }

        return new IncrementalQuery
        {
            Sql = query,
            Parameters = lastWatermark != null
                ? new Dictionary<string, object> { ["@WatermarkValue"] = lastWatermark.LastSyncTimestamp.Value }
                : new Dictionary<string, object>()
        };
    }
}
```

**Tests to Write:**
- `ChangeDetectionTests.cs`:
  - `Should_Generate_Full_Query_For_First_Sync()`
  - `Should_Generate_Incremental_Query_With_Watermark()`
  - `Should_Handle_Different_Column_Types()`
  - `Should_Support_Composite_Keys()`

---

### **Phase 4: SQL Server Importer**

**Goal:** Load data from Iceberg into target SQL Server with merge logic

**Files to Create:**
- `src/DataTransfer.Iceberg/Integration/SqlServerImporter.cs`
- `src/DataTransfer.Iceberg/MergeStrategies/IMergeStrategy.cs`
- `src/DataTransfer.Iceberg/MergeStrategies/UpsertMergeStrategy.cs`
- `src/DataTransfer.Iceberg/MergeStrategies/AppendOnlyMergeStrategy.cs`
- `src/DataTransfer.Iceberg/Models/ImportResult.cs`

**Key Implementation Details:**

`SqlServerImporter.cs`:
```csharp
public class SqlServerImporter
{
    public async Task<ImportResult> ImportAsync(
        IAsyncEnumerable<Dictionary<string, object>> data,
        string connectionString,
        string tableName,
        IMergeStrategy mergeStrategy,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        // 1. Create temp table
        var tempTable = $"#Temp_{tableName}_{Guid.NewGuid():N}";
        await CreateTempTable(connection, tableName, tempTable, cancellationToken);

        // 2. Bulk copy data to temp table
        var rowCount = await BulkCopyToTemp(data, connection, tempTable, cancellationToken);

        // 3. Execute merge strategy
        var mergeResult = await mergeStrategy.MergeAsync(connection, tableName, tempTable, cancellationToken);

        return new ImportResult
        {
            Success = true,
            RowsImported = rowCount,
            RowsInserted = mergeResult.Inserted,
            RowsUpdated = mergeResult.Updated
        };
    }
}
```

`UpsertMergeStrategy.cs`:
```csharp
public class UpsertMergeStrategy : IMergeStrategy
{
    private readonly string _primaryKeyColumn;

    public async Task<MergeResult> MergeAsync(
        SqlConnection connection,
        string targetTable,
        string tempTable,
        CancellationToken cancellationToken)
    {
        var mergeSql = $@"
            MERGE {targetTable} AS target
            USING {tempTable} AS source
            ON target.{_primaryKeyColumn} = source.{_primaryKeyColumn}
            WHEN MATCHED THEN
                UPDATE SET {BuildUpdateColumns()}
            WHEN NOT MATCHED THEN
                INSERT ({BuildColumnList()})
                VALUES ({BuildValueList()});
        ";

        await using var command = new SqlCommand(mergeSql, connection);
        var affectedRows = await command.ExecuteNonQueryAsync(cancellationToken);

        return new MergeResult
        {
            Inserted = affectedRows,  // Simplified - could use OUTPUT clause for details
            Updated = 0
        };
    }
}
```

**Tests to Write:**
- `SqlServerImporterTests.cs`:
  - `Should_Import_Data_To_Target_Table()`
  - `Should_Use_Bulk_Copy_For_Performance()`
  - `Should_Execute_Merge_Strategy()`
  - `Should_Handle_Large_Datasets()`
- `UpsertMergeStrategyTests.cs`:
  - `Should_Update_Existing_Rows()`
  - `Should_Insert_New_Rows()`
  - `Should_Match_On_Primary_Key()`

---

### **Phase 5: Watermark Management**

**Goal:** Track sync state between runs

**Files to Create:**
- `src/DataTransfer.Iceberg/Watermarks/IWatermarkStore.cs`
- `src/DataTransfer.Iceberg/Watermarks/FileWatermarkStore.cs`
- `src/DataTransfer.Iceberg/Models/Watermark.cs`

**Key Implementation Details:**

`Watermark.cs`:
```csharp
public class Watermark
{
    public string TableName { get; set; }
    public DateTime? LastSyncTimestamp { get; set; }
    public long? LastSyncId { get; set; }
    public long? LastIcebergSnapshot { get; set; }
    public int RowCount { get; set; }
    public DateTime CreatedAt { get; set; }
}
```

`FileWatermarkStore.cs`:
```csharp
public class FileWatermarkStore : IWatermarkStore
{
    private readonly string _watermarkDirectory;

    public async Task<Watermark?> GetWatermarkAsync(string tableName)
    {
        var filePath = Path.Combine(_watermarkDirectory, $"{tableName}.json");
        if (!File.Exists(filePath))
            return null;

        var json = await File.ReadAllTextAsync(filePath);
        return JsonSerializer.Deserialize<Watermark>(json);
    }

    public async Task SetWatermarkAsync(string tableName, Watermark watermark)
    {
        var filePath = Path.Combine(_watermarkDirectory, $"{tableName}.json");
        var json = JsonSerializer.Serialize(watermark, new JsonSerializerOptions { WriteIndented = true });
        await File.WriteAllTextAsync(filePath, json);
    }
}
```

**Tests to Write:**
- `WatermarkStoreTests.cs`:
  - `Should_Store_And_Retrieve_Watermark()`
  - `Should_Return_Null_For_New_Table()`
  - `Should_Overwrite_Existing_Watermark()`
  - `Should_Handle_Concurrent_Access()`

---

### **Phase 6: Orchestration**

**Goal:** Coordinate the entire incremental sync workflow

**Files to Create:**
- `src/DataTransfer.Iceberg/Integration/IncrementalSyncCoordinator.cs`
- `src/DataTransfer.Iceberg/Models/SyncOptions.cs`
- `src/DataTransfer.Iceberg/Models/SyncResult.cs`

**Key Implementation Details:**

`IncrementalSyncCoordinator.cs`:
```csharp
public class IncrementalSyncCoordinator
{
    private readonly IChangeDetectionStrategy _changeDetection;
    private readonly IcebergAppender _appender;
    private readonly IcebergReader _reader;
    private readonly SqlServerImporter _importer;
    private readonly IWatermarkStore _watermarkStore;
    private readonly ILogger<IncrementalSyncCoordinator> _logger;

    public async Task<SyncResult> SyncAsync(
        string sourceConnection,
        string sourceTable,
        string icebergTable,
        string targetConnection,
        string targetTable,
        SyncOptions options,
        CancellationToken cancellationToken = default)
    {
        var startTime = DateTime.UtcNow;
        _logger.LogInformation("Starting incremental sync for {Table}", icebergTable);

        try
        {
            // 1. Get last watermark
            var lastWatermark = await _watermarkStore.GetWatermarkAsync(icebergTable);
            _logger.LogInformation("Last watermark: {Watermark}", lastWatermark?.LastSyncTimestamp);

            // 2. Extract changes from source
            await using var sourceConn = new SqlConnection(sourceConnection);
            await sourceConn.OpenAsync(cancellationToken);

            var query = await _changeDetection.BuildIncrementalQueryAsync(sourceTable, lastWatermark, sourceConn);
            var changes = await ExtractChanges(sourceConn, query, cancellationToken);

            if (changes.Count == 0)
            {
                _logger.LogInformation("No changes detected");
                return new SyncResult { Success = true, RowsExtracted = 0 };
            }

            _logger.LogInformation("Extracted {Count} changed rows", changes.Count);

            // 3. Append to Iceberg (or create new table if first sync)
            AppendResult appendResult;
            if (lastWatermark == null)
            {
                // First sync - create new table
                appendResult = await CreateInitialTable(icebergTable, changes, cancellationToken);
            }
            else
            {
                // Incremental sync - append
                appendResult = await _appender.AppendAsync(icebergTable, changes, cancellationToken);
            }

            _logger.LogInformation("Appended to Iceberg, snapshot: {Snapshot}", appendResult.NewSnapshotId);

            // 4. Read from Iceberg
            var data = _reader.ReadTableAsync(icebergTable, cancellationToken);

            // 5. Import to target
            var mergeStrategy = CreateMergeStrategy(options);
            var importResult = await _importer.ImportAsync(data, targetConnection, targetTable, mergeStrategy, cancellationToken);

            _logger.LogInformation("Imported {Count} rows to target", importResult.RowsImported);

            // 6. Update watermark
            var newWatermark = new Watermark
            {
                TableName = icebergTable,
                LastSyncTimestamp = DateTime.UtcNow,
                LastIcebergSnapshot = appendResult.NewSnapshotId,
                RowCount = changes.Count,
                CreatedAt = DateTime.UtcNow
            };
            await _watermarkStore.SetWatermarkAsync(icebergTable, newWatermark);

            return new SyncResult
            {
                Success = true,
                RowsExtracted = changes.Count,
                RowsAppended = appendResult.RowsAppended,
                RowsImported = importResult.RowsImported,
                NewSnapshotId = appendResult.NewSnapshotId,
                NewWatermark = newWatermark,
                Duration = DateTime.UtcNow - startTime
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Sync failed");
            return new SyncResult
            {
                Success = false,
                ErrorMessage = ex.Message,
                Duration = DateTime.UtcNow - startTime
            };
        }
    }
}
```

**Tests to Write:**
- `IncrementalSyncCoordinatorTests.cs`:
  - `Should_Complete_Full_Sync_Workflow()`
  - `Should_Handle_First_Sync_Differently()`
  - `Should_Update_Watermark_After_Success()`
  - `Should_Not_Update_Watermark_On_Failure()`
  - `Should_Support_Cancellation()`

---

### **Phase 7: Demo & Documentation**

**Files to Create:**
- `demo/06-incremental-sync-demo.sh` - Main demo script
- `demo/07-simulate-source-changes.sql` - Adds new rows to source
- `demo/08-verify-incremental-sync.sql` - Validates results
- `docs/INCREMENTAL_SYNC_GUIDE.md` - User guide

**Demo Script Flow:**

`06-incremental-sync-demo.sh`:
```bash
#!/bin/bash
# Demonstrates incremental sync

# 1. Setup Docker SQL Server
./demo/00-setup-sqlserver-docker.sh

# 2. Create initial datasets (1000 rows each)
docker exec sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P IcebergDemo@2024 -i /tmp/setup.sql -C

# 3. Initial sync (full export)
dotnet run --project src/IncrementalSyncDemo -- \
  --mode initial \
  --source "IcebergDemo_Source" \
  --table "Customers" \
  --target "IcebergDemo_Target"

echo "Initial sync complete. Snapshot 1 created."

# 4. Simulate changes in source (add 100 rows)
docker exec sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P IcebergDemo@2024 -i /tmp/add-changes.sql -C

echo "Added 100 new rows to source database"

# 5. Incremental sync (extract delta, append to Iceberg, merge to target)
dotnet run --project src/IncrementalSyncDemo -- \
  --mode incremental \
  --source "IcebergDemo_Source" \
  --table "Customers" \
  --target "IcebergDemo_Target"

echo "Incremental sync complete. Snapshot 2 created."

# 6. Verify results
docker exec sqlserver-iceberg-demo /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P IcebergDemo@2024 -i /tmp/verify.sql -C

# 7. Show Iceberg snapshots
echo "Iceberg snapshots:"
cat /tmp/iceberg-demo-warehouse/customers/metadata/v*.metadata.json | \
  jq '.snapshots[] | {snapshot_id, timestamp_ms, manifest_list}'
```

---

## Testing Requirements

### Integration Test: End-to-End Incremental Sync
```csharp
[Fact]
public async Task Should_Perform_Complete_Incremental_Sync()
{
    // Arrange
    var warehouse = CreateTempWarehouse();
    var sourceDb = CreateSourceDatabase();
    var targetDb = CreateTargetDatabase();

    await PopulateSourceDatabase(sourceDb, 1000); // Initial data

    var coordinator = new IncrementalSyncCoordinator(/* deps */);

    // Act 1: Initial sync
    var result1 = await coordinator.SyncAsync(
        sourceDb.ConnectionString,
        "Customers",
        "customers",
        targetDb.ConnectionString,
        "Customers",
        new SyncOptions { Mode = SyncMode.Initial }
    );

    // Assert 1
    Assert.True(result1.Success);
    Assert.Equal(1000, result1.RowsExtracted);
    Assert.Equal(1000, result1.RowsImported);
    Assert.Equal(1000, await CountRows(targetDb, "Customers"));

    // Act 2: Add changes to source
    await AddRowsToSource(sourceDb, 100);

    // Act 3: Incremental sync
    var result2 = await coordinator.SyncAsync(
        sourceDb.ConnectionString,
        "Customers",
        "customers",
        targetDb.ConnectionString,
        "Customers",
        new SyncOptions { Mode = SyncMode.Incremental }
    );

    // Assert 2
    Assert.True(result2.Success);
    Assert.Equal(100, result2.RowsExtracted);
    Assert.Equal(100, result2.RowsAppended);
    Assert.Equal(1100, await CountRows(targetDb, "Customers"));

    // Assert 3: Verify Iceberg has 2 snapshots
    var metadata = catalog.LoadTable("customers");
    Assert.Equal(2, metadata.Snapshots.Count);
}
```

## Implementation Guidelines

### TDD Approach (Follow strictly)
1. **RED**: Write failing test first
2. **GREEN**: Implement minimal code to pass
3. **REFACTOR**: Improve code while keeping tests green
4. Commit after each phase with `[RED]`, `[GREEN]`, `[REFACTOR]` tags

### Code Quality
- Follow existing code style (match `SqlServerToIcebergExporter.cs`)
- Use `ILogger<T>` for all logging
- Async/await throughout with `CancellationToken` support
- Proper `IDisposable` patterns for resources
- Null safety with nullable reference types

### Performance Considerations
- Use `SqlBulkCopy` for large imports (>1000 rows)
- Stream data with `IAsyncEnumerable<T>` to avoid loading all into memory
- Buffer Parquet reads (similar to `IcebergParquetWriter` buffer size)
- Use indexed columns for watermark queries

### Error Handling
- Wrap operations in try-catch with meaningful error messages
- Log errors with context (table name, row count, etc.)
- Return structured results (don't throw exceptions for business logic failures)
- Support retry logic for transient failures

## Success Criteria

✅ The implementation is complete when:
1. All 6 phases are implemented with passing tests
2. End-to-end integration test passes
3. Demo script successfully syncs 1000 → 1100 rows incrementally
4. Iceberg table has 2 snapshots after incremental sync
5. Target SQL Server has all 1100 rows after sync
6. Watermark is correctly stored and retrieved
7. Performance: Can sync 10,000 rows in < 10 seconds

## Additional Context

- **Existing codebase location:** `/home/richard/sonnet45/src/DataTransfer.Iceberg/`
- **Test location:** `/home/richard/sonnet45/tests/DataTransfer.Iceberg.Tests/`
- **Demo location:** `/home/richard/sonnet45/demo/`
- **Documentation:** `/home/richard/sonnet45/docs/`
- **NuGet packages already installed:**
  - ParquetSharp 20.0.0
  - Apache.Avro 1.11.3
  - Microsoft.Data.SqlClient 5.2.0
  - Microsoft.Extensions.Logging.Abstractions 8.0.0

## Start Here

Begin with **Phase 1: Iceberg Append Capability** as it's the foundation for everything else.

Create the test file first:
```csharp
// tests/DataTransfer.Iceberg.Tests/Integration/IcebergAppenderTests.cs
[Fact]
public async Task Should_Append_Data_To_Existing_Table()
{
    // RED phase - this will fail initially
    var appender = new IcebergAppender(_catalog, _logger);

    // Create initial table with 5 rows
    var initialData = CreateSampleData(5);
    await _writer.WriteTableAsync("test_table", _schema, initialData);

    // Append 3 more rows
    var appendData = CreateSampleData(3);
    var result = await appender.AppendAsync("test_table", appendData);

    // Assert
    Assert.True(result.Success);
    Assert.Equal(3, result.RowsAppended);

    // Verify metadata has 2 snapshots
    var metadata = _catalog.LoadTable("test_table");
    Assert.Equal(2, metadata.Snapshots.Count);
}
```

Then implement `IcebergAppender` to make it pass (GREEN phase).

Good luck! 🚀
