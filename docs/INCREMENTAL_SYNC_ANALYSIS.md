# Incremental Sync Analysis & Implementation Plan

## Current State Analysis

### ✅ What Currently Exists

**1. Export (SQL Server → Iceberg)**
- `SqlServerToIcebergExporter` - Full table export from SQL Server
- `IcebergTableWriter` - Writes complete datasets to NEW Iceberg tables
- `IcebergParquetWriter` - Writes Parquet files with field IDs
- Automatic schema inference from `SqlDataReader`
- Type mapping (SQL Server → Iceberg → Parquet)

**2. Iceberg Catalog Management**
- `FilesystemCatalog`:
  - `InitializeTable()` - Creates table directory structure
  - `CommitAsync()` - Atomic commits with version-hint.txt
  - `LoadTable()` - Reads existing table metadata
  - `TableExists()` - Checks if table exists
  - `GetNextVersion()` - Determines next snapshot version

**3. Metadata Generation**
- `ManifestFileGenerator` - Creates Avro manifest files
- `ManifestListGenerator` - Creates manifest list files
- `TableMetadataGenerator` - Generates v{N}.metadata.json
- Full Iceberg v2 format compliance

**4. Validation & Querying**
- Validation scripts (PyIceberg, DuckDB)
- Query examples with DuckDB

### ❌ What's Missing for Incremental Sync

**1. Append to Existing Iceberg Tables**
- Current: `IcebergTableWriter.WriteTableAsync()` creates NEW tables only
- Missing: Append new data to existing table (new snapshot)
- Missing: Read existing metadata to determine next snapshot

**2. Change Detection in SQL Server**
- Missing: Identify new/changed rows since last sync
- Missing: Watermark/checkpoint tracking
- Missing: Query patterns for incremental extraction

**3. Import from Iceberg to SQL Server**
- Missing: Read Parquet files from Iceberg
- Missing: Convert Iceberg data back to SQL Server
- Missing: Bulk insert or merge into destination

**4. Sync State Management**
- Missing: Track what's been synced
- Missing: Store watermarks (timestamps, IDs, etc.)
- Missing: Handle failures and resume

**5. Merge/Upsert Logic**
- Missing: Identify primary keys
- Missing: Update existing rows vs insert new rows
- Missing: Handle deletions (if needed)

## Proposed Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Incremental Sync Workflow                     │
└─────────────────────────────────────────────────────────────────┘

Source SQL Server                 Iceberg Warehouse              Target SQL Server
┌───────────────┐                ┌──────────────────┐           ┌───────────────┐
│  Customers    │                │  customers/      │           │  Customers    │
│  - 1000 rows  │                │  - data/         │           │  - 900 rows   │
│  - Updated    │   Extract      │  - metadata/     │  Load     │  - Outdated   │
│    today      │─────────────>  │  - Snapshots:    │────────>  │               │
│               │   (Delta)      │    * v1 (900)    │  (Merge)  │               │
│               │                │    * v2 (100) ←  │           │               │
└───────────────┘                └──────────────────┘           └───────────────┘
        │                                  │                            │
        ├─ Change Detection                ├─ Append New Snapshot      ├─ Merge Logic
        ├─ Watermark: 2024-10-10          ├─ Time Travel Support      ├─ Update/Insert
        └─ Extract: rows > 2024-10-10     └─ Manifest Management      └─ Primary Key Match
```

## Required Components

### 1. **IncrementalSyncCoordinator** (NEW)
Main orchestrator for the entire sync process.

**Responsibilities:**
- Coordinate extract → append → load workflow
- Manage sync state (watermarks)
- Handle errors and retries
- Logging and metrics

**Interface:**
```csharp
public class IncrementalSyncCoordinator
{
    Task<SyncResult> SyncAsync(
        string sourceConnection,
        string sourceTable,
        string icebergTable,
        string targetConnection,
        string targetTable,
        SyncOptions options
    );
}
```

### 2. **ChangeDetectionStrategy** (NEW)
Identifies what data needs to be synced from source.

**Strategies:**
- **Timestamp-based**: Extract rows where `ModifiedDate > watermark`
- **ID-based**: Extract rows where `ID > max_synced_id`
- **Change Tracking**: Use SQL Server Change Tracking feature
- **Full refresh**: Extract everything (fallback)

**Interface:**
```csharp
public interface IChangeDetectionStrategy
{
    Task<IncrementalQuery> BuildQueryAsync(
        string tableName,
        Watermark lastWatermark
    );
}
```

### 3. **IcebergAppender** (NEW)
Appends new data to existing Iceberg tables.

**Responsibilities:**
- Load existing table metadata
- Generate new snapshot ID
- Write new Parquet files (incremental data only)
- Create new manifest
- Update metadata with new snapshot
- Commit atomically

**Key Method:**
```csharp
public class IcebergAppender
{
    Task<AppendResult> AppendAsync(
        string tableName,
        List<Dictionary<string, object>> newData,
        CancellationToken cancellationToken
    );
}
```

### 4. **IcebergReader** (NEW)
Reads data from Iceberg tables.

**Responsibilities:**
- Read specific snapshots (time travel)
- Read all data or specific partitions
- Stream Parquet files
- Convert to in-memory representation

**Interface:**
```csharp
public class IcebergReader
{
    IAsyncEnumerable<Dictionary<string, object>> ReadAsync(
        string tableName,
        ReadOptions options
    );

    IAsyncEnumerable<Dictionary<string, object>> ReadSnapshotAsync(
        string tableName,
        long snapshotId
    );
}
```

### 5. **SqlServerImporter** (NEW)
Loads data from Iceberg into SQL Server.

**Responsibilities:**
- Read data from IcebergReader
- Batch into efficient chunks
- Generate MERGE/INSERT/UPDATE SQL
- Use SqlBulkCopy for performance
- Handle errors and retries

**Interface:**
```csharp
public class SqlServerImporter
{
    Task<ImportResult> ImportAsync(
        IAsyncEnumerable<Dictionary<string, object>> data,
        string connectionString,
        string tableName,
        ImportOptions options
    );
}
```

### 6. **WatermarkStore** (NEW)
Tracks sync state between runs.

**Responsibilities:**
- Store last synced timestamp/ID/snapshot
- Persist to durable storage (file, database, etc.)
- Retrieve watermark for next sync
- Support multiple tables

**Storage Options:**
- JSON file: `{warehouse}/watermarks/{table}.json`
- SQL Server table: `SyncWatermarks`
- Metadata in Iceberg table properties

**Interface:**
```csharp
public interface IWatermarkStore
{
    Task<Watermark> GetWatermarkAsync(string tableName);
    Task SetWatermarkAsync(string tableName, Watermark watermark);
}
```

### 7. **MergeStrategy** (NEW)
Determines how to apply changes to target.

**Strategies:**
- **Append-only**: Just INSERT new rows
- **Upsert**: UPDATE existing, INSERT new (requires PK)
- **Replace**: TRUNCATE and reload
- **SCD2**: Slowly Changing Dimension Type 2

**Interface:**
```csharp
public interface IMergeStrategy
{
    Task<MergeResult> ApplyAsync(
        IAsyncEnumerable<Dictionary<string, object>> sourceData,
        string connectionString,
        string tableName,
        MergeOptions options
    );
}
```

## Data Flow

### Detailed Step-by-Step Process

**Initial Setup (First Sync)**
```
1. Source SQL Server: 1000 rows
2. Export all → Iceberg (snapshot 1, 1000 rows)
3. Import all → Target SQL Server (1000 rows)
4. Store watermark: timestamp = 2024-10-10, snapshot = 1
```

**Incremental Sync (Subsequent Syncs)**
```
1. Source SQL Server: 1100 rows (100 new since 2024-10-10)
2. Detect changes: Query WHERE ModifiedDate > '2024-10-10'
   → Returns 100 rows
3. Append to Iceberg:
   - Write new Parquet file: data-0002.parquet (100 rows)
   - Create new manifest
   - Update metadata: snapshot 2 references both data files
   - Commit snapshot 2
4. Import from Iceberg:
   - Read snapshot 2 delta (100 new rows)
   - Or read full table (1100 rows) and MERGE
5. Merge into Target:
   - MERGE ON primary_key
   - UPDATE matched rows
   - INSERT new rows
   → Target now has 1100 rows
6. Update watermark: timestamp = 2024-10-11, snapshot = 2
```

## Models & Data Structures

### SyncOptions
```csharp
public class SyncOptions
{
    public string? WatermarkColumn { get; set; }  // e.g., "ModifiedDate"
    public string? PrimaryKeyColumn { get; set; }  // e.g., "CustomerID"
    public ChangeDetectionMode Mode { get; set; }  // Timestamp, ID, ChangeTracking
    public MergeMode MergeMode { get; set; }       // Upsert, AppendOnly, Replace
    public int BatchSize { get; set; } = 1000;
    public bool EnableLogging { get; set; } = true;
}
```

### Watermark
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

### SyncResult
```csharp
public class SyncResult
{
    public bool Success { get; set; }
    public int RowsExtracted { get; set; }
    public int RowsAppended { get; set; }
    public int RowsImported { get; set; }
    public long NewSnapshotId { get; set; }
    public Watermark NewWatermark { get; set; }
    public TimeSpan Duration { get; set; }
    public string? ErrorMessage { get; set; }
}
```

## Implementation Phases

### Phase 1: Iceberg Append Capability (Core)
**Files to Create:**
- `IcebergAppender.cs`
- `AppendResult.cs`

**Files to Modify:**
- `FilesystemCatalog.cs` - Add `AppendSnapshot()` method
- `TableMetadataGenerator.cs` - Support updating existing metadata

**Tests:**
- Append to existing table
- Multiple appends create multiple snapshots
- Metadata tracks all snapshots correctly

### Phase 2: Iceberg Reader
**Files to Create:**
- `IcebergReader.cs`
- `IcebergParquetReader.cs`
- `ReadOptions.cs`

**Dependencies:**
- ParquetSharp (already installed)
- Avro reader for manifests (already have Apache.Avro)

**Tests:**
- Read full table
- Read specific snapshot
- Read with filtering

### Phase 3: Change Detection
**Files to Create:**
- `IChangeDetectionStrategy.cs`
- `TimestampChangeDetection.cs`
- `IdBasedChangeDetection.cs`
- `IncrementalQuery.cs`

**Tests:**
- Generate correct WHERE clauses
- Handle NULL watermarks (first sync)
- Support various SQL Server data types

### Phase 4: SQL Server Importer
**Files to Create:**
- `SqlServerImporter.cs`
- `IMergeStrategy.cs`
- `UpsertMergeStrategy.cs`
- `AppendOnlyMergeStrategy.cs`
- `ImportResult.cs`

**Tests:**
- Bulk insert performance
- MERGE statement generation
- Handle duplicates and conflicts

### Phase 5: Watermark Management
**Files to Create:**
- `IWatermarkStore.cs`
- `FileWatermarkStore.cs`
- `SqlServerWatermarkStore.cs`
- `Watermark.cs`

**Tests:**
- Store and retrieve watermarks
- Handle concurrent access
- Persist across restarts

### Phase 6: Orchestration
**Files to Create:**
- `IncrementalSyncCoordinator.cs`
- `SyncOptions.cs`
- `SyncResult.cs`

**Tests:**
- End-to-end sync workflow
- Error handling and recovery
- Multiple tables in parallel

### Phase 7: Demo & Documentation
**Files to Create:**
- `demo/06-incremental-sync-demo.sh`
- `demo/07-simulate-source-changes.sql`
- `demo/08-verify-incremental-sync.sql`
- `docs/INCREMENTAL_SYNC_GUIDE.md`

## Technical Considerations

### 1. Parquet Reading
**Approach:** Use ParquetSharp to read existing data files
- Already have ParquetSharp 20.0.0 installed
- Use `ParquetFileReader` and `LogicalColumnReader<T>`
- Need to handle field-ID mapping from schema

### 2. Avro Reading
**Approach:** Use Apache.Avro to read manifests
- Already have Apache.Avro 1.11.3 installed
- Read manifest files to get data file list
- Read manifest-list to get manifest list

### 3. Snapshot Management
**Approach:** Append to existing metadata
- Read current metadata JSON
- Add new snapshot to snapshots array
- Update current-snapshot-id
- Increment last-sequence-number
- Write new v{N+1}.metadata.json

### 4. Change Detection Performance
**Approach:** Use indexed columns
- Ensure watermark columns are indexed
- Use clustered index on primary key
- Consider SQL Server Change Tracking for large tables

### 5. Merge Performance
**Approach:** Use SqlBulkCopy + MERGE
- Stage data in temp table using SqlBulkCopy
- Execute MERGE statement from temp table
- Drop temp table
- Much faster than row-by-row

### 6. Concurrency
**Approach:** Optimistic locking
- Use version-hint.txt for atomic commits (already implemented)
- Retry on conflict
- Consider distributed locking for multi-process

## Testing Strategy

### Unit Tests
- Each component in isolation
- Mock dependencies
- Cover edge cases (empty data, NULL values, conflicts)

### Integration Tests
- Full workflow with test databases
- Docker SQL Server containers
- Temporary Iceberg warehouses
- Cleanup after tests

### Performance Tests
- Large datasets (1M+ rows)
- Measure throughput (rows/second)
- Memory usage under load
- Concurrent syncs

### Scenario Tests
- Initial full sync
- Incremental sync with 10% change
- No changes (should be fast)
- Schema evolution (add column)
- Handle failures (network, disk, corruption)

## Open Questions & Decisions

### 1. How to handle schema evolution?
**Options:**
- Reject if schemas don't match (safest)
- Auto-add missing columns (flexible)
- Use Iceberg schema evolution (complex)

**Recommendation:** Start with strict matching, add flexibility later

### 2. How to handle deletions?
**Options:**
- Ignore (append-only, keep history)
- Soft delete (add IsDeleted flag)
- Hard delete (remove from target, complex in Iceberg)

**Recommendation:** Start without delete support, add later if needed

### 3. Where to store watermarks?
**Options:**
- File in Iceberg warehouse
- SQL Server metadata table
- Iceberg table properties
- Separate config database

**Recommendation:** Start with files, add SQL Server option later

### 4. How to identify primary keys?
**Options:**
- Explicit configuration (SyncOptions.PrimaryKeyColumn)
- Auto-detect from SQL Server metadata
- Require composite keys support

**Recommendation:** Start with single-column PK in config

### 5. Batch size for imports?
**Options:**
- Fixed (e.g., 10,000 rows)
- Dynamic based on row size
- Configurable per table

**Recommendation:** Default 10,000, make configurable

## Success Criteria

✅ **Must Have (MVP)**
1. Append new snapshots to existing Iceberg tables
2. Read data from Iceberg Parquet files
3. Detect changes using timestamp watermark
4. MERGE data into target SQL Server (upsert)
5. Store and retrieve watermarks
6. End-to-end sync completes successfully
7. Demo shows incremental sync with simulated changes

✅ **Should Have (V2)**
1. ID-based change detection
2. Multiple merge strategies (append-only, replace)
3. Concurrent table syncs
4. Retry logic and error recovery
5. Performance metrics and logging

✅ **Nice to Have (Future)**
1. SQL Server Change Tracking integration
2. Schema evolution support
3. Deletion handling
4. Partitioned table support
5. Distributed/parallel sync

## Estimated Complexity

- **Phase 1 (Append):** 2-3 days
- **Phase 2 (Reader):** 2-3 days
- **Phase 3 (Change Detection):** 1-2 days
- **Phase 4 (Importer):** 2-3 days
- **Phase 5 (Watermarks):** 1 day
- **Phase 6 (Orchestration):** 2 days
- **Phase 7 (Demo):** 1 day

**Total:** ~11-17 days for full implementation

**MVP (Phases 1-6):** ~10-16 days
**Demo:** +1 day
