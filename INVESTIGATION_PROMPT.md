# Investigation Prompt: Fix Failing EndToEndSync Test

**Date:** 2025-10-11
**Branch:** `feature/incremental-sync`
**Status:** 107/108 tests passing (99.1%)
**Failing Test:** `Should_Sync_Complete_Workflow_Across_Multiple_Cycles`

---

## Background

This is a .NET 8 project implementing incremental synchronization between SQL Server databases using Apache Iceberg as an intermediate storage layer. The system uses watermark-based change detection to sync only modified data.

**Recently Completed:**
- Implemented manifest accumulation for proper Iceberg snapshot handling
- Fixed nullable string preservation in Parquet writer
- Added empty table guards
- Fixed test configuration issues

**Current State:** All core functionality works (phases 1-7 complete), but one end-to-end test fails during the update cycle.

---

## The Problem

**Test Location:** `tests/DataTransfer.Iceberg.Tests/Integration/EndToEndSyncTests.cs:54-134`
**Failure Point:** Line 145 - `Assert.Equal(50, updatedCount);`

The test executes 4 sync cycles:
1. ✅ **Cycle 1:** Initial sync (500 rows) - PASSES
2. ✅ **Cycle 2:** Add 200 new rows (700 total) - PASSES
3. ❌ **Cycle 3:** Update 50 rows - FAILS
4. (Not reached) **Cycle 4:** No changes

### Cycle 3 Failure Symptoms

From test output with enhanced logging:

```
[CYCLE 3] === UPDATE CYCLE STARTING ===
[CYCLE 3] Watermark before update: 2025-10-11T19:16:48.5822094Z
[CYCLE 3] Source has 50 rows with 'Updated' suffix
[CYCLE 3] Updated rows have ModifiedDate range: 2025-10-11T19:16:49.6966667 to 2025-10-11T19:16:50.1533333
[CYCLE 3] Watermark LastSyncTimestamp: 2025-10-11T19:16:48.5822094Z
[CYCLE 3] Updated rows are AFTER watermark: True
[CYCLE 3] Incremental query SQL: SELECT * FROM Products WHERE ModifiedDate > @WatermarkValue
[CYCLE 3] Parameter @WatermarkValue = 10/11/2025 19:16:48
[CYCLE 3] Sync result - Success: True, RowsExtracted: 50, RowsImported: 0  ← PROBLEM
[CYCLE 3] Iceberg has 750 total rows, 50 with 'Updated' suffix
[CYCLE 3] Target has 0 rows with 'Updated' suffix (expected 50)  ← PROBLEM
```

**Key Observation:** `RowsImported: 0` despite:
- ✅ 50 rows correctly extracted from source
- ✅ 750 rows exist in Iceberg (500 + 200 + 50)
- ✅ 50 rows in Iceberg have 'Updated' suffix
- ❌ 0 rows imported to target
- ❌ 0 rows in target have 'Updated' suffix

---

## What We've Proven

### ✅ SqlServerImporter Works in Isolation
**Test:** `SqlServerImporterDiagnosticTests.Diagnose_Import_From_Iceberg_Table`
- Creates Iceberg table with 3 rows
- Reads from Iceberg
- Imports to SQL Server
- **Result:** PASSES - All 3 rows imported successfully

**Conclusion:** The importer itself is not broken.

### ✅ Iceberg Reader Works After Sync
The test successfully reads 750 rows from Iceberg AFTER Cycle 3 completes:
```csharp
var icebergData = await ReadIcebergTable("products_sync");
// Returns 750 rows correctly
```

**Conclusion:** The Iceberg data is present and readable.

### ✅ Data Flow Works for Cycles 1 & 2
- Cycle 1 (initial 500 rows): RowsImported = 500
- Cycle 2 (additional 200 rows): RowsImported = 200

**Conclusion:** The issue is specific to update cycles (Cycle 3), not insert cycles.

---

## Root Cause Hypothesis

The `IAsyncEnumerable<Dictionary<string, object>>` returned by `IcebergReader.ReadTableAsync()` appears to be **empty when enumerated by SqlServerImporter** during Cycle 3, despite:
1. The data existing in Iceberg
2. The reader being able to read the data when called directly in the test

**Likely causes:**

### Hypothesis 1: IAsyncEnumerable Consumed Before Import
**File:** `src/DataTransfer.Iceberg/Integration/IncrementalSyncCoordinator.cs:110-114`

```csharp
// 4. Read from Iceberg
var data = _reader.ReadTableAsync(icebergTable, cancellationToken);

// 5. Import to target
var mergeStrategy = CreateMergeStrategy(options);
var importResult = await _importer.ImportAsync(data, targetConnection, targetTable, mergeStrategy, cancellationToken);
```

**Issue:** Is the `IAsyncEnumerable` being consumed somewhere between creation and import?

### Hypothesis 2: Manifest Accumulation Bug
**Recent Change:** `src/DataTransfer.Iceberg/Integration/IcebergAppender.cs:202-240`

We recently implemented manifest accumulation to fix multi-append reading. The logic now:
1. Reads previous manifest list
2. Carries forward old manifests
3. Adds new manifest
4. Writes accumulated manifest list

**Issue:** Could the reader be failing to enumerate data files from accumulated manifest lists?

**Critical Code:**
```csharp
// IcebergAppender.cs:217-229
if (File.Exists(previousManifestListPath))
{
    var previousManifests = generator.ReadManifestList(previousManifestListPath);

    // Carry forward previous manifests with existing_files_count instead of added_files_count
    foreach (var (path, size, _) in previousManifests)
    {
        allManifests.Add((path, size, 0)); // 0 added, all are existing
    }
}
```

### Hypothesis 3: Catalog/Reader Not Seeing Latest Snapshot
**File:** `src/DataTransfer.Iceberg/Readers/IcebergReader.cs:40-51`

```csharp
// 1. Load table metadata
var metadata = _catalog.LoadTable(tableName);
if (metadata == null)
{
    throw new InvalidOperationException($"Table {tableName} does not exist");
}

var currentSnapshot = metadata.Snapshots.First(s => s.SnapshotId == metadata.CurrentSnapshotId);
```

**Issue:** Is the catalog caching metadata? Does it need to be refreshed to see the newly appended snapshot?

### Hypothesis 4: Third Append Creating Invalid Manifest Structure
Cycles 1 and 2 work (initial write + first append), but Cycle 3 fails (second append).

**Issue:** Is there a bug in how we handle the **second append** specifically?

---

## Investigation Steps

### Step 1: Add Logging to IcebergReader
**File:** `src/DataTransfer.Iceberg/Readers/IcebergReader.cs`

Add logging to `ReadTableAsync()` method:
- Log when enumeration starts
- Log snapshot ID being read
- Log number of manifest files found
- Log number of data files found
- Log each row yielded

### Step 2: Add Logging to IncrementalSyncCoordinator
**File:** `src/DataTransfer.Iceberg/Integration/IncrementalSyncCoordinator.cs`

Add logging around lines 110-114:
- Log immediately before calling `ReadTableAsync()`
- Log the snapshot ID that was just created by appender
- Consider materializing a sample of the enumerable to verify it's not empty

### Step 3: Inspect Manifest List Structure
Add diagnostic logging to see what's in the manifest list after Cycle 3 append:

```csharp
// After line 79 in IcebergAppender.cs
_logger.LogDebug("Manifest list contains {Count} manifests", allManifests.Count);
foreach (var (path, size, addedCount) in allManifests)
{
    _logger.LogDebug("  Manifest: {Path}, Size: {Size}, Added: {AddedCount}", path, size, addedCount);
}
```

### Step 4: Verify Catalog State
Check if `FilesystemCatalog.LoadTable()` needs to refresh metadata:

```csharp
// Before reading in coordinator, reload metadata
var metadata = _catalog.LoadTable(icebergTable);
_logger.LogInformation("Current snapshot: {SnapshotId}, Total snapshots: {Count}",
    metadata.CurrentSnapshotId, metadata.Snapshots.Count);
```

### Step 5: Test Manifest Accumulation Directly
Create a focused unit test:

```csharp
[Fact]
public async Task Should_Read_After_Third_Append()
{
    // Arrange
    var schema = CreateSimpleSchema();

    // Act - Write initial + 2 appends (same as failing test)
    await _writer.WriteTableAsync("test_table", schema, CreateSampleData(500));
    await _appender.AppendAsync("test_table", CreateSampleData(200, startId: 501));
    await _appender.AppendAsync("test_table", CreateSampleData(50, startId: 701));

    // Act - Read all data
    var rows = new List<Dictionary<string, object>>();
    await foreach (var row in _reader.ReadTableAsync("test_table"))
    {
        rows.Add(row);
    }

    // Assert
    Assert.Equal(750, rows.Count);
}
```

### Step 6: Compare Working vs Broken Sync
Compare the importer behavior between Cycle 2 (works) and Cycle 3 (broken):
- Are they using different code paths?
- Is there a difference in how the merge strategy behaves?
- Check if UpsertMergeStrategy has issues with already-existing rows

---

## Key Files to Examine

1. **`src/DataTransfer.Iceberg/Integration/IncrementalSyncCoordinator.cs`** (lines 108-116)
   - How data flows from reader to importer

2. **`src/DataTransfer.Iceberg/Readers/IcebergReader.cs`** (lines 40-86)
   - How manifest lists are read
   - How data files are enumerated

3. **`src/DataTransfer.Iceberg/Integration/IcebergAppender.cs`** (lines 202-240)
   - Manifest accumulation logic (recently changed)

4. **`src/DataTransfer.Iceberg/Metadata/ManifestListGenerator.cs`** (lines 161-192)
   - `ReadManifestList()` method

5. **`src/DataTransfer.Iceberg/Integration/SqlServerImporter.cs`** (lines 130-180)
   - `MaterializeDataTable()` - already has logging added

6. **`src/DataTransfer.Iceberg/Catalog/FilesystemCatalog.cs`** (lines 100-150)
   - `LoadTable()` method - check for caching issues

---

## Success Criteria

1. ✅ Test `Should_Sync_Complete_Workflow_Across_Multiple_Cycles` passes
2. ✅ `RowsImported` in Cycle 3 equals 50 (not 0)
3. ✅ Target database contains 50 rows with 'Updated' suffix after Cycle 3
4. ✅ All other tests remain passing (107+ tests)
5. ✅ No regressions in Cycles 1, 2, or 4

---

## Debugging Commands

```bash
# Run the specific failing test with enhanced output
dotnet test tests/DataTransfer.Iceberg.Tests/DataTransfer.Iceberg.Tests.csproj \
  --filter "FullyQualifiedName~Should_Sync_Complete_Workflow_Across_Multiple_Cycles" \
  --logger "console;verbosity=detailed" > test_output.log 2>&1

# Extract Cycle 3 logging
grep "\[CYCLE 3\]" test_output.log

# Run diagnostic test (proves importer works)
dotnet test tests/DataTransfer.Iceberg.Tests/DataTransfer.Iceberg.Tests.csproj \
  --filter "FullyQualifiedName~Diagnose_Import_From_Iceberg_Table"

# Run all IcebergReader tests (verify no regressions from manifest accumulation)
dotnet test tests/DataTransfer.Iceberg.Tests/DataTransfer.Iceberg.Tests.csproj \
  --filter "FullyQualifiedName~IcebergReaderTests"

# Run all EndToEndSync tests
dotnet test tests/DataTransfer.Iceberg.Tests/DataTransfer.Iceberg.Tests.csproj \
  --filter "FullyQualifiedName~EndToEndSyncTests"
```

---

## Additional Context

### Manifest Accumulation Logic (Recently Added)

The manifest accumulation fix ensures that each new snapshot includes ALL data files from previous snapshots plus new files. Before the fix, only delta files were included, causing multi-append reads to return incomplete data.

**Old behavior (broken):**
- Snapshot 1: [file1.parquet] → reader gets 1 file
- Snapshot 2: [file2.parquet] → reader gets 1 file (missing file1)
- Snapshot 3: [file3.parquet] → reader gets 1 file (missing file1, file2)

**New behavior (fixed):**
- Snapshot 1: [file1.parquet] → reader gets 1 file
- Snapshot 2: [file1.parquet, file2.parquet] → reader gets 2 files ✓
- Snapshot 3: [file1.parquet, file2.parquet, file3.parquet] → reader gets 3 files ✓

The fix was validated and works for simple reads (IcebergReaderTests all pass). The issue only manifests in the end-to-end coordinator workflow during Cycle 3.

### IAsyncEnumerable Behavior

`IAsyncEnumerable<T>` can only be enumerated once. If something consumes it before the importer, it will appear empty to the importer. Check for:
- Debug logging that iterates the enumerable
- Multiple enumerations
- Disposal before enumeration completes

---

## Expected Fix Location

Based on the evidence, the most likely fix location is:

**Option A:** `IcebergReader.ReadTableAsync()` - Bug in how accumulated manifests are read
**Option B:** `IncrementalSyncCoordinator.SyncAsync()` - Data enumerable consumed prematurely
**Option C:** `ManifestListGenerator.ReadManifestList()` - Incorrectly reading manifest entries

**Recommended approach:** Start with Step 1-2 (add logging), then Step 3 (inspect manifests), then Step 5 (focused unit test).

---

## Related Commits

- `e4d3c4f` - fix(iceberg): resolve all test failures - 107/108 passing (99.1%)
- `07cc839` - debug(test): add comprehensive logging and diagnostic test for import issue

---

## Question to Answer

**Why does `_reader.ReadTableAsync()` return an empty enumerable during Cycle 3 when called from the coordinator, but returns data correctly when called directly in the test or diagnostic?**

Answer this question and you'll have found the bug.

---

Good luck! 🔍
