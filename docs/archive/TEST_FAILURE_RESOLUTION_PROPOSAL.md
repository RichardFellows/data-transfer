# Test Failure Resolution Proposal

**Date:** 2025-10-11
**Status:** 5 failing tests (103/108 passing - 95.4%)
**Impact:** Low - Core workflows verified, issues are edge cases and test configuration

---

## Executive Summary

All 5 failing tests have been analyzed with root causes identified. Solutions range from simple fixes (test configuration) to moderate complexity (Iceberg spec compliance).

**Recommendation:** Fix in priority order:
1. **Phase 7 test config issues (2 tests)** - Simple, 30 minutes
2. **Nullable string handling (1 test)** - Simple, 15 minutes
3. **Empty table support (1 test)** - Moderate, 1 hour
4. **Multi-append manifest accumulation (1 test)** - Complex, 2-3 hours

---

## Phase 2 Failures (IcebergReaderTests)

### ❌ Test 1: Should_Handle_Multiple_Data_Files

**Location:** `tests/DataTransfer.Iceberg.Tests/Readers/IcebergReaderTests.cs:73-94`

**Failure:**
```
Assert.Equal() Failure
Expected: 15
Actual:   5
```

**Root Cause:**
`IcebergAppender.GenerateManifestList()` creates a new manifest list with only the NEW manifest, not accumulating previous manifests. According to Iceberg spec, each snapshot should reference ALL data files (cumulative), not deltas.

**Code Location:** `src/DataTransfer.Iceberg/Integration/IcebergAppender.cs:202-222`

```csharp
// CURRENT (INCORRECT) - Only writes new manifest
private string GenerateManifestList(
    string tablePath,
    string manifestRelativePath,
    int addedFilesCount)
{
    var generator = new ManifestListGenerator();
    var manifestListFileName = $"snap-{Guid.NewGuid()}.avro";
    var manifestListPath = Path.Combine(tablePath, "metadata", manifestListFileName);

    generator.WriteManifestList(
        manifestPath: manifestRelativePath,  // ← Only NEW manifest
        outputPath: manifestListPath,
        manifestSizeInBytes: manifestSize,
        addedFilesCount: addedFilesCount);

    return $"metadata/{manifestListFileName}";
}
```

**Solution:**
Read the previous snapshot's manifest list and carry forward all previous manifests + add the new one.

```csharp
// CORRECTED - Accumulate all manifests
private string GenerateManifestList(
    string tablePath,
    TableMetadata existingMetadata,
    string newManifestPath,
    int addedFilesCount)
{
    var generator = new ManifestListGenerator();
    var manifestListFileName = $"snap-{Guid.NewGuid()}.avro";
    var manifestListPath = Path.Combine(tablePath, "metadata", manifestListFileName);

    // 1. Get previous manifests from current snapshot
    var currentSnapshot = existingMetadata.Snapshots
        .First(s => s.SnapshotId == existingMetadata.CurrentSnapshotId);
    var previousManifestListPath = Path.Combine(tablePath, currentSnapshot.ManifestList);

    // 2. Read existing manifests
    var previousManifests = ReadManifestListEntries(previousManifestListPath);

    // 3. Add new manifest to list
    var allManifests = previousManifests.ToList();
    allManifests.Add(new ManifestListEntry
    {
        ManifestPath = newManifestPath,
        ManifestLength = new FileInfo(Path.Combine(tablePath, newManifestPath)).Length,
        AddedFilesCount = addedFilesCount
    });

    // 4. Write accumulated manifest list
    generator.WriteManifestList(allManifests, manifestListPath);

    return $"metadata/{manifestListFileName}";
}
```

**Changes Required:**
1. Update `IcebergAppender.GenerateManifestList()` signature to accept `TableMetadata`
2. Create `ReadManifestListEntries()` helper method to parse Avro manifest list
3. Update `ManifestListGenerator.WriteManifestList()` to accept list of entries
4. Update call site at `IcebergAppender.cs:79`

**Complexity:** Moderate-High (2-3 hours)
**Risk:** Medium - Affects core append logic
**Testing:** Verify all IcebergAppender tests still pass + fix this test

---

### ❌ Test 2: Should_Return_Empty_For_Empty_Table

**Location:** `tests/DataTransfer.Iceberg.Tests/Readers/IcebergReaderTests.cs:193-209`

**Failure:**
```
System.InvalidOperationException : Table empty_table does not exist
```

**Root Cause:**
`IcebergTableWriter.WriteTableAsync()` returns early when data is empty (line 59-70), never committing metadata. The reader can't find the table because no `v1.metadata.json` was created.

**Code Location:** `src/DataTransfer.Iceberg/Integration/IcebergTableWriter.cs:58-70`

```csharp
// CURRENT (INCORRECT) - Early return, no metadata commit
if (data.Count == 0)
{
    _logger.LogWarning("No data to write for table {Table}, creating empty table", tableName);
    return new IcebergWriteResult
    {
        Success = true,
        SnapshotId = 0,
        TablePath = tablePath,
        DataFileCount = 0,
        RecordCount = 0
    };
}
```

**Solution:**
Create metadata with empty snapshot (no data files, but valid table structure).

```csharp
// CORRECTED - Create metadata for empty table
if (data.Count == 0)
{
    _logger.LogWarning("No data to write for table {Table}, creating empty table", tableName);

    // Generate snapshot ID even for empty table
    var snapshotId = GenerateSnapshotId();

    // Create empty manifest list (no manifests)
    var emptyManifestListPath = GenerateEmptyManifestList(tablePath);

    // Generate metadata with empty snapshot
    var metadata = GenerateTableMetadata(schema, tablePath, emptyManifestListPath, snapshotId);

    // Commit metadata
    var success = await _catalog.CommitAsync(tableName, metadata, cancellationToken);

    if (!success)
    {
        return new IcebergWriteResult
        {
            Success = false,
            ErrorMessage = $"Failed to commit empty Iceberg table {tableName}"
        };
    }

    return new IcebergWriteResult
    {
        Success = true,
        SnapshotId = snapshotId,
        TablePath = tablePath,
        DataFileCount = 0,
        RecordCount = 0
    };
}
```

**Changes Required:**
1. Create `GenerateEmptyManifestList()` helper method
2. Update empty data case in `WriteTableAsync()` to commit metadata
3. Update `IcebergReader` to handle empty manifest lists gracefully

**Complexity:** Moderate (1 hour)
**Risk:** Low - Isolated to edge case
**Testing:** Verify all IcebergTableWriter tests still pass + fix this test

---

### ❌ Test 3: Should_Handle_Nullable_Fields

**Location:** `tests/DataTransfer.Iceberg.Tests/Readers/IcebergReaderTests.cs:135-158`

**Failure:**
```
Assert.Null() Failure
Expected: (null)
Actual:   ""  (empty string)
```

**Root Cause:**
`IcebergParquetWriter.WriteColumnBatch()` converts null values to empty strings for the string type (line 368).

**Code Location:** `src/DataTransfer.Iceberg/Writers/IcebergParquetWriter.cs:366-369`

```csharp
// CURRENT (INCORRECT) - Converts null to empty string
case "string":
    var stringWriter = columnWriter.LogicalWriter<string>();
    stringWriter.WriteBatch(values.Select(v => v?.ToString() ?? string.Empty).ToArray());
    break;
```

**Solution:**
Preserve null values when writing strings (strings are nullable reference types in Parquet).

```csharp
// CORRECTED - Preserve null values
case "string":
    var stringWriter = columnWriter.LogicalWriter<string>();
    stringWriter.WriteBatch(values.Select(v => v?.ToString()).ToArray());
    break;
```

**Alternative (More Robust):**
Check field.Required to determine null handling:

```csharp
case "string":
    var stringWriter = columnWriter.LogicalWriter<string>();
    if (field.Required)
    {
        // Required strings should never be null - convert to empty string
        stringWriter.WriteBatch(values.Select(v => v?.ToString() ?? string.Empty).ToArray());
    }
    else
    {
        // Optional strings should preserve null values
        stringWriter.WriteBatch(values.Select(v => v?.ToString()).ToArray());
    }
    break;
```

**Changes Required:**
1. Update string case in `WriteColumnBatch()` method
2. Consider adding null validation for Required fields

**Complexity:** Simple (15 minutes)
**Risk:** Very Low - String handling only
**Testing:** Verify all Parquet writer tests still pass + fix this test

---

## Phase 7 Failures (EndToEndSyncTests)

### ❌ Test 4: Should_Handle_Large_Dataset_Sync

**Location:** `tests/DataTransfer.Iceberg.Tests/Integration/EndToEndSyncTests.cs:136-171`

**Failure:**
```
Invalid column name 'ProductId' in LargeOrders table
```

**Root Cause:**
Test helper method `CreateSourceTable()` creates a generic schema with columns `ProductId, Name, Price, ModifiedDate`, but this is used for a table named `LargeOrders`. The table schema should have `OrderId` as primary key, not `ProductId`.

**Code Location:** `tests/DataTransfer.Iceberg.Tests/Integration/EndToEndSyncTests.cs:306-321`

```csharp
// CURRENT - Generic schema reused for all tables
private async Task CreateSourceTable(string tableName)
{
    await using var connection = new SqlConnection(GetConnectionString(_sourceDatabase));
    await connection.OpenAsync();

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = $@"
        IF OBJECT_ID('{tableName}', 'U') IS NOT NULL DROP TABLE {tableName};
        CREATE TABLE {tableName} (
            ProductId INT PRIMARY KEY,  // ← Wrong for LargeOrders
            Name NVARCHAR(200),
            Price DECIMAL(18,2),
            ModifiedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
        )";
    await cmd.ExecuteNonQueryAsync();
}
```

**Solution:**
Create table-specific schemas or parameterize the primary key column name.

**Option 1: Parameterize CreateSourceTable**
```csharp
private async Task CreateSourceTable(string tableName, string primaryKeyColumn = "ProductId")
{
    await using var connection = new SqlConnection(GetConnectionString(_sourceDatabase));
    await connection.OpenAsync();

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = $@"
        IF OBJECT_ID('{tableName}', 'U') IS NOT NULL DROP TABLE {tableName};
        CREATE TABLE {tableName} (
            {primaryKeyColumn} INT PRIMARY KEY,
            Name NVARCHAR(200),
            Price DECIMAL(18,2),
            ModifiedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
        )";
    await cmd.ExecuteNonQueryAsync();
}

// Then call it with:
await CreateSourceTable("LargeOrders", "OrderId");
```

**Option 2: Create Specialized Helper**
```csharp
private async Task CreateOrdersTable(string tableName)
{
    await using var connection = new SqlConnection(GetConnectionString(_sourceDatabase));
    await connection.OpenAsync();

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = $@"
        IF OBJECT_ID('{tableName}', 'U') IS NOT NULL DROP TABLE {tableName};
        CREATE TABLE {tableName} (
            OrderId INT PRIMARY KEY,
            CustomerId INT,
            OrderDate DATETIME2,
            Amount DECIMAL(18,2),
            Status NVARCHAR(50),
            ModifiedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
        )";
    await cmd.ExecuteNonQueryAsync();
}
```

**Changes Required:**
1. Update `CreateSourceTable()` to accept primary key parameter OR create specialized helper
2. Update `CreateTargetTable()` similarly
3. Update `InsertLargeDataset()` to use `OrderId` column
4. Update test call at line 140: `await CreateSourceTable("LargeOrders", "OrderId");`

**Complexity:** Simple (15 minutes)
**Risk:** None - Test-only change
**Testing:** Run EndToEndSyncTests

---

### ❌ Test 5: Should_Preserve_Data_Accuracy_Across_Sync

**Location:** `tests/DataTransfer.Iceberg.Tests/Integration/EndToEndSyncTests.cs:173-215`

**Failure:**
```
Invalid column name 'TransactionId' in Transactions table
```

**Root Cause:**
Similar to Test 4 - `InsertTransactions()` expects a transaction-specific schema but gets the generic `ProductId` schema.

**Code Location:** `tests/DataTransfer.Iceberg.Tests/Integration/EndToEndSyncTests.cs:399-422`

```csharp
// CURRENT - Uses ProductId column incorrectly
private async Task InsertTransactions()
{
    // ...
    cmd.CommandText = @"
        INSERT INTO Transactions (ProductId, Name, Price, ModifiedDate)
        VALUES (@Id, @Desc, @Amount, GETUTCDATE())";
    // ...
}
```

**Solution:**
Create proper transaction table schema or update InsertTransactions to use ProductId.

**Option 1: Keep Generic Schema (Simplest)**
```csharp
// No changes needed to CreateSourceTable
// Just update InsertTransactions column references

private async Task InsertTransactions()
{
    await using var connection = new SqlConnection(GetConnectionString(_sourceDatabase));
    await connection.OpenAsync();

    var transactions = new[]
    {
        (1, 100.50m, "Payment from customer A"),
        (2, 250.00m, "Refund to customer B"),
        (3, 75.25m, "Payment from customer C")
    };

    foreach (var (id, amount, desc) in transactions)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
            INSERT INTO Transactions (ProductId, Name, Price, ModifiedDate)
            VALUES (@Id, @Desc, @Amount, GETUTCDATE())";
        cmd.Parameters.AddWithValue("@Id", id);
        cmd.Parameters.AddWithValue("@Desc", desc);
        cmd.Parameters.AddWithValue("@Amount", amount);
        await cmd.ExecuteNonQueryAsync();
    }
}

// Update GetTransactionData and GetTargetTransactionData similarly
```

**Option 2: Create Proper Transaction Schema**
```csharp
private async Task CreateTransactionTable(string tableName)
{
    await using var connection = new SqlConnection(GetConnectionString(_sourceDatabase));
    await connection.OpenAsync();

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = $@"
        IF OBJECT_ID('{tableName}', 'U') IS NOT NULL DROP TABLE {tableName};
        CREATE TABLE {tableName} (
            TransactionId INT PRIMARY KEY,
            Amount DECIMAL(18,2) NOT NULL,
            Description NVARCHAR(500),
            ModifiedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE()
        )";
    await cmd.ExecuteNonQueryAsync();
}
```

**Recommendation:** Use Option 1 (simplest) - just fix the column references in test helper methods.

**Changes Required:**
1. Keep `CreateSourceTable("Transactions")` as-is (uses ProductId schema)
2. Update `InsertTransactions()` to use `ProductId` column (already correct)
3. Update `GetTransactionData()` to return `(ProductId, Price, Name)` tuples
4. Update test assertions to match actual schema

**Complexity:** Simple (15 minutes)
**Risk:** None - Test-only change
**Testing:** Run EndToEndSyncTests

---

## Implementation Priority

### Priority 1: Phase 7 Test Configuration (30 minutes total)
**Impact:** High visibility, easy wins
**Risk:** None - test-only changes

1. ✅ Fix Test 4: Update LargeOrders test schema
2. ✅ Fix Test 5: Update Transactions test column references

### Priority 2: Nullable String Handling (15 minutes)
**Impact:** Data fidelity in production
**Risk:** Very Low - isolated change

3. ✅ Fix Test 3: Preserve null values in string columns

### Priority 3: Empty Table Support (1 hour)
**Impact:** Medium - edge case handling
**Risk:** Low - isolated to empty table scenario

4. ✅ Fix Test 2: Create metadata for empty tables

### Priority 4: Multi-Append Manifest Accumulation (2-3 hours)
**Impact:** High - Iceberg spec compliance
**Risk:** Medium - affects core append logic

5. ✅ Fix Test 1: Accumulate manifests across snapshots

---

## Testing Strategy

After each fix:
1. Run specific test to verify fix
2. Run all tests in affected component (IcebergReaderTests, EndToEndSyncTests)
3. Run full test suite to ensure no regressions
4. Update documentation with new test counts

### Verification Commands

```bash
# Test 1 (multi-append)
dotnet test --filter "FullyQualifiedName~Should_Handle_Multiple_Data_Files"

# Test 2 (empty table)
dotnet test --filter "FullyQualifiedName~Should_Return_Empty_For_Empty_Table"

# Test 3 (nullable strings)
dotnet test --filter "FullyQualifiedName~Should_Handle_Nullable_Fields"

# Tests 4 & 5 (end-to-end)
dotnet test --filter "FullyQualifiedName~EndToEndSyncTests"

# Full test suite
dotnet test tests/DataTransfer.Iceberg.Tests
```

---

## Expected Outcome

After all fixes implemented:
- **Total Tests:** 108
- **Passing:** 108 (100%)
- **Failing:** 0

**Estimated Total Time:** 4-5 hours
**Recommended Approach:** Fix in priority order with commits after each fix (TDD methodology)

---

## Decision Required

**Question for User:** Which priority level should we implement?

- **Option A:** All 5 tests (4-5 hours) - 100% pass rate
- **Option B:** Priorities 1-3 (1.75 hours) - 4/5 fixed, 99.1% pass rate
- **Option C:** Priority 1 only (30 minutes) - 2/5 fixed, 97.2% pass rate

**Recommendation:** **Option B** - Fixes all simple issues, defers complex manifest accumulation for future iteration when needed for production time-travel queries.
