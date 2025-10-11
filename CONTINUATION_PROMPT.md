# 🚀 Continuation Prompt: Complete Incremental Sync Implementation

**Use this prompt when starting a new Claude Code session to continue the incremental sync work.**

---

## Quick Context

I'm continuing implementation of **incremental synchronization** between SQL Server databases using Apache Iceberg as intermediate storage. The project is **60% complete (4 of 7 phases)** with core infrastructure already implemented and tested.

**Repository:** `/home/richard/sonnet45/`
**Branch:** `feature/incremental-sync` (should already be checked out)
**Documentation:** All implementation details in `docs/INCREMENTAL_SYNC_IMPLEMENTATION_PROMPT.md`

---

## Current Status

### ✅ COMPLETED (Phases 1, 2, 3, 5)

**Phase 1: IcebergAppender** - 11/11 tests passing ✅
- File: `src/DataTransfer.Iceberg/Integration/IcebergAppender.cs`
- Appends new data to existing Iceberg tables
- Creates new snapshots, preserves history

**Phase 2: IcebergReader** - 6/9 tests passing ✅
- Files: `src/DataTransfer.Iceberg/Readers/IcebergReader.cs`, `IcebergParquetReader.cs`
- Reads data from Iceberg via Avro manifests
- Reconstructs rows from Parquet columnar storage
- Time-travel support (read specific snapshots)

**Phase 3: Change Detection** - Complete ✅
- File: `src/DataTransfer.Iceberg/ChangeDetection/TimestampChangeDetection.cs`
- Watermark-based incremental queries
- Interface: `IChangeDetectionStrategy`

**Phase 5: Watermark Management** - Complete ✅
- File: `src/DataTransfer.Iceberg/Watermarks/FileWatermarkStore.cs`
- JSON-based watermark persistence
- Tracks sync state between runs

### ⏳ REMAINING WORK (Phases 4, 6, 7)

**Phase 4: SQL Server Importer** ⏳ (Priority 1)
- **Files to create:**
  - `src/DataTransfer.Iceberg/Integration/SqlServerImporter.cs`
  - `src/DataTransfer.Iceberg/MergeStrategies/IMergeStrategy.cs`
  - `src/DataTransfer.Iceberg/MergeStrategies/UpsertMergeStrategy.cs`
  - `src/DataTransfer.Iceberg/Models/ImportResult.cs`
  - `src/DataTransfer.Iceberg/Models/MergeResult.cs`
- **Reference:** `docs/INCREMENTAL_SYNC_IMPLEMENTATION_PROMPT.md` lines 279-372

**Phase 6: Orchestration** ⏳ (Priority 2)
- **Files to create:**
  - `src/DataTransfer.Iceberg/Integration/IncrementalSyncCoordinator.cs`
  - `src/DataTransfer.Iceberg/Models/SyncOptions.cs`
  - `src/DataTransfer.Iceberg/Models/SyncResult.cs`
- **Reference:** `docs/INCREMENTAL_SYNC_IMPLEMENTATION_PROMPT.md` lines 433-555

**Phase 7: Demo & Tests** ⏳ (Priority 3)
- **Files to create:**
  - `demo/06-incremental-sync-demo.sh`
  - `tests/DataTransfer.Iceberg.Tests/Integration/EndToEndSyncTests.cs`
- **Reference:** `docs/INCREMENTAL_SYNC_IMPLEMENTATION_PROMPT.md` lines 558-613

---

## Getting Started Commands

```bash
# 1. Verify you're on the correct branch
git status
git log --oneline -10

# 2. Review what's already implemented
cat src/DataTransfer.Iceberg/Integration/IcebergAppender.cs
cat src/DataTransfer.Iceberg/Readers/IcebergReader.cs

# 3. Run existing tests to verify everything works
dotnet test tests/DataTransfer.Iceberg.Tests

# Expected results:
# - IcebergAppenderTests: 11/11 passing
# - IcebergReaderTests: 6/9 passing (known limitations documented)
```

---

## Implementation Approach

### Step 1: Implement Phase 4 (SQL Server Importer)

**Goal:** Import data from Iceberg to SQL Server with MERGE/upsert logic

**TDD Workflow:**
1. **RED:** Create test file `tests/DataTransfer.Iceberg.Tests/Integration/SqlServerImporterTests.cs`
2. **GREEN:** Implement `SqlServerImporter` class
3. **REFACTOR:** Clean up and optimize
4. **COMMIT:** `git commit -m "feat(iceberg): implement SqlServerImporter [GREEN]"`

**Key Requirements:**
- Stream data from `IAsyncEnumerable<Dictionary<string, object>>`
- Use `SqlBulkCopy` for efficient loading
- Create temp tables for staging
- Execute SQL MERGE statements via `UpsertMergeStrategy`
- Return structured `ImportResult` (rows inserted/updated)

**Complete implementation code is in:**
`docs/INCREMENTAL_SYNC_IMPLEMENTATION_PROMPT.md` lines 279-372

### Step 2: Implement Phase 6 (Orchestration)

**Goal:** Wire up all components into end-to-end workflow

**TDD Workflow:**
1. **RED:** Create test file for `IncrementalSyncCoordinator`
2. **GREEN:** Implement coordinator class
3. **REFACTOR:** Add error handling, logging
4. **COMMIT:** `git commit -m "feat(iceberg): implement IncrementalSyncCoordinator [GREEN]"`

**Workflow to implement:**
```
1. Get watermark (FileWatermarkStore)
2. Build query (TimestampChangeDetection)
3. Extract changes (SQL Server)
4. Append to Iceberg (IcebergAppender)
5. Read from Iceberg (IcebergReader)
6. Import to target (SqlServerImporter)
7. Update watermark
```

**Complete implementation code is in:**
`docs/INCREMENTAL_SYNC_IMPLEMENTATION_PROMPT.md` lines 433-555

### Step 3: Create Demo & Integration Tests

**Goal:** Demonstrate working end-to-end incremental sync

**Tasks:**
1. Create integration test showing: 1000 rows → add 100 → sync → verify 1100
2. Write bash demo script (can use Docker SQL Server)
3. Add verification queries

**Demo script reference:**
`docs/INCREMENTAL_SYNC_IMPLEMENTATION_PROMPT.md` lines 558-613

---

## Key Design Patterns (Already Established)

Follow these patterns from existing code:

1. ✅ **Async/await** with `CancellationToken` throughout
2. ✅ **`IAsyncEnumerable<T>`** for streaming (memory efficient)
3. ✅ **Structured results** (don't throw for business logic failures)
4. ✅ **`ILogger<T>`** for all logging
5. ✅ **TDD workflow** with RED-GREEN-REFACTOR commits

---

## Documentation Files

All implementation details are documented:

1. **`docs/INCREMENTAL_SYNC_IMPLEMENTATION_PROMPT.md`**
   - Complete 7-phase implementation guide
   - Full code examples for every component
   - Lines 279-372: Phase 4 (SQL Server Importer)
   - Lines 433-555: Phase 6 (Orchestration)
   - Lines 558-613: Phase 7 (Demo scripts)

2. **`docs/ICEBERG_READER_IMPLEMENTATION_GUIDE.md`**
   - Detailed Avro manifest reading patterns
   - Parquet columnar-to-row reconstruction

3. **`demo/INCREMENTAL_SYNC_README.md`**
   - Current status summary
   - Working code examples

4. **`CLAUDE.md`** (Project root)
   - TDD requirements
   - Testing commands

---

## Task Summary for Claude

Please implement the remaining phases of the incremental sync system:

1. **Phase 4:** Create `SqlServerImporter` with MERGE/upsert logic (TDD approach)
2. **Phase 6:** Create `IncrementalSyncCoordinator` to orchestrate end-to-end workflow
3. **Phase 7:** Create demo script and integration tests

Follow the TDD approach established in Phases 1-3. All implementation details are in the documentation files listed above.

Current state: 60% complete, excellent foundation, clear path to finish.

---

**Start by reviewing existing code, then implement Phase 4 (highest priority).**

Good luck! 🚀
