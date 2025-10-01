# Quick Start Guide for New LLM Context

## Current State
- **107 tests passing** across 5 layers
- Core, Configuration, SqlServer, Parquet, Pipeline layers: ✅ COMPLETE
- Console application: ✅ COMPLETE
- Integration tests: 🔨 TODO (immediate priority)

## Immediate Task: Integration Tests

### What to do RIGHT NOW:

1. **Read these files first (in order):**
   - `IMPLEMENTATION_STATUS.md` - Overall project status and integration test spec
   - `ARCHITECTURE.md` - Technical architecture

2. **Add Testcontainers.MsSql package:**
   ```bash
   cd tests/DataTransfer.Integration.Tests
   dotnet add package Testcontainers.MsSql
   ```

3. **Implement integration tests:**
   - Create end-to-end tests using SQL Server containers
   - Test all 4 partition strategies (Date, IntDate, Scd2, Static)
   - Verify full Extract → Parquet → Load pipeline
   - Ensure data integrity after transfer

4. **Test:**
   ```bash
   dotnet test tests/DataTransfer.Integration.Tests
   ```

5. **Commit:**
   ```bash
   git add tests/DataTransfer.Integration.Tests/
   git commit -m "test(integration): add end-to-end tests with Testcontainers [GREEN]

   Added integration tests using SQL Server containers:
   - Test full Extract → Parquet → Load pipeline
   - Cover all 4 partition strategies
   - Verify data integrity after transfer
   - X integration tests passing

   🤖 Generated with [Claude Code](https://claude.com/claude-code)

   Co-Authored-By: Claude <noreply@anthropic.com>"
   ```

## Quick Commands Reference

```bash
# Build solution
dotnet build

# Run all tests
dotnet test

# Run console app
dotnet run --project src/DataTransfer.Console

# Check git status
git status

# See recent commits
git log --oneline -5
```

## Key Architecture Points

1. **Layered architecture:**
   Console → Pipeline → (SqlServer + Parquet) → Core

2. **Dependency Injection:**
   - ITableExtractor → SqlTableExtractor
   - IParquetStorage → ParquetStorage (constructor: basePath)
   - IDataLoader → SqlDataLoader
   - DataTransferOrchestrator

3. **Data flow:**
   Extract (SQL → JSON) → Write (JSON → Parquet) → Read (Parquet → JSON) → Load (JSON → SQL)

4. **Configuration:**
   - Loaded via ConfigurationLoader
   - Validated via ConfigurationValidator
   - Located at `config/appsettings.json`

## Classes You Need to Use

All already implemented and tested:

```csharp
// Load config
var configLoader = new ConfigurationLoader();
var config = await configLoader.LoadAsync("config/appsettings.json");

// Validate config
var validator = new ConfigurationValidator();
var result = validator.Validate(config);

// Transfer a table
var orchestrator = new DataTransferOrchestrator(extractor, storage, loader, logger);
var transferResult = await orchestrator.TransferTableAsync(
    tableConfig,
    sourceConnectionString,
    destinationConnectionString);
```

## Project Files

- `DataTransfer.sln` - Solution file
- `src/DataTransfer.Console/` - Your target directory
- `config/appsettings.json` - Configuration file
- `CLAUDE.md` - Project instructions (TDD, commit format)
- `requirements.md` - Original requirements

## Git Workflow

1. Always follow TDD: RED → GREEN → REFACTOR
2. Commit format:
   ```
   <type>(<scope>): <description> [PHASE]

   Body with details

   🤖 Generated with [Claude Code](https://claude.com/claude-code)

   Co-Authored-By: Claude <noreply@anthropic.com>
   ```
3. Types: feat, fix, refactor, test, docs
4. Phases: [RED], [GREEN], [REFACTOR], or combined [GREEN+REFACTOR]

## Success Criteria for Integration Tests

✅ Testcontainers.MsSql package added
✅ SQL Server container spins up successfully
✅ Tests create source tables with sample data
✅ Full pipeline executes (Extract → Parquet → Load)
✅ Data integrity verified in destination
✅ All 4 partition types tested
✅ All tests passing (107 + new integration tests)

## After Integration Tests are Done

Next tasks:
- Update Docker deployment for .NET 8
- Update README.md with comprehensive usage docs
- Optional: Add performance benchmarks

## Context Files

All context is preserved in these files:
1. `IMPLEMENTATION_STATUS.md` - What's done, what remains, next tasks
2. `CONSOLE_APP_SPEC.md` - Console app implementation details (COMPLETE)
3. `TEST_COVERAGE_SUMMARY.md` - All 107 tests documented
4. `ARCHITECTURE.md` - Technical architecture and design decisions
5. `QUICK_START.md` - This file (quick reference)

## Important Notes

- DO NOT create unnecessary files
- ALWAYS prefer editing existing files
- Follow TDD strictly
- Commit after each phase
- Use provided commit format exactly
- All dependencies already configured in other projects
- 107 tests passing - don't break them!

## Need Help?

Read in this order:
1. QUICK_START.md (this file)
2. CONSOLE_APP_SPEC.md (implementation details)
3. IMPLEMENTATION_STATUS.md (full context)
4. ARCHITECTURE.md (deep dive)

All files are comprehensive and contain everything needed to continue the project successfully.

Good luck! 🚀
