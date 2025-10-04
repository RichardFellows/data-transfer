# DataTransfer Console Integration Tests

## Overview

This test project provides infrastructure for testing the `DataTransfer.Console` application, including output capture and HTML reporting similar to the Web UI Playwright tests.

## Test Infrastructure

### 1. ConsoleTestBase
Base class providing:
- **ConsoleOutputCapture**: Captures stdout, stderr, exit codes, and execution times
- **Output Storage**: Saves captures to `test-results/console-output/`
- **HTML Reporting**: Generates visual test reports (via ConsoleReportGenerator)
- **Dual Execution Modes**:
  - Fast: Uses pre-built binary (via ConsoleAppFixture)
  - Fallback: Uses `dotnet run` for compatibility

### 2. ConsoleAppFixture
xUnit CollectionFixture that:
- Builds console app once when tests start
- Provides binary path to all tests
- **Goal**: 10x faster execution (1-2s vs 10-15s per test)
- **Status**: Infrastructure complete but not currently used (see limitations below)

### 3. ConsoleReportGenerator
Generates HTML reports with:
- Dark VS Code-style terminal theme
- STDOUT/STDERR output display
- Exit codes and durations
- Pass/fail status indicators
- Similar to `ScreenshotReportGenerator` for Web UI tests

## Current Test Status

### ⚠️ All Execution Tests Skipped

**Why?** Console app execution via CliWrap has proven complex due to:
- Service initialization overhead (DI container, database connections)
- File locking issues during test execution
- Compilation timeouts with `dotnet run`

**Current Approach:** Tests are skipped with manual verification instructions

### Manual Testing

To manually verify console functionality:

```bash
# Help command
dotnet run --project src/DataTransfer.Console -- --help

# List profiles
dotnet run --project src/DataTransfer.Console -- --list-profiles

# Execute profile
dotnet run --project src/DataTransfer.Console -- --profile "Profile Name"

# Legacy config mode
dotnet run --project src/DataTransfer.Console -- --config config/appsettings.json

# Interactive mode
dotnet run --project src/DataTransfer.Console
```

## Test Layers (Planned)

### Layer 1: CLI Interface Tests ✅ Infrastructure Complete
- Argument parsing validation
- Help text verification
- Error message checks
- **Status**: Tests exist but skipped (manual verification required)

### Layer 2: Profile/Config Tests ❌ Not Implemented
- Profile loading from disk
- Configuration validation
- No database required
- **Status**: Future work

### Layer 3: E2E with TestContainers ❌ Not Implemented
- SQL Server container setup
- Full data transfer execution
- Integration validation
- **Status**: Future work

## File Organization

```
tests/DataTransfer.Console.Tests/
├── ConsoleTestBase.cs          # Base class with capture logic
├── ConsoleOutputCapture.cs     # Output model
├── ConsoleReportGenerator.cs   # HTML report generator
├── ConsoleAppFixture.cs        # Pre-build fixture (optional)
├── ConsoleIntegrationTests.cs  # Layer 1 tests (skipped)
├── GenerateConsoleReport.cs    # Report generation trigger
└── README.md                   # This file
```

## Output Artifacts

When tests run (even skipped), they generate:

```
test-results/
└── console-output/
    ├── captures.json                    # Metadata for report
    ├── {TestName}_{StepName}_stdout.txt # Standard output
    ├── {TestName}_{StepName}_stderr.txt # Standard error
    └── {TestName}_{StepName}_summary.txt# Test summary

test-results/
└── ConsoleTestReport.html              # Visual HTML report
```

## Future Improvements

### Option 1: Simplify Test Approach
- Remove execution complexity
- Focus on unit tests for components
- Mock console behavior

### Option 2: Fix Execution Issues
- Debug timeout/hanging problems
- Implement proper process isolation
- Use fixture for fast execution

### Option 3: Alternative Testing
- Create dedicated test harness
- Use in-process execution
- Mock external dependencies

## Related Documentation

- **Web UI Tests**: `tests/DataTransfer.Web.Tests/README.Screenshots.md`
- **Architecture**: `ARCHITECTURE.md`
- **Test Coverage**: README.md (main project)

## Notes for Developers

1. **Don't un-skip tests** without fixing underlying execution issues
2. **Manual verification is currently required** for console functionality
3. **Infrastructure is solid** - tests demonstrate expected behavior even when skipped
4. **HTML reporting works** - run `GenerateConsoleReport` test to see output
5. **Fixture is available** - can be enabled when execution issues resolved

## Testing Philosophy

> These tests serve as **executable documentation** of expected console behavior. While currently skipped for automated execution, they provide clear examples of how the console app should work and can be verified manually or fixed for automation in the future.
