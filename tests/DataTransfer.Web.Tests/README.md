# DataTransfer Web UI Tests

End-to-end tests for the DataTransfer Blazor web interface using Playwright.

## Prerequisites

1. **Playwright Browsers**: Chromium browser must be installed
   ```bash
   playwright install chromium
   ```

2. **Web Application**: The DataTransfer.Web app must be running on port 5000
   ```bash
   # In one terminal, start the web app:
   dotnet run --project src/DataTransfer.Web --urls http://localhost:5000
   ```

3. **Optional - SQL Server**: For testing actual data transfers (not just UI)
   ```bash
   ./demo/run-bidirectional-demo.sh
   # This sets up Docker SQL Server with test databases
   ```

## Running the Tests

```bash
# Run all web UI tests
dotnet test tests/DataTransfer.Web.Tests

# Run specific test
dotnet test tests/DataTransfer.Web.Tests --filter "FullyQualifiedName~HomePage_Should_Load"

# Run with verbose output
dotnet test tests/DataTransfer.Web.Tests -v normal
```

## Test Coverage

The test suite verifies:

### ✅ Home Page (Dashboard)
- Page loads and displays title
- Statistics cards are present
- Navigation links work

### ✅ New Transfer Page
- Form loads with transfer type selector
- SQL→Parquet: Shows correct source/destination fields
- Parquet→SQL: Shows correct source/destination fields
- SQL→SQL: Documents current error behavior (NotImplementedException)

### ✅ History Page
- Page loads with table or "no transfers" message
- Table has correct columns when transfers exist
- Refresh button is available

### ✅ Navigation
- Can navigate between all pages
- URLs update correctly

## Current Known Issues (Captured by Tests)

1. **SQL→SQL Transfer Not Implemented in Web UI**
   - Test: `NewTransferPage_SqlToSql_Submit_Should_Show_Error_Or_NotImplemented`
   - Expected: Shows NotImplementedException error
   - This is by design - SQL→SQL uses different orchestrator (DataTransferOrchestrator)
   - **Fix needed**: Either implement support or remove option from UI

## Test Design Philosophy

These tests follow a "document-then-fix" approach:
1. Tests capture **current behavior** (including errors)
2. Tests define **expected behavior**
3. Failing tests guide fixes
4. Once fixed, tests verify correct behavior

## Troubleshooting

**Test fails with "Target closed" or connection errors:**
- Ensure web app is running on port 5000
- Check firewall settings
- Verify `http://localhost:5000` is accessible in browser

**Playwright browser not found:**
- Run `playwright install chromium`
- Verify PATH includes playwright

**Tests are flaky:**
- Increase wait timeouts in test code
- Check system resources (memory, CPU)
- Run tests individually to isolate issues
