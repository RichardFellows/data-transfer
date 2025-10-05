# DataTransfer Web UI Tests

End-to-end tests for the DataTransfer Blazor web interface using Playwright.

## Prerequisites

1. **Playwright Browsers**: Chromium browser must be installed
   ```bash
   playwright install chromium
   ```

2. **SQL Server**: The test suite automatically starts/stops SQL Server via TestContainers
   - `WebApplicationFixture` starts SQL Server 2022 container (~10s startup)
   - Seeds test databases: TestSource, TestDestination
   - Creates schemas (dbo, sales, hr) with sample tables
   - Uses Respawn for fast database state reset between tests
   - Container shared across all tests for optimal performance
   - No manual SQL Server setup needed!

3. **Web Application**: The test suite automatically starts/stops the web server
   - `WebApplicationFixture` handles server lifecycle
   - Server starts on port 5000 before tests run
   - Server shuts down automatically after tests complete
   - No manual server startup needed!

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
- Only shows supported transfer types (SQL→Parquet, Parquet→SQL)
- SQL→Parquet: Shows correct source/destination fields
- Parquet→SQL: Shows correct source/destination fields
- SQL→SQL: Option removed (requires different orchestrator)

### ✅ History Page
- Page loads with table or "no transfers" message
- Table has correct columns when transfers exist
- Refresh button is available

### ✅ Navigation
- Can navigate between all pages
- URLs update correctly

## Design Decisions

1. **SQL→SQL Transfer Not Available in Web UI**
   - Test: `NewTransferPage_Should_Only_Show_Supported_Transfer_Types`
   - SQL→SQL transfers use the legacy `DataTransferOrchestrator` with multi-table configuration
   - Web UI focuses on new bi-directional features using `UnifiedTransferOrchestrator`
   - For SQL→SQL migrations, use the console application with `demo-config.json`

## Test Design Philosophy

These tests follow a "document-then-fix" approach:
1. Tests capture **current behavior** (including errors)
2. Tests define **expected behavior**
3. Failing tests guide fixes
4. Once fixed, tests verify correct behavior

## Troubleshooting

**Docker not available:**
- TestContainers requires Docker Desktop running
- Verify Docker is running: `docker ps`
- On WSL2: Ensure Docker Desktop integration is enabled

**Port 5000 already in use:**
- The `WebApplicationFixture` detects if port 5000 is in use
- If a server is already running, it uses that instead of starting a new one
- To use a fresh server, kill any existing processes on port 5000

**SQL Server container slow to start:**
- First test run takes ~10-15s to download SQL Server image
- Subsequent runs use cached image (~10s startup)
- Container is shared across all tests for performance

**Playwright browser not found:**
- Run `playwright install chromium`
- Verify PATH includes playwright

**Tests timeout on dropdown selection:**
- These are UI/selector test issues, not infrastructure problems
- TestContainers provides working SQL Server with test data
- Check selector values match current UI implementation

**Tests are flaky:**
- Increase wait timeouts in test code
- Check system resources (memory, CPU)
- Run tests individually to isolate issues
