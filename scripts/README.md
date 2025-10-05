# Manual Testing Scripts

Scripts for setting up and tearing down the manual testing environment.

## Quick Start

```bash
# Start everything
./scripts/start-manual-testing.sh

# Open browser to http://localhost:5000

# When done, stop everything
./scripts/stop-manual-testing.sh
```

## What `start-manual-testing.sh` Does

1. **Checks Docker** - Verifies Docker is running
2. **Starts SQL Server** - Launches SQL Server 2022 container (or reuses existing)
3. **Waits for SQL Ready** - Ensures SQL Server is accepting connections
4. **Seeds Databases** - Creates and populates test databases:
   - `TestSource` with sample data
   - `TestDestination` (empty)
5. **Configures Web App** - Updates `appsettings.json` with connection string
6. **Starts Web Server** - Launches the Blazor app on port 5000

### Test Data Created

**TestSource Database (for SQL→Parquet exports):**
- `dbo.Customers` (3 rows)
- `sales.Orders` (2 rows)
- `sales.Products` (2 rows)
- `hr.Employees` (2 rows)

**TestDestination Database (for Parquet→SQL imports):**
- `dbo.Customers` (empty, ready for imports)
- `sales.Orders` (empty, ready for imports)
- `sales.Products` (empty, ready for imports)
- `hr.Employees` (empty, ready for imports)

## What `stop-manual-testing.sh` Does

1. Stops the web server
2. Stops the SQL Server container (doesn't remove it)
3. Restores original `appsettings.json`

## Connection Details

**SQL Server:**
- Host: `localhost:1433`
- User: `sa`
- Password: `YourStrong@Passw0rd`
- Container: `datatransfer-manual-test-sql`

**Web Application:**
- URL: `http://localhost:5000`
- Logs: `/tmp/datatransfer-web.log`

## Troubleshooting

**Port 1433 already in use:**
```bash
# Find and stop conflicting process
sudo lsof -i :1433
docker ps  # Check for other SQL containers
```

**Port 5000 already in use:**
```bash
# The script automatically kills existing process
# Or manually:
kill $(lsof -t -i:5000)
```

**SQL Server won't start:**
```bash
# Check container logs
docker logs datatransfer-manual-test-sql

# Remove and recreate
docker rm -f datatransfer-manual-test-sql
./scripts/start-manual-testing.sh
```

**Web server won't start:**
```bash
# Check logs
tail -f /tmp/datatransfer-web.log

# Check if dotnet is installed
dotnet --version
```

## Manual Cleanup

If you want to completely remove everything:

```bash
# Stop services
./scripts/stop-manual-testing.sh

# Remove SQL container
docker rm -f datatransfer-manual-test-sql

# Remove web logs
rm /tmp/datatransfer-web.log

# Remove appsettings backup
rm src/DataTransfer.Web/appsettings.json.backup
```

## Automated Testing vs Manual Testing

**Automated Tests** (via `dotnet test`):
- Use TestContainers to create ephemeral SQL Server
- Automatic setup and teardown
- Isolated, reproducible environment

**Manual Testing** (via these scripts):
- Persistent SQL Server container
- Allows interactive testing
- Can inspect database state between operations
- Useful for debugging and exploration
