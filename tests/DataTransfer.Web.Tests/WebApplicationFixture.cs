using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Microsoft.Data.SqlClient;
using Respawn;
using Testcontainers.MsSql;
using Xunit;

namespace DataTransfer.Web.Tests;

/// <summary>
/// xUnit CollectionFixture that starts SQL Server container and web application once
/// Shared across all tests for optimal performance with Respawn for state cleanup
/// </summary>
public class WebApplicationFixture : IAsyncLifetime
{
    private Process? _webProcess;
    private MsSqlContainer? _sqlContainer;
    private const string ProjectPath = "src/DataTransfer.Web";
    private const string BaseUrl = "http://localhost:5000";
    private const int ServerPort = 5000;
    private const string SqlPassword = "YourStrong@Passw0rd";

    public string Url => BaseUrl;
    public string SqlConnectionString { get; private set; } = string.Empty;
    public Respawner? DatabaseCheckpoint { get; private set; }

    public async Task InitializeAsync()
    {
        // Step 1: Start SQL Server container (reused across all tests)
        Console.WriteLine("🐳 Starting SQL Server container...");

        _sqlContainer = new MsSqlBuilder()
            .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
            .WithPassword(SqlPassword)
            .WithPortBinding(1433, assignRandomHostPort: true) // Use random port to avoid conflicts
            .WithCleanUp(true)
            .Build();

        await _sqlContainer.StartAsync();
        SqlConnectionString = _sqlContainer.GetConnectionString();

        Console.WriteLine($"✓ SQL Server ready: {SqlConnectionString}");

        // Step 2: Seed test databases
        await SeedTestDatabasesAsync();

        // Step 3: Initialize Respawn checkpoint for state cleanup
        await using var connection = new SqlConnection(SqlConnectionString);
        await connection.OpenAsync();

        DatabaseCheckpoint = await Respawner.CreateAsync(connection, new RespawnerOptions
        {
            DbAdapter = DbAdapter.SqlServer,
            SchemasToInclude = new[] { "dbo" },
            TablesToIgnore = new[] { new Respawn.Graph.Table("__EFMigrationsHistory") }
        });

        Console.WriteLine("✓ Respawn checkpoint created");

        // Step 4: Start web application (check if already running)
        if (IsPortInUse(ServerPort))
        {
            Console.WriteLine($"⚠️  Port {ServerPort} already in use - assuming web server is running");
            return;
        }

        Console.WriteLine($"Starting web application at {BaseUrl}...");

        // Set environment variable to use test SQL Server
        var startInfo = new ProcessStartInfo
        {
            FileName = "dotnet",
            Arguments = $"run --project {ProjectPath} --urls {BaseUrl}",
            WorkingDirectory = "/home/richard/sonnet45",
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };

        // Override connection string to use test container
        startInfo.EnvironmentVariables["ConnectionStrings__LocalDemo"] = SqlConnectionString;

        _webProcess = new Process { StartInfo = startInfo };

        // Capture output for debugging
        _webProcess.OutputDataReceived += (sender, e) =>
        {
            if (!string.IsNullOrEmpty(e.Data))
            {
                Console.WriteLine($"[WebApp] {e.Data}");
            }
        };

        _webProcess.ErrorDataReceived += (sender, e) =>
        {
            if (!string.IsNullOrEmpty(e.Data))
            {
                Console.Error.WriteLine($"[WebApp ERROR] {e.Data}");
            }
        };

        _webProcess.Start();
        _webProcess.BeginOutputReadLine();
        _webProcess.BeginErrorReadLine();

        // Wait for server to be ready
        var ready = await WaitForServerReady(BaseUrl, TimeSpan.FromSeconds(30));

        if (!ready)
        {
            _webProcess?.Kill(true);
            throw new InvalidOperationException($"Web server failed to start at {BaseUrl} within 30 seconds");
        }

        Console.WriteLine($"✓ Web application ready at {BaseUrl}");
    }

    /// <summary>
    /// Seeds test databases with minimal schema for dropdown tests
    /// </summary>
    private async Task SeedTestDatabasesAsync()
    {
        await using var connection = new SqlConnection(SqlConnectionString);
        await connection.OpenAsync();

        // Create test databases
        await using var createDbCmd = connection.CreateCommand();
        createDbCmd.CommandText = @"
            IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'TestSource')
                CREATE DATABASE TestSource;
            IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'TestDestination')
                CREATE DATABASE TestDestination;
        ";
        await createDbCmd.ExecuteNonQueryAsync();

        Console.WriteLine("✓ Test databases created: TestSource, TestDestination");

        // Seed TestSource with sample schema
        await connection.ChangeDatabaseAsync("TestSource");

        await using var createSchemaCmd = connection.CreateCommand();
        createSchemaCmd.CommandText = @"
            -- Create schemas
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'sales')
                EXEC('CREATE SCHEMA sales');
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'hr')
                EXEC('CREATE SCHEMA hr');

            -- Create sample tables
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Customers' AND schema_id = SCHEMA_ID('dbo'))
                CREATE TABLE dbo.Customers (Id INT PRIMARY KEY, Name NVARCHAR(100));

            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Orders' AND schema_id = SCHEMA_ID('sales'))
                CREATE TABLE sales.Orders (Id INT PRIMARY KEY, CustomerId INT, OrderDate DATE);

            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Products' AND schema_id = SCHEMA_ID('sales'))
                CREATE TABLE sales.Products (Id INT PRIMARY KEY, ProductName NVARCHAR(100), Price DECIMAL(10,2));

            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Employees' AND schema_id = SCHEMA_ID('hr'))
                CREATE TABLE hr.Employees (Id INT PRIMARY KEY, Name NVARCHAR(100), Department NVARCHAR(50));

            -- Insert sample data
            IF NOT EXISTS (SELECT * FROM dbo.Customers)
            BEGIN
                INSERT INTO dbo.Customers VALUES (1, 'Acme Corp'), (2, 'TechStart Inc'), (3, 'Global Solutions');
                INSERT INTO sales.Orders VALUES (1, 1, '2025-01-15'), (2, 2, '2025-01-16');
                INSERT INTO sales.Products VALUES (1, 'Widget', 99.99), (2, 'Gadget', 149.99);
                INSERT INTO hr.Employees VALUES (1, 'Alice', 'Engineering'), (2, 'Bob', 'Sales');
            END
        ";
        await createSchemaCmd.ExecuteNonQueryAsync();

        Console.WriteLine("✓ Test schema seeded: dbo, sales, hr schemas with sample tables");

        // Seed TestDestination with empty tables (matching structure)
        await connection.ChangeDatabaseAsync("TestDestination");

        await using var createDestSchemaCmd = connection.CreateCommand();
        createDestSchemaCmd.CommandText = @"
            -- Create schemas
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'sales')
                EXEC('CREATE SCHEMA sales');
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'hr')
                EXEC('CREATE SCHEMA hr');

            -- Create empty tables matching TestSource structure
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Customers' AND schema_id = SCHEMA_ID('dbo'))
                CREATE TABLE dbo.Customers (Id INT PRIMARY KEY, Name NVARCHAR(100));

            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Orders' AND schema_id = SCHEMA_ID('sales'))
                CREATE TABLE sales.Orders (Id INT PRIMARY KEY, CustomerId INT, OrderDate DATE);

            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Products' AND schema_id = SCHEMA_ID('sales'))
                CREATE TABLE sales.Products (Id INT PRIMARY KEY, ProductName NVARCHAR(100), Price DECIMAL(10,2));

            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Employees' AND schema_id = SCHEMA_ID('hr'))
                CREATE TABLE hr.Employees (Id INT PRIMARY KEY, Name NVARCHAR(100), Department NVARCHAR(50));
        ";
        await createDestSchemaCmd.ExecuteNonQueryAsync();

        Console.WriteLine("✓ TestDestination seeded: empty tables ready for imports");
    }

    /// <summary>
    /// Resets database state using Respawn - call between tests if needed
    /// </summary>
    public async Task ResetDatabaseAsync()
    {
        if (DatabaseCheckpoint == null)
            throw new InvalidOperationException("Database checkpoint not initialized");

        await using var connection = new SqlConnection(SqlConnectionString);
        await connection.OpenAsync();
        await DatabaseCheckpoint.ResetAsync(connection);

        Console.WriteLine("🔄 Database state reset via Respawn");
    }

    public async Task DisposeAsync()
    {
        if (_webProcess != null && !_webProcess.HasExited)
        {
            Console.WriteLine("Shutting down web application...");
            _webProcess.Kill(true);
            _webProcess.WaitForExit(5000);
            _webProcess.Dispose();
        }

        if (_sqlContainer != null)
        {
            Console.WriteLine("Stopping SQL Server container...");
            await _sqlContainer.StopAsync();
            await _sqlContainer.DisposeAsync();
        }
    }

    private static bool IsPortInUse(int port)
    {
        try
        {
            using var client = new TcpClient();
            client.Connect("localhost", port);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static async Task<bool> WaitForServerReady(string url, TimeSpan timeout)
    {
        using var httpClient = new HttpClient();
        var deadline = DateTime.UtcNow.Add(timeout);

        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var response = await httpClient.GetAsync(url);
                if (response.StatusCode == HttpStatusCode.OK)
                {
                    return true;
                }
            }
            catch
            {
                // Server not ready yet, continue waiting
            }

            await Task.Delay(500);
        }

        return false;
    }
}

/// <summary>
/// Collection definition for web UI tests requiring a running server
/// </summary>
[CollectionDefinition("WebApplication")]
public class WebApplicationCollection : ICollectionFixture<WebApplicationFixture>
{
    // This class is never instantiated - it's just a marker for xUnit
}
