using DataTransfer.Configuration;
using DataTransfer.Configuration.Models;
using DataTransfer.Configuration.Services;
using DataTransfer.Core.Interfaces;
using DataTransfer.Core.Models;
using DataTransfer.Parquet;
using DataTransfer.Pipeline;
using DataTransfer.SqlServer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;

// Configure Serilog
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .WriteTo.Console()
    .WriteTo.File("logs/datatransfer-.txt", rollingInterval: RollingInterval.Day)
    .CreateLogger();

try
{
    Log.Information("DataTransfer Console Application Starting");

    var host = CreateHost(args);
    var logger = host.Services.GetRequiredService<ILogger<Program>>();

    // Determine mode based on arguments
    if (args.Length == 0)
    {
        // Interactive mode
        logger.LogInformation("Starting interactive mode");
        return await RunInteractiveModeAsync(host.Services);
    }
    else
    {
        // Command-line mode
        logger.LogInformation("Starting command-line mode");
        return await RunCommandLineModeAsync(args, host.Services);
    }
}
catch (Exception ex)
{
    Log.Fatal(ex, "Fatal error: {Message}", ex.Message);
    return 1;
}
finally
{
    Log.CloseAndFlush();
}

static IHost CreateHost(string[] args)
{
    var builder = Host.CreateDefaultBuilder(args);

    builder.ConfigureServices((context, services) =>
    {
        services.AddLogging(loggingBuilder =>
            loggingBuilder.AddSerilog(dispose: true));

        // Core services
        services.AddSingleton<SqlQueryBuilder>();
        services.AddSingleton<ITableExtractor, SqlTableExtractor>();
        services.AddSingleton<IDataLoader, SqlDataLoader>();
        services.AddSingleton<IParquetStorage>(sp =>
            new ParquetStorage("parquet-output"));
        services.AddSingleton<IParquetExtractor>(sp =>
            new ParquetExtractor("parquet-output"));
        services.AddSingleton<IParquetWriter, ParquetWriter>();

        // Orchestrators (both old and new)
        services.AddSingleton<DataTransferOrchestrator>();
        services.AddSingleton<UnifiedTransferOrchestrator>();

        // Configuration services
        services.AddSingleton<ConfigurationLoader>();
        services.AddSingleton<ConfigurationValidator>();

        // Profile service
        var profilesDir = context.Configuration["Profiles:StorageDirectory"] ?? "./profiles";
        services.AddSingleton<TransferProfileService>(sp =>
            new TransferProfileService(
                sp.GetRequiredService<ILogger<TransferProfileService>>(),
                profilesDir));
    });

    return builder.Build();
}

static async Task<int> RunInteractiveModeAsync(IServiceProvider services)
{
    var profileService = services.GetRequiredService<TransferProfileService>();
    var orchestrator = services.GetRequiredService<UnifiedTransferOrchestrator>();
    var logger = services.GetRequiredService<ILogger<Program>>();

    while (true)
    {
        Console.WriteLine();
        Console.WriteLine("╔════════════════════════════════════════════╗");
        Console.WriteLine("║   🔄 DataTransfer Console                 ║");
        Console.WriteLine("╚════════════════════════════════════════════╝");
        Console.WriteLine();
        Console.WriteLine("1. Run from config file (legacy)");
        Console.WriteLine("2. Load saved profile");
        Console.WriteLine("3. List all profiles");
        Console.WriteLine("4. Exit");
        Console.WriteLine();
        Console.Write("Select option (1-4): ");

        var choice = Console.ReadLine();

        switch (choice)
        {
            case "1":
                await RunFromConfigFileAsync(services);
                break;
            case "2":
                await RunFromProfileAsync(profileService, orchestrator, logger);
                break;
            case "3":
                await ListProfilesAsync(profileService);
                break;
            case "4":
                Console.WriteLine("Exiting...");
                return 0;
            default:
                Console.WriteLine("Invalid option. Please try again.");
                break;
        }

        Console.WriteLine();
        Console.WriteLine("Press any key to continue...");
        Console.ReadKey();
    }
}

static async Task RunFromConfigFileAsync(IServiceProvider services)
{
    var configLoader = services.GetRequiredService<ConfigurationLoader>();
    var validator = services.GetRequiredService<ConfigurationValidator>();
    var orchestrator = services.GetRequiredService<DataTransferOrchestrator>();
    var logger = services.GetRequiredService<ILogger<Program>>();

    Console.Write("Enter config file path (default: config/appsettings.json): ");
    var configPath = Console.ReadLine();
    if (string.IsNullOrWhiteSpace(configPath))
    {
        configPath = "config/appsettings.json";
    }

    try
    {
        logger.LogInformation("Loading configuration from {ConfigPath}", configPath);
        var config = await configLoader.LoadAsync(configPath);

        // Validate configuration
        var validationResult = validator.Validate(config);
        if (!validationResult.IsValid)
        {
            Console.WriteLine("❌ Configuration validation failed:");
            foreach (var error in validationResult.Errors)
            {
                Console.WriteLine($"   - {error}");
            }
            return;
        }

        Console.WriteLine($"✓ Configuration loaded: {config.Tables.Count} tables");
        Console.WriteLine("Press ENTER to execute or Ctrl+C to cancel...");
        Console.ReadLine();

        int successCount = 0;
        int failureCount = 0;
        long totalRowsExtracted = 0;
        long totalRowsLoaded = 0;

        foreach (var tableConfig in config.Tables)
        {
            try
            {
                Console.WriteLine($"Processing {tableConfig.Source.FullyQualifiedName}...");

                var result = await orchestrator.TransferTableAsync(
                    tableConfig,
                    config.Connections.Source,
                    config.Connections.Destination);

                if (result.Success)
                {
                    successCount++;
                    totalRowsExtracted += result.RowsExtracted;
                    totalRowsLoaded += result.RowsLoaded;
                    Console.WriteLine($"✓ Completed: {result.RowsLoaded:N0} rows in {result.Duration.TotalSeconds:F2}s");
                }
                else
                {
                    failureCount++;
                    Console.WriteLine($"✗ Failed: {result.ErrorMessage}");
                }
            }
            catch (Exception ex)
            {
                failureCount++;
                Console.WriteLine($"✗ Exception: {ex.Message}");
            }
        }

        Console.WriteLine();
        Console.WriteLine("=====================================");
        Console.WriteLine("Transfer Summary:");
        Console.WriteLine($"  Success: {successCount}");
        Console.WriteLine($"  Failed: {failureCount}");
        Console.WriteLine($"  Total Rows: {totalRowsLoaded:N0}");
        Console.WriteLine("=====================================");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"❌ Error: {ex.Message}");
        logger.LogError(ex, "Failed to process config file");
    }
}

static async Task RunFromProfileAsync(
    TransferProfileService profileService,
    UnifiedTransferOrchestrator orchestrator,
    Microsoft.Extensions.Logging.ILogger logger)
{
    var profiles = await profileService.GetAllProfilesAsync();

    if (!profiles.Any())
    {
        Console.WriteLine("❌ No profiles found. Create one using the Web UI first.");
        return;
    }

    Console.WriteLine();
    Console.WriteLine("Available Profiles:");
    Console.WriteLine("===================");
    for (int i = 0; i < profiles.Count; i++)
    {
        var p = profiles[i];
        Console.WriteLine($"{i + 1}. {p.ProfileName}");
        Console.WriteLine($"   Type: {p.Configuration.TransferType}");
        if (!string.IsNullOrEmpty(p.Description))
        {
            Console.WriteLine($"   Description: {p.Description}");
        }
        if (p.Tags.Any())
        {
            Console.WriteLine($"   Tags: {string.Join(", ", p.Tags)}");
        }
        Console.WriteLine();
    }

    Console.Write($"Select profile (1-{profiles.Count}): ");
    if (int.TryParse(Console.ReadLine(), out int selection) &&
        selection > 0 && selection <= profiles.Count)
    {
        var profile = profiles[selection - 1];
        Console.WriteLine();
        Console.WriteLine($"✓ Loaded: {profile.ProfileName}");
        Console.WriteLine($"  Type: {profile.Configuration.TransferType}");

        // Show configuration details
        if (profile.Configuration.TransferType == TransferType.SqlToParquet)
        {
            var table = profile.Configuration.Source.Table;
            Console.WriteLine($"  Source: {table?.Database}.{table?.Schema}.{table?.Table}");
            Console.WriteLine($"  Destination: {profile.Configuration.Destination.ParquetPath}");
        }
        else if (profile.Configuration.TransferType == TransferType.ParquetToSql)
        {
            var table = profile.Configuration.Destination.Table;
            Console.WriteLine($"  Source: {profile.Configuration.Source.ParquetPath}");
            Console.WriteLine($"  Destination: {table?.Database}.{table?.Schema}.{table?.Table}");
        }

        Console.WriteLine();
        Console.WriteLine("Press ENTER to execute or Ctrl+C to cancel...");
        Console.ReadLine();

        try
        {
            var transferId = $"console-{Guid.NewGuid():N}";
            logger.LogInformation("Executing profile: {ProfileName} ({TransferId})", profile.ProfileName, transferId);

            var result = await orchestrator.ExecuteTransferAsync(
                profile.Configuration,
                CancellationToken.None);

            if (result.Success)
            {
                Console.WriteLine();
                Console.WriteLine("✓ Transfer completed successfully!");
                Console.WriteLine($"  Rows transferred: {result.RowsLoaded:N0}");
                Console.WriteLine($"  Duration: {result.Duration.TotalSeconds:F2} seconds");
                Console.WriteLine($"  Throughput: {result.RowsLoaded / result.Duration.TotalSeconds:N0} rows/sec");
            }
            else
            {
                Console.WriteLine();
                Console.WriteLine($"✗ Transfer failed: {result.ErrorMessage}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Error executing transfer: {ex.Message}");
            logger.LogError(ex, "Failed to execute profile: {ProfileName}", profile.ProfileName);
        }
    }
    else
    {
        Console.WriteLine("Invalid selection.");
    }
}

static async Task ListProfilesAsync(TransferProfileService profileService)
{
    var profiles = await profileService.GetAllProfilesAsync();

    if (!profiles.Any())
    {
        Console.WriteLine("No profiles found.");
        return;
    }

    Console.WriteLine();
    Console.WriteLine("Saved Profiles:");
    Console.WriteLine("===============");
    foreach (var profile in profiles)
    {
        Console.WriteLine();
        Console.WriteLine($"📋 {profile.ProfileName}");
        Console.WriteLine($"   ID: {profile.ProfileId}");
        Console.WriteLine($"   Type: {profile.Configuration.TransferType}");
        if (!string.IsNullOrEmpty(profile.Description))
        {
            Console.WriteLine($"   Description: {profile.Description}");
        }
        Console.WriteLine($"   Created: {profile.CreatedDate:yyyy-MM-dd HH:mm} by {profile.CreatedBy}");
        if (profile.Tags.Any())
        {
            Console.WriteLine($"   Tags: {string.Join(", ", profile.Tags)}");
        }
    }
}

static async Task<int> RunCommandLineModeAsync(string[] args, IServiceProvider services)
{
    var logger = services.GetRequiredService<ILogger<Program>>();
    var profileService = services.GetRequiredService<TransferProfileService>();
    var orchestrator = services.GetRequiredService<UnifiedTransferOrchestrator>();

    // Parse arguments
    string? profileName = null;
    string? configPath = null;
    bool listProfiles = false;

    for (int i = 0; i < args.Length; i++)
    {
        switch (args[i])
        {
            case "--profile":
                if (i + 1 < args.Length)
                {
                    profileName = args[i + 1];
                    i++;
                }
                break;
            case "--config":
                if (i + 1 < args.Length)
                {
                    configPath = args[i + 1];
                    i++;
                }
                break;
            case "--list-profiles":
                listProfiles = true;
                break;
            case "--help":
                ShowHelp();
                return 0;
        }
    }

    if (listProfiles)
    {
        await ListProfilesAsync(profileService);
        return 0;
    }

    if (!string.IsNullOrEmpty(profileName))
    {
        // Run from profile
        var profiles = await profileService.GetAllProfilesAsync();
        var profile = profiles.FirstOrDefault(p => p.ProfileName == profileName);

        if (profile == null)
        {
            logger.LogError("Profile '{ProfileName}' not found", profileName);
            return 1;
        }

        logger.LogInformation("Executing profile: {ProfileName}", profileName);
        var result = await orchestrator.ExecuteTransferAsync(profile.Configuration, CancellationToken.None);

        if (result.Success)
        {
            logger.LogInformation("✓ Transfer completed: {Rows} rows in {Duration}s",
                result.RowsLoaded, result.Duration.TotalSeconds);
            return 0;
        }
        else
        {
            logger.LogError("✗ Transfer failed: {Error}", result.ErrorMessage);
            return 1;
        }
    }
    else if (!string.IsNullOrEmpty(configPath))
    {
        // Run from config file (legacy mode)
        var configLoader = services.GetRequiredService<ConfigurationLoader>();
        var validator = services.GetRequiredService<ConfigurationValidator>();
        var legacyOrchestrator = services.GetRequiredService<DataTransferOrchestrator>();

        logger.LogInformation("Loading configuration from {ConfigPath}", configPath);
        var config = await configLoader.LoadAsync(configPath);

        var validationResult = validator.Validate(config);
        if (!validationResult.IsValid)
        {
            logger.LogError("Configuration validation failed:");
            foreach (var error in validationResult.Errors)
            {
                logger.LogError("  - {Error}", error);
            }
            return 1;
        }

        int successCount = 0;
        int failureCount = 0;

        foreach (var tableConfig in config.Tables)
        {
            try
            {
                var result = await legacyOrchestrator.TransferTableAsync(
                    tableConfig,
                    config.Connections.Source,
                    config.Connections.Destination);

                if (result.Success)
                {
                    successCount++;
                    logger.LogInformation("✓ {Table}: {Rows} rows",
                        tableConfig.Source.FullyQualifiedName, result.RowsLoaded);
                }
                else
                {
                    failureCount++;
                    logger.LogError("✗ {Table}: {Error}",
                        tableConfig.Source.FullyQualifiedName, result.ErrorMessage);
                }
            }
            catch (Exception ex)
            {
                failureCount++;
                logger.LogError(ex, "✗ {Table}: Exception", tableConfig.Source.FullyQualifiedName);
            }
        }

        logger.LogInformation("Summary: {Success} succeeded, {Failed} failed", successCount, failureCount);
        return failureCount == 0 ? 0 : 1;
    }
    else
    {
        ShowHelp();
        return 1;
    }
}

static void ShowHelp()
{
    Console.WriteLine();
    Console.WriteLine("DataTransfer Console - Usage:");
    Console.WriteLine("=============================");
    Console.WriteLine();
    Console.WriteLine("Interactive mode (no arguments):");
    Console.WriteLine("  dotnet run --project src/DataTransfer.Console");
    Console.WriteLine();
    Console.WriteLine("Command-line mode:");
    Console.WriteLine("  --profile <name>      Run transfer from saved profile");
    Console.WriteLine("  --config <path>       Run transfer from config file (legacy)");
    Console.WriteLine("  --list-profiles       List all saved profiles");
    Console.WriteLine("  --help                Show this help message");
    Console.WriteLine();
    Console.WriteLine("Examples:");
    Console.WriteLine("  dotnet run --project src/DataTransfer.Console -- --profile \"Daily Orders Extract\"");
    Console.WriteLine("  dotnet run --project src/DataTransfer.Console -- --list-profiles");
    Console.WriteLine("  dotnet run --project src/DataTransfer.Console -- --config config/appsettings.json");
    Console.WriteLine();
}

// Make Program class accessible to tests
public partial class Program { }
