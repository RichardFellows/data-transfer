using DataTransfer.Configuration;
using DataTransfer.Core.Interfaces;
using DataTransfer.Parquet;
using DataTransfer.Pipeline;
using DataTransfer.SqlServer;
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

    var builder = Host.CreateDefaultBuilder(args);

    builder.ConfigureServices((context, services) =>
    {
        services.AddLogging(loggingBuilder =>
            loggingBuilder.AddSerilog(dispose: true));

        // Register implementations
        services.AddSingleton<SqlQueryBuilder>();
        services.AddSingleton<ITableExtractor, SqlTableExtractor>();
        services.AddSingleton<IParquetStorage>(sp =>
            new ParquetStorage("parquet-output"));
        services.AddSingleton<IDataLoader, SqlDataLoader>();
        services.AddSingleton<DataTransferOrchestrator>();
        services.AddSingleton<ConfigurationLoader>();
        services.AddSingleton<ConfigurationValidator>();
    });

    var host = builder.Build();

    // Get services
    var configLoader = host.Services.GetRequiredService<ConfigurationLoader>();
    var validator = host.Services.GetRequiredService<ConfigurationValidator>();
    var orchestrator = host.Services.GetRequiredService<DataTransferOrchestrator>();
    var logger = host.Services.GetRequiredService<ILogger<Program>>();

    // Parse command-line arguments
    string configPath = "config/appsettings.json";
    for (int i = 0; i < args.Length; i++)
    {
        if (args[i] == "--config" && i + 1 < args.Length)
        {
            configPath = args[i + 1];
            break;
        }
    }

    // Load configuration
    logger.LogInformation("Loading configuration from {ConfigPath}", configPath);
    var config = await configLoader.LoadAsync(configPath);

    // Validate configuration
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

    logger.LogInformation("Configuration loaded and validated successfully");
    logger.LogInformation("Processing {TableCount} tables", config.Tables.Count);

    int successCount = 0;
    int failureCount = 0;
    long totalRowsExtracted = 0;
    long totalRowsLoaded = 0;

    foreach (var tableConfig in config.Tables)
    {
        try
        {
            logger.LogInformation("Starting transfer for {Table}", tableConfig.Source.FullyQualifiedName);

            var result = await orchestrator.TransferTableAsync(
                tableConfig,
                config.Connections.Source,
                config.Connections.Destination);

            if (result.Success)
            {
                successCount++;
                totalRowsExtracted += result.RowsExtracted;
                totalRowsLoaded += result.RowsLoaded;
                logger.LogInformation("✓ Completed {Table}: {Rows} rows in {Duration}ms",
                    tableConfig.Source.FullyQualifiedName,
                    result.RowsLoaded,
                    result.Duration.TotalMilliseconds);
            }
            else
            {
                failureCount++;
                logger.LogError("✗ Failed {Table}: {Error}",
                    tableConfig.Source.FullyQualifiedName,
                    result.ErrorMessage);
            }
        }
        catch (Exception ex)
        {
            failureCount++;
            logger.LogError(ex, "✗ Exception processing {Table}: {Message}",
                tableConfig.Source.FullyQualifiedName,
                ex.Message);
        }
    }

    logger.LogInformation("=====================================");
    logger.LogInformation("Transfer Summary:");
    logger.LogInformation("  Success: {Success}", successCount);
    logger.LogInformation("  Failed: {Failed}", failureCount);
    logger.LogInformation("  Total Rows Extracted: {Extracted}", totalRowsExtracted);
    logger.LogInformation("  Total Rows Loaded: {Loaded}", totalRowsLoaded);
    logger.LogInformation("=====================================");

    return failureCount == 0 ? 0 : 1;
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
