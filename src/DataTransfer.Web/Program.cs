using DataTransfer.Core.Interfaces;
using DataTransfer.Iceberg.Catalog;
using DataTransfer.Iceberg.ChangeDetection;
using DataTransfer.Iceberg.Integration;
using DataTransfer.Iceberg.Readers;
using DataTransfer.Iceberg.Watermarks;
using DataTransfer.Iceberg.Writers;
using DataTransfer.Parquet;
using DataTransfer.Pipeline;
using DataTransfer.SqlServer;
using DataTransfer.Web.Components;
using DataTransfer.Web.Services;
using DataTransfer.Configuration.Services;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

// Configure Serilog
Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(builder.Configuration)
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .CreateLogger();

builder.Services.AddSerilog();

// Add services to the container.
builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

// Add DataTransfer services
builder.Services.AddSingleton<SqlQueryBuilder>();
builder.Services.AddSingleton<ITableExtractor, SqlTableExtractor>();
builder.Services.AddSingleton<IDataLoader, SqlDataLoader>();
builder.Services.AddSingleton<IParquetStorage>(sp =>
    new ParquetStorage("./parquet-files"));
builder.Services.AddSingleton<IParquetExtractor>(sp =>
    new ParquetExtractor("./parquet-files"));
builder.Services.AddSingleton<IParquetWriter, ParquetWriter>();
builder.Services.AddSingleton<UnifiedTransferOrchestrator>();

// Iceberg services
var icebergWarehousePath = builder.Configuration["Iceberg:WarehousePath"] ?? "./iceberg-warehouse";
builder.Services.AddSingleton<FilesystemCatalog>(sp =>
    new FilesystemCatalog(
        icebergWarehousePath,
        sp.GetRequiredService<ILogger<FilesystemCatalog>>()));

builder.Services.AddSingleton<SqlServerToIcebergExporter>();
builder.Services.AddSingleton<IcebergTableWriter>();
builder.Services.AddSingleton<IcebergParquetWriter>();
builder.Services.AddSingleton<IcebergReader>();
builder.Services.AddSingleton<IcebergAppender>();
builder.Services.AddSingleton<SqlServerImporter>();

// Incremental sync services
builder.Services.AddSingleton<IWatermarkStore>(sp =>
    new FileWatermarkStore(Path.Combine(icebergWarehousePath, ".watermarks")));
builder.Services.AddSingleton<IChangeDetectionStrategy, TimestampChangeDetection>();
builder.Services.AddSingleton<IncrementalSyncCoordinator>();

// Web-specific services
builder.Services.AddSingleton<TransferExecutionService>();
builder.Services.AddSingleton<TransferHistoryService>();
builder.Services.AddSingleton<DatabaseMetadataService>();
builder.Services.AddSingleton<ParquetFileService>(sp =>
    new ParquetFileService(sp.GetRequiredService<ILogger<ParquetFileService>>(), "./parquet-files"));
builder.Services.AddSingleton<TransferProfileService>(sp =>
    new TransferProfileService(sp.GetRequiredService<ILogger<TransferProfileService>>(), "./profiles"));
builder.Services.AddSingleton<IcebergTableService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

app.UseHttpsRedirection();

app.UseStaticFiles();
app.UseAntiforgery();

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();
