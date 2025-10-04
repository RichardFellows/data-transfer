using DataTransfer.Core.Interfaces;
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

// Web-specific services
builder.Services.AddSingleton<TransferExecutionService>();
builder.Services.AddSingleton<TransferHistoryService>();
builder.Services.AddSingleton<DatabaseMetadataService>();
builder.Services.AddSingleton<ParquetFileService>(sp =>
    new ParquetFileService(sp.GetRequiredService<ILogger<ParquetFileService>>(), "./parquet-files"));
builder.Services.AddSingleton<TransferProfileService>(sp =>
    new TransferProfileService(sp.GetRequiredService<ILogger<TransferProfileService>>(), "./profiles"));

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
