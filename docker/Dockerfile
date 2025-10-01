# Build stage
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Copy solution and project files
COPY DataTransfer.sln ./
COPY src/DataTransfer.Core/DataTransfer.Core.csproj src/DataTransfer.Core/
COPY src/DataTransfer.Configuration/DataTransfer.Configuration.csproj src/DataTransfer.Configuration/
COPY src/DataTransfer.SqlServer/DataTransfer.SqlServer.csproj src/DataTransfer.SqlServer/
COPY src/DataTransfer.Parquet/DataTransfer.Parquet.csproj src/DataTransfer.Parquet/
COPY src/DataTransfer.Pipeline/DataTransfer.Pipeline.csproj src/DataTransfer.Pipeline/
COPY src/DataTransfer.Console/DataTransfer.Console.csproj src/DataTransfer.Console/

# Restore dependencies
RUN dotnet restore

# Copy source code
COPY src/ src/

# Build and publish
RUN dotnet publish src/DataTransfer.Console/DataTransfer.Console.csproj -c Release -o /app/publish

# Runtime stage
FROM mcr.microsoft.com/dotnet/runtime:8.0 AS runtime
WORKDIR /app

# Create non-root user
RUN useradd -m -u 1001 datatransfer

# Copy published application
COPY --from=build /app/publish .

# Create directories for volumes
RUN mkdir -p /config /parquet-output /logs && chown -R datatransfer:datatransfer /config /parquet-output /logs /app

# Define volumes
VOLUME ["/config", "/parquet-output", "/logs"]

# Switch to non-root user
USER datatransfer

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD test -f /app/DataTransfer.Console.dll || exit 1

# Environment variables
ENV DOTNET_EnableDiagnostics=0
ENV ASPNETCORE_ENVIRONMENT=Production

ENTRYPOINT ["dotnet", "DataTransfer.Console.dll"]
