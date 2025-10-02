using BenchmarkDotNet.Attributes;
using DataTransfer.Core.Models;
using DataTransfer.Core.Strategies;
using DataTransfer.SqlServer;

namespace DataTransfer.Benchmarks;

[MemoryDiagnoser]
public class QueryBuildingBenchmarks
{
    private SqlQueryBuilder? _queryBuilder;
    private TableConfiguration? _staticConfig;
    private TableConfiguration? _dateConfig;
    private TableConfiguration? _intDateConfig;
    private TableConfiguration? _scd2Config;
    private DateTime _startDate;
    private DateTime _endDate;

    [GlobalSetup]
    public void Setup()
    {
        _queryBuilder = new SqlQueryBuilder();
        _startDate = new DateTime(2024, 1, 1);
        _endDate = new DateTime(2024, 12, 31);

        _staticConfig = new TableConfiguration
        {
            Source = new TableIdentifier { Database = "TestDB", Schema = "dbo", Table = "StaticTable" },
            Destination = new TableIdentifier { Database = "TestDB", Schema = "dbo", Table = "StaticTable" },
            Partitioning = new PartitioningConfiguration { Type = PartitionType.Static },
            ExtractSettings = new ExtractSettings
            {
                DateRange = new DateRange { StartDate = _startDate, EndDate = _endDate }
            }
        };

        _dateConfig = new TableConfiguration
        {
            Source = new TableIdentifier { Database = "TestDB", Schema = "dbo", Table = "Orders" },
            Destination = new TableIdentifier { Database = "TestDB", Schema = "dbo", Table = "Orders" },
            Partitioning = new PartitioningConfiguration
            {
                Type = PartitionType.Date,
                Column = "OrderDate"
            },
            ExtractSettings = new ExtractSettings
            {
                DateRange = new DateRange { StartDate = _startDate, EndDate = _endDate }
            }
        };

        _intDateConfig = new TableConfiguration
        {
            Source = new TableIdentifier { Database = "TestDB", Schema = "dbo", Table = "Transactions" },
            Destination = new TableIdentifier { Database = "TestDB", Schema = "dbo", Table = "Transactions" },
            Partitioning = new PartitioningConfiguration
            {
                Type = PartitionType.IntDate,
                Column = "DateKey"
            },
            ExtractSettings = new ExtractSettings
            {
                DateRange = new DateRange { StartDate = _startDate, EndDate = _endDate }
            }
        };

        _scd2Config = new TableConfiguration
        {
            Source = new TableIdentifier { Database = "TestDB", Schema = "dbo", Table = "Customers" },
            Destination = new TableIdentifier { Database = "TestDB", Schema = "dbo", Table = "Customers" },
            Partitioning = new PartitioningConfiguration
            {
                Type = PartitionType.Scd2,
                Column = "EffectiveDate",
                Format = "ExpiryDate"
            },
            ExtractSettings = new ExtractSettings
            {
                DateRange = new DateRange { StartDate = _startDate, EndDate = _endDate }
            }
        };
    }

    [Benchmark]
    public string BuildQuery_Static()
    {
        var strategy = PartitionStrategyFactory.Create(_staticConfig!.Partitioning);
        return _queryBuilder!.BuildSelectQuery(_staticConfig!, strategy, _startDate, _endDate);
    }

    [Benchmark]
    public string BuildQuery_DatePartitioned()
    {
        var strategy = PartitionStrategyFactory.Create(_dateConfig!.Partitioning);
        return _queryBuilder!.BuildSelectQuery(_dateConfig!, strategy, _startDate, _endDate);
    }

    [Benchmark]
    public string BuildQuery_IntDatePartitioned()
    {
        var strategy = PartitionStrategyFactory.Create(_intDateConfig!.Partitioning);
        return _queryBuilder!.BuildSelectQuery(_intDateConfig!, strategy, _startDate, _endDate);
    }

    [Benchmark]
    public string BuildQuery_Scd2()
    {
        var strategy = PartitionStrategyFactory.Create(_scd2Config!.Partitioning);
        return _queryBuilder!.BuildSelectQuery(_scd2Config!, strategy, _startDate, _endDate);
    }
}
