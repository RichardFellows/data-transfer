namespace DataTransfer.Core.Strategies;

public class DatePartitionStrategy : PartitionStrategy
{
    private readonly string _columnName;

    public DatePartitionStrategy(string columnName)
    {
        _columnName = columnName;
    }

    public override string GetPartitionPath(DateTime date)
    {
        return $"year={date.Year:D4}/month={date.Month:D2}/day={date.Day:D2}";
    }

    public override string BuildWhereClause(DateTime startDate, DateTime endDate)
    {
        if (startDate.Date == endDate.Date)
        {
            return $"{_columnName} >= '{startDate:yyyy-MM-dd}' AND {_columnName} < '{startDate.AddDays(1):yyyy-MM-dd}'";
        }

        return $"{_columnName} >= '{startDate:yyyy-MM-dd}' AND {_columnName} <= '{endDate:yyyy-MM-dd}'";
    }
}
