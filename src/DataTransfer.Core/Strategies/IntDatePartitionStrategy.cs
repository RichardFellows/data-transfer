namespace DataTransfer.Core.Strategies;

public class IntDatePartitionStrategy : PartitionStrategy
{
    private readonly string _columnName;
    private readonly string _format;

    public IntDatePartitionStrategy(string columnName, string format)
    {
        _columnName = columnName;
        _format = format;
    }

    public override string GetPartitionPath(DateTime date)
    {
        return $"year={date.Year:D4}/month={date.Month:D2}/day={date.Day:D2}";
    }

    public override string BuildWhereClause(DateTime startDate, DateTime endDate)
    {
        var startInt = int.Parse(startDate.ToString(_format));
        var endInt = int.Parse(endDate.ToString(_format));

        if (startInt == endInt)
        {
            return $"{_columnName} = {startInt}";
        }

        return $"{_columnName} >= {startInt} AND {_columnName} <= {endInt}";
    }
}
