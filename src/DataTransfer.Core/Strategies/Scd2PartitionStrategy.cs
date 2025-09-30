namespace DataTransfer.Core.Strategies;

public class Scd2PartitionStrategy : PartitionStrategy
{
    private readonly string _effectiveDateColumn;
    private readonly string _expirationDateColumn;

    public Scd2PartitionStrategy(string effectiveDateColumn, string expirationDateColumn)
    {
        _effectiveDateColumn = effectiveDateColumn;
        _expirationDateColumn = expirationDateColumn;
    }

    public override string GetPartitionPath(DateTime date)
    {
        return $"year={date.Year:D4}/month={date.Month:D2}/day={date.Day:D2}";
    }

    public override string BuildWhereClause(DateTime startDate, DateTime endDate)
    {
        // For SCD2 tables, we want records that were effective during the date range
        // This means: EffectiveDate <= endDate AND (ExpirationDate > startDate OR ExpirationDate IS NULL)
        return $"{_effectiveDateColumn} >= '{startDate:yyyy-MM-dd}' " +
               $"AND {_effectiveDateColumn} <= '{endDate:yyyy-MM-dd}' " +
               $"AND ({_expirationDateColumn} > '{endDate:yyyy-MM-dd}' OR {_expirationDateColumn} IS NULL)";
    }
}
