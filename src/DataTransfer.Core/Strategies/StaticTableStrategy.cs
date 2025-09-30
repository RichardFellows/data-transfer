namespace DataTransfer.Core.Strategies;

public class StaticTableStrategy : PartitionStrategy
{
    public override string GetPartitionPath(DateTime date)
    {
        return "static";
    }

    public override string BuildWhereClause(DateTime startDate, DateTime endDate)
    {
        return string.Empty;
    }
}
