namespace DataTransfer.Core.Strategies;

public abstract class PartitionStrategy
{
    public abstract string GetPartitionPath(DateTime date);

    public abstract string BuildWhereClause(DateTime startDate, DateTime endDate);
}
