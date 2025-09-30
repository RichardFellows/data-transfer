using System.Text;
using DataTransfer.Core.Models;
using DataTransfer.Core.Strategies;

namespace DataTransfer.SqlServer;

public class SqlQueryBuilder
{
    public string BuildSelectQuery(
        TableConfiguration tableConfig,
        PartitionStrategy? partitionStrategy,
        DateTime? startDate = null,
        DateTime? endDate = null)
    {
        var sb = new StringBuilder();
        sb.Append("SELECT * FROM ");
        sb.Append(FormatTableName(tableConfig.Source));

        AppendWhereClause(sb, partitionStrategy, startDate, endDate);

        return sb.ToString();
    }

    public string BuildCountQuery(
        TableConfiguration tableConfig,
        PartitionStrategy? partitionStrategy,
        DateTime? startDate = null,
        DateTime? endDate = null)
    {
        var sb = new StringBuilder();
        sb.Append("SELECT COUNT(*) FROM ");
        sb.Append(FormatTableName(tableConfig.Source));

        AppendWhereClause(sb, partitionStrategy, startDate, endDate);

        return sb.ToString();
    }

    public string BuildInsertQuery(TableConfiguration tableConfig, string[] columns)
    {
        var sb = new StringBuilder();
        sb.Append("INSERT INTO ");
        sb.Append(FormatTableName(tableConfig.Destination));
        sb.Append(" (");

        for (int i = 0; i < columns.Length; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"[{columns[i]}]");
        }

        sb.Append(") VALUES (");

        for (int i = 0; i < columns.Length; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"@{columns[i]}");
        }

        sb.Append(")");

        return sb.ToString();
    }

    public string BuildTruncateQuery(TableConfiguration tableConfig)
    {
        return $"TRUNCATE TABLE {FormatTableName(tableConfig.Destination)}";
    }

    private string FormatTableName(TableIdentifier identifier)
    {
        return $"[{identifier.Database}].[{identifier.Schema}].[{identifier.Table}]";
    }

    private void AppendWhereClause(
        StringBuilder sb,
        PartitionStrategy? partitionStrategy,
        DateTime? startDate,
        DateTime? endDate)
    {
        if (partitionStrategy == null || startDate == null || endDate == null)
        {
            return;
        }

        var whereClause = partitionStrategy.BuildWhereClause(startDate.Value, endDate.Value);

        if (!string.IsNullOrEmpty(whereClause))
        {
            sb.Append(" WHERE ");
            sb.Append(whereClause);
        }
    }
}
