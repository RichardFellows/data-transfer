using DataTransfer.Core.Models;

namespace DataTransfer.Core.Strategies;

public static class PartitionStrategyFactory
{
    public static PartitionStrategy Create(PartitioningConfiguration config)
    {
        return config.Type switch
        {
            PartitionType.Date => CreateDateStrategy(config),
            PartitionType.IntDate => CreateIntDateStrategy(config),
            PartitionType.Scd2 => CreateScd2Strategy(config),
            PartitionType.Static => new StaticTableStrategy(),
            _ => throw new ArgumentException($"Unsupported partition type: {config.Type}")
        };
    }

    private static DatePartitionStrategy CreateDateStrategy(PartitioningConfiguration config)
    {
        if (string.IsNullOrWhiteSpace(config.Column))
        {
            throw new ArgumentException("Column name is required for Date partition type");
        }

        return new DatePartitionStrategy(config.Column);
    }

    private static IntDatePartitionStrategy CreateIntDateStrategy(PartitioningConfiguration config)
    {
        if (string.IsNullOrWhiteSpace(config.Column))
        {
            throw new ArgumentException("Column name is required for IntDate partition type");
        }

        var format = config.Format ?? "yyyyMMdd";
        return new IntDatePartitionStrategy(config.Column, format);
    }

    private static Scd2PartitionStrategy CreateScd2Strategy(PartitioningConfiguration config)
    {
        if (string.IsNullOrWhiteSpace(config.Column))
        {
            throw new ArgumentException("Column name is required for Scd2 partition type");
        }

        // For Scd2, we use Column for EffectiveDate and Format for ExpirationDate
        var expirationColumn = config.Format ?? "ExpirationDate";
        return new Scd2PartitionStrategy(config.Column, expirationColumn);
    }
}
