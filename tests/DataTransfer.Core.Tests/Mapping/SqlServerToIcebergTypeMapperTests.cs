using DataTransfer.Core.Mapping;
using System.Data;
using Xunit;

namespace DataTransfer.Core.Tests.Mapping;

public class SqlServerToIcebergTypeMapperTests
{
    [Theory]
    [InlineData(SqlDbType.BigInt, "long")]
    [InlineData(SqlDbType.Int, "int")]
    [InlineData(SqlDbType.SmallInt, "int")]
    [InlineData(SqlDbType.TinyInt, "int")]
    public void Should_Map_Integer_Types_Correctly(SqlDbType sqlType, string expectedIcebergType)
    {
        // Act
        var result = SqlServerToIcebergTypeMapper.MapType(sqlType);

        // Assert
        Assert.Equal(expectedIcebergType, result);
    }

    [Fact]
    public void Should_Map_Bit_To_Boolean()
    {
        // Act
        var result = SqlServerToIcebergTypeMapper.MapType(SqlDbType.Bit);

        // Assert
        Assert.Equal("boolean", result);
    }

    [Theory]
    [InlineData(SqlDbType.Float, "double")]
    [InlineData(SqlDbType.Real, "float")]
    public void Should_Map_Floating_Point_Types_Correctly(SqlDbType sqlType, string expectedIcebergType)
    {
        // Act
        var result = SqlServerToIcebergTypeMapper.MapType(sqlType);

        // Assert
        Assert.Equal(expectedIcebergType, result);
    }

    [Fact]
    public void Should_Map_Decimal_With_Precision_And_Scale()
    {
        // Act
        var result = SqlServerToIcebergTypeMapper.MapType(SqlDbType.Decimal, 18, 2);

        // Assert
        Assert.NotNull(result);

        // Result should be an anonymous object with type, precision, and scale
        var resultType = result.GetType();
        var typeProperty = resultType.GetProperty("type")?.GetValue(result);
        var precisionProperty = resultType.GetProperty("precision")?.GetValue(result);
        var scaleProperty = resultType.GetProperty("scale")?.GetValue(result);

        Assert.Equal("decimal", typeProperty);
        Assert.Equal(18, precisionProperty);
        Assert.Equal(2, scaleProperty);
    }

    [Fact]
    public void Should_Map_Decimal_With_Default_Precision_When_Not_Specified()
    {
        // Act
        var result = SqlServerToIcebergTypeMapper.MapType(SqlDbType.Decimal);

        // Assert
        var resultType = result.GetType();
        var typeProperty = resultType.GetProperty("type")?.GetValue(result);
        var precisionProperty = resultType.GetProperty("precision")?.GetValue(result);
        var scaleProperty = resultType.GetProperty("scale")?.GetValue(result);

        Assert.Equal("decimal", typeProperty);
        Assert.Equal(18, precisionProperty);  // Default precision
        Assert.Equal(0, scaleProperty);       // Default scale
    }

    [Theory]
    [InlineData(SqlDbType.Money)]
    [InlineData(SqlDbType.SmallMoney)]
    public void Should_Map_Money_Types_To_Decimal(SqlDbType sqlType)
    {
        // Act
        var result = SqlServerToIcebergTypeMapper.MapType(sqlType);

        // Assert
        var resultType = result.GetType();
        var typeProperty = resultType.GetProperty("type")?.GetValue(result);
        Assert.Equal("decimal", typeProperty);
    }

    [Theory]
    [InlineData(SqlDbType.Date, "date")]
    [InlineData(SqlDbType.DateTime, "timestamp")]
    [InlineData(SqlDbType.DateTime2, "timestamp")]
    [InlineData(SqlDbType.SmallDateTime, "timestamp")]
    public void Should_Map_Date_And_Time_Types_Correctly(SqlDbType sqlType, string expectedIcebergType)
    {
        // Act
        var result = SqlServerToIcebergTypeMapper.MapType(sqlType);

        // Assert
        Assert.Equal(expectedIcebergType, result);
    }

    [Fact]
    public void Should_Map_DateTimeOffset_To_TimestampTz()
    {
        // Act
        var result = SqlServerToIcebergTypeMapper.MapType(SqlDbType.DateTimeOffset);

        // Assert
        Assert.Equal("timestamptz", result);
    }

    [Theory]
    [InlineData(SqlDbType.Char)]
    [InlineData(SqlDbType.NChar)]
    [InlineData(SqlDbType.VarChar)]
    [InlineData(SqlDbType.NVarChar)]
    [InlineData(SqlDbType.Text)]
    [InlineData(SqlDbType.NText)]
    public void Should_Map_String_Types_To_String(SqlDbType sqlType)
    {
        // Act
        var result = SqlServerToIcebergTypeMapper.MapType(sqlType);

        // Assert
        Assert.Equal("string", result);
    }

    [Theory]
    [InlineData(SqlDbType.Binary)]
    [InlineData(SqlDbType.VarBinary)]
    [InlineData(SqlDbType.Image)]
    public void Should_Map_Binary_Types_To_Binary(SqlDbType sqlType)
    {
        // Act
        var result = SqlServerToIcebergTypeMapper.MapType(sqlType);

        // Assert
        Assert.Equal("binary", result);
    }

    [Fact]
    public void Should_Map_UniqueIdentifier_To_Uuid()
    {
        // Act
        var result = SqlServerToIcebergTypeMapper.MapType(SqlDbType.UniqueIdentifier);

        // Assert
        Assert.Equal("uuid", result);
    }

    [Fact]
    public void Should_Throw_On_Unsupported_Type()
    {
        // Act & Assert
        var exception = Assert.Throws<NotSupportedException>(() =>
            SqlServerToIcebergTypeMapper.MapType(SqlDbType.Timestamp));

        Assert.Contains("Timestamp", exception.Message);
        Assert.Contains("not supported", exception.Message);
    }

    [Fact]
    public void Should_Throw_On_Variant_Type()
    {
        // Act & Assert
        var exception = Assert.Throws<NotSupportedException>(() =>
            SqlServerToIcebergTypeMapper.MapType(SqlDbType.Variant));

        Assert.Contains("Variant", exception.Message);
    }

    [Fact]
    public void Should_Throw_On_Xml_Type()
    {
        // Act & Assert
        var exception = Assert.Throws<NotSupportedException>(() =>
            SqlServerToIcebergTypeMapper.MapType(SqlDbType.Xml));

        Assert.Contains("Xml", exception.Message);
    }
}
