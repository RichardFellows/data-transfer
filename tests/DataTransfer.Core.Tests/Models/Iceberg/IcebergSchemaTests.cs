using DataTransfer.Core.Models.Iceberg;
using System.Text.Json;
using Xunit;

namespace DataTransfer.Core.Tests.Models.Iceberg;

public class IcebergSchemaTests
{
    [Fact]
    public void Should_Initialize_With_Default_Type_Struct()
    {
        // Arrange & Act
        var schema = new IcebergSchema();

        // Assert
        Assert.Equal("struct", schema.Type);
    }

    [Fact]
    public void Should_Initialize_With_Empty_Fields_List()
    {
        // Arrange & Act
        var schema = new IcebergSchema();

        // Assert
        Assert.NotNull(schema.Fields);
        Assert.Empty(schema.Fields);
    }

    [Fact]
    public void Should_Generate_Sequential_Field_IDs()
    {
        // Arrange
        var schema = new IcebergSchema
        {
            SchemaId = 0
        };

        // Act - Add fields with sequential IDs
        schema.Fields.Add(new IcebergField
        {
            Id = 1,
            Name = "customer_id",
            Required = true,
            Type = "long"
        });

        schema.Fields.Add(new IcebergField
        {
            Id = 2,
            Name = "customer_name",
            Required = false,
            Type = "string"
        });

        schema.Fields.Add(new IcebergField
        {
            Id = 3,
            Name = "created_date",
            Required = true,
            Type = "date"
        });

        // Assert
        Assert.Equal(3, schema.Fields.Count);
        Assert.Equal(1, schema.Fields[0].Id);
        Assert.Equal(2, schema.Fields[1].Id);
        Assert.Equal(3, schema.Fields[2].Id);
    }

    [Fact]
    public void Should_Serialize_To_Valid_Iceberg_Json()
    {
        // Arrange
        var schema = new IcebergSchema
        {
            Type = "struct",
            SchemaId = 0,
            Fields = new List<IcebergField>
            {
                new IcebergField
                {
                    Id = 1,
                    Name = "customer_id",
                    Required = true,
                    Type = "long"
                },
                new IcebergField
                {
                    Id = 2,
                    Name = "customer_name",
                    Required = false,
                    Type = "string"
                }
            }
        };

        // Act
        var json = JsonSerializer.Serialize(schema, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });

        // Assert - Verify JSON structure contains expected properties
        Assert.Contains("\"type\":\"struct\"", json);
        Assert.Contains("\"schema-id\":0", json);  // Iceberg uses hyphenated property names
        Assert.Contains("\"fields\":", json);
        Assert.Contains("\"id\":1", json);
        Assert.Contains("\"name\":\"customer_id\"", json);
        Assert.Contains("\"required\":true", json);
        Assert.Contains("\"type\":\"long\"", json);

        // Verify we can deserialize back (Type will be JsonElement for object type)
        var deserialized = JsonSerializer.Deserialize<IcebergSchema>(json, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });

        Assert.NotNull(deserialized);
        Assert.Equal("struct", deserialized.Type);
        Assert.Equal(0, deserialized.SchemaId);
        Assert.Equal(2, deserialized.Fields.Count);
        Assert.Equal(1, deserialized.Fields[0].Id);
        Assert.Equal("customer_id", deserialized.Fields[0].Name);
        Assert.True(deserialized.Fields[0].Required);
    }

    [Fact]
    public void Should_Map_Nullability_Correctly()
    {
        // Arrange
        var requiredField = new IcebergField
        {
            Id = 1,
            Name = "id",
            Required = true,
            Type = "long"
        };

        var optionalField = new IcebergField
        {
            Id = 2,
            Name = "description",
            Required = false,
            Type = "string"
        };

        // Assert
        Assert.True(requiredField.Required, "Required field should have Required = true");
        Assert.False(optionalField.Required, "Optional field should have Required = false");
    }

    [Fact]
    public void IcebergField_Should_Support_Primitive_Type_As_String()
    {
        // Arrange
        var field = new IcebergField
        {
            Id = 1,
            Name = "test_field",
            Required = true,
            Type = "int"
        };

        // Assert
        Assert.Equal("int", field.Type);
    }

    [Fact]
    public void IcebergField_Should_Support_Complex_Type_As_Object()
    {
        // Arrange - Decimal type with precision and scale
        var decimalType = new { type = "decimal", precision = 18, scale = 2 };

        var field = new IcebergField
        {
            Id = 1,
            Name = "amount",
            Required = true,
            Type = decimalType
        };

        // Assert
        Assert.NotNull(field.Type);
        Assert.IsNotType<string>(field.Type);
        // Verify it's a complex object (anonymous type in this case)
        var typeProperty = field.Type.GetType().GetProperty("type");
        Assert.NotNull(typeProperty);
    }
}

public class IcebergTableMetadataTests
{
    [Fact]
    public void Should_Initialize_With_Format_Version_2()
    {
        // Arrange & Act
        var metadata = new IcebergTableMetadata();

        // Assert
        Assert.Equal(2, metadata.FormatVersion);
    }

    [Fact]
    public void Should_Initialize_With_Empty_Collections()
    {
        // Arrange & Act
        var metadata = new IcebergTableMetadata();

        // Assert
        Assert.NotNull(metadata.Schemas);
        Assert.Empty(metadata.Schemas);
        Assert.NotNull(metadata.PartitionSpecs);
        Assert.Empty(metadata.PartitionSpecs);
        Assert.NotNull(metadata.Snapshots);
        Assert.Empty(metadata.Snapshots);
    }

    [Fact]
    public void Should_Have_Default_Spec_Id_Zero()
    {
        // Arrange & Act
        var metadata = new IcebergTableMetadata();

        // Assert
        Assert.Equal(0, metadata.DefaultSpecId);
        Assert.Equal(0, metadata.LastPartitionId);
    }

    [Fact]
    public void Should_Allow_Null_Current_Snapshot_Id()
    {
        // Arrange & Act
        var metadata = new IcebergTableMetadata
        {
            CurrentSnapshotId = null
        };

        // Assert
        Assert.Null(metadata.CurrentSnapshotId);
    }

    [Fact]
    public void Should_Serialize_Complete_Table_Metadata()
    {
        // Arrange
        var schema = new IcebergSchema
        {
            Type = "struct",
            SchemaId = 0,
            Fields = new List<IcebergField>
            {
                new IcebergField { Id = 1, Name = "id", Required = true, Type = "long" }
            }
        };

        var snapshot = new IcebergSnapshot
        {
            SnapshotId = 12345678901234,
            TimestampMs = 1633072800000,
            ManifestList = "metadata/snap-12345678901234.avro"
        };

        var metadata = new IcebergTableMetadata
        {
            FormatVersion = 2,
            TableUuid = "550e8400-e29b-41d4-a716-446655440000",
            Location = "/warehouse/test_table",
            LastUpdatedMs = 1633072800000,
            LastColumnId = 1,
            Schemas = new List<IcebergSchema> { schema },
            CurrentSchemaId = 0,
            DefaultSpecId = 0,
            LastPartitionId = 0,
            Snapshots = new List<IcebergSnapshot> { snapshot },
            CurrentSnapshotId = 12345678901234
        };

        // Act
        var json = JsonSerializer.Serialize(metadata, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = true
        });

        // Assert
        Assert.Contains("\"formatVersion\": 2", json);
        Assert.Contains("\"tableUuid\"", json);
        Assert.Contains("\"location\"", json);
        Assert.Contains("\"schemas\"", json);
        Assert.Contains("\"snapshots\"", json);
    }
}

public class IcebergSnapshotTests
{
    [Fact]
    public void Should_Store_Snapshot_Id()
    {
        // Arrange & Act
        var snapshot = new IcebergSnapshot
        {
            SnapshotId = 12345678901234,
            TimestampMs = 1633072800000,
            ManifestList = "metadata/snap-12345678901234.avro"
        };

        // Assert
        Assert.Equal(12345678901234, snapshot.SnapshotId);
        Assert.Equal(1633072800000, snapshot.TimestampMs);
        Assert.Equal("metadata/snap-12345678901234.avro", snapshot.ManifestList);
    }

    [Fact]
    public void Should_Serialize_With_Correct_Property_Names()
    {
        // Arrange
        var snapshot = new IcebergSnapshot
        {
            SnapshotId = 123,
            TimestampMs = 456,
            ManifestList = "test.avro"
        };

        // Act
        var json = JsonSerializer.Serialize(snapshot, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });

        // Assert
        Assert.Contains("\"snapshotId\"", json);
        Assert.Contains("\"timestampMs\"", json);
        Assert.Contains("\"manifestList\"", json);
    }
}
