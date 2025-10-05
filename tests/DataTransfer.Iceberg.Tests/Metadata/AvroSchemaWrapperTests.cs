using Avro;
using DataTransfer.Iceberg.Metadata;
using Xunit;

namespace DataTransfer.Iceberg.Tests.Metadata;

public class AvroSchemaWrapperTests
{
    [Fact]
    public void Should_Preserve_Field_Id_In_ToString()
    {
        // Arrange - Iceberg manifest entry schema with field-id attributes
        const string compliantSchema = @"{
            ""type"": ""record"",
            ""name"": ""manifest_entry"",
            ""fields"": [
                {
                    ""name"": ""status"",
                    ""type"": ""int"",
                    ""field-id"": 0
                },
                {
                    ""name"": ""snapshot_id"",
                    ""type"": [""null"", ""long""],
                    ""default"": null,
                    ""field-id"": 1
                }
            ]
        }";

        // Act
        var wrapper = new AvroSchemaWrapper(compliantSchema);
        var serialized = wrapper.ToString();

        // Assert
        Assert.Contains("\"field-id\": 0", serialized);
        Assert.Contains("\"field-id\": 1", serialized);
    }

    [Fact]
    public void Should_Preserve_Exact_Original_Schema_Format()
    {
        // Arrange - Schema with specific formatting and field-id
        const string compliantSchema = @"{
            ""type"": ""record"",
            ""name"": ""test_record"",
            ""fields"": [
                {
                    ""name"": ""test_field"",
                    ""type"": ""string"",
                    ""field-id"": 100
                }
            ]
        }";

        // Act
        var wrapper = new AvroSchemaWrapper(compliantSchema);
        var wrapperSerialized = wrapper.ToString();

        // Assert - Wrapper preserves EXACT original formatting and field-id
        Assert.Equal(compliantSchema, wrapperSerialized);
        Assert.Contains("\"field-id\": 100", wrapperSerialized);

        // Note: Apache.Avro 1.11.3 may preserve field-id, but the wrapper
        // guarantees preservation of the EXACT original JSON format, which is
        // critical for Iceberg metadata file generation
    }

    [Fact]
    public void Should_Preserve_Element_Id_For_List_Types()
    {
        // Arrange - Iceberg list schema with element-id
        const string compliantSchema = @"{
            ""type"": ""record"",
            ""name"": ""list_record"",
            ""fields"": [
                {
                    ""name"": ""items"",
                    ""type"": {
                        ""type"": ""array"",
                        ""items"": ""string"",
                        ""element-id"": 200
                    },
                    ""field-id"": 1
                }
            ]
        }";

        // Act
        var wrapper = new AvroSchemaWrapper(compliantSchema);
        var serialized = wrapper.ToString();

        // Assert
        Assert.Contains("\"element-id\": 200", serialized);
        Assert.Contains("\"field-id\": 1", serialized);
    }

    [Fact]
    public void Should_Preserve_Key_Id_And_Value_Id_For_Map_Types()
    {
        // Arrange - Iceberg map schema with key-id and value-id
        const string compliantSchema = @"{
            ""type"": ""record"",
            ""name"": ""map_record"",
            ""fields"": [
                {
                    ""name"": ""properties"",
                    ""type"": {
                        ""type"": ""map"",
                        ""values"": ""string"",
                        ""key-id"": 300,
                        ""value-id"": 301
                    },
                    ""field-id"": 1
                }
            ]
        }";

        // Act
        var wrapper = new AvroSchemaWrapper(compliantSchema);
        var serialized = wrapper.ToString();

        // Assert
        Assert.Contains("\"key-id\": 300", serialized);
        Assert.Contains("\"value-id\": 301", serialized);
        Assert.Contains("\"field-id\": 1", serialized);
    }

    [Fact]
    public void Should_Be_Implicitly_Convertible_To_Avro_Schema()
    {
        // Arrange
        const string compliantSchema = @"{
            ""type"": ""record"",
            ""name"": ""test"",
            ""fields"": [
                {
                    ""name"": ""id"",
                    ""type"": ""int"",
                    ""field-id"": 1
                }
            ]
        }";

        // Act
        var wrapper = new AvroSchemaWrapper(compliantSchema);
        Avro.Schema innerSchema = wrapper;  // Implicit conversion

        // Assert
        Assert.NotNull(innerSchema);
        Assert.Equal("test", innerSchema.Name);
    }

    [Fact]
    public void Should_Parse_Valid_Avro_Schema()
    {
        // Arrange
        const string compliantSchema = @"{
            ""type"": ""record"",
            ""name"": ""valid_record"",
            ""fields"": [
                {
                    ""name"": ""value"",
                    ""type"": ""long"",
                    ""field-id"": 1
                }
            ]
        }";

        // Act & Assert - Should not throw
        var wrapper = new AvroSchemaWrapper(compliantSchema);
        Assert.NotNull(wrapper);
    }

    [Fact]
    public void Should_Throw_On_Invalid_Json_Schema()
    {
        // Arrange - Malformed JSON
        const string invalidSchema = @"{
            ""type"": ""record"",
            malformed syntax here
        }";

        // Act & Assert - Should throw during JSON parsing
        Assert.ThrowsAny<Exception>(() => new AvroSchemaWrapper(invalidSchema));
    }
}
