using Avro;

namespace DataTransfer.Iceberg.Metadata;

/// <summary>
/// Wrapper around Apache.Avro.Schema that preserves Iceberg-specific attributes
/// CRITICAL: Apache.Avro strips field-id, element-id, key-id, value-id during serialization
/// This wrapper intercepts ToString() to return the original compliant schema
/// </summary>
public class AvroSchemaWrapper
{
    private readonly string _originalJsonSchema;
    private readonly Avro.Schema _innerSchema;

    /// <summary>
    /// Creates a wrapper that preserves the original Iceberg-compliant Avro schema
    /// </summary>
    /// <param name="compliantJsonSchema">Complete Iceberg Avro schema with field-id attributes</param>
    public AvroSchemaWrapper(string compliantJsonSchema)
    {
        _originalJsonSchema = compliantJsonSchema;
        _innerSchema = Avro.Schema.Parse(compliantJsonSchema);
    }

    /// <summary>
    /// Gets the inner parsed Avro schema (for compatibility with Avro APIs)
    /// </summary>
    public Avro.Schema InnerSchema => _innerSchema;

    /// <summary>
    /// CRITICAL METHOD: Returns original JSON instead of regenerated version
    /// This preserves Iceberg field-id, element-id, key-id, value-id attributes
    /// </summary>
    public override string ToString()
    {
        return _originalJsonSchema;
    }

    /// <summary>
    /// Equality comparison based on original JSON
    /// </summary>
    public override bool Equals(object? obj)
    {
        if (obj is AvroSchemaWrapper wrapper)
        {
            return _originalJsonSchema == wrapper._originalJsonSchema;
        }
        return false;
    }

    /// <summary>
    /// Hash code based on original JSON
    /// </summary>
    public override int GetHashCode()
    {
        return _originalJsonSchema.GetHashCode();
    }

    /// <summary>
    /// Implicit conversion to Avro.Schema for compatibility
    /// </summary>
    public static implicit operator Avro.Schema(AvroSchemaWrapper wrapper)
    {
        return wrapper._innerSchema;
    }
}
