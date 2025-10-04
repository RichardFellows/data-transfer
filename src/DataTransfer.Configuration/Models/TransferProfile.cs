using DataTransfer.Core.Models;

namespace DataTransfer.Configuration.Models;

/// <summary>
/// Represents a saved transfer configuration profile/template
/// Stored as JSON files for portability and version control
/// </summary>
public class TransferProfile
{
    /// <summary>
    /// Unique identifier for the profile (GUID)
    /// </summary>
    public string ProfileId { get; set; } = Guid.NewGuid().ToString();

    /// <summary>
    /// Human-readable name for the profile
    /// </summary>
    public string ProfileName { get; set; } = string.Empty;

    /// <summary>
    /// Optional description of what this profile does
    /// </summary>
    public string? Description { get; set; }

    /// <summary>
    /// The complete transfer configuration
    /// </summary>
    public TransferConfiguration Configuration { get; set; } = new();

    /// <summary>
    /// Tags for organization and filtering
    /// </summary>
    public List<string> Tags { get; set; } = new();

    /// <summary>
    /// User who created this profile
    /// </summary>
    public string CreatedBy { get; set; } = "system";

    /// <summary>
    /// When this profile was created
    /// </summary>
    public DateTime CreatedDate { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// User who last modified this profile
    /// </summary>
    public string? ModifiedBy { get; set; }

    /// <summary>
    /// When this profile was last modified
    /// </summary>
    public DateTime? ModifiedDate { get; set; }

    /// <summary>
    /// Whether this profile is active (soft delete)
    /// </summary>
    public bool IsActive { get; set; } = true;
}

/// <summary>
/// Container for all profiles stored in profiles.json
/// </summary>
public class ProfilesCollection
{
    /// <summary>
    /// All saved profiles
    /// </summary>
    public List<TransferProfile> Profiles { get; set; } = new();
}
