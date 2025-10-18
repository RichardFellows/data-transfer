namespace DataTransfer.Configuration.Models;

/// <summary>
/// Configuration for a specific environment (dev, staging, prod, etc.)
/// </summary>
public class EnvironmentConfiguration
{
    /// <summary>
    /// Environment name (e.g., "dev", "staging", "prod")
    /// </summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Environment-specific variables for token replacement
    /// </summary>
    public Dictionary<string, string> Variables { get; set; } = new();
}

/// <summary>
/// Container for multiple environment configurations
/// </summary>
public class EnvironmentSettings
{
    /// <summary>
    /// List of available environments
    /// </summary>
    public List<EnvironmentConfiguration> Environments { get; set; } = new();
}
