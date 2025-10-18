using System.Text.RegularExpressions;
using DataTransfer.Configuration.Models;

namespace DataTransfer.Configuration;

/// <summary>
/// Manages environment configurations and token replacement
/// </summary>
public class EnvironmentManager
{
    private readonly EnvironmentSettings _settings;
    private static readonly Regex TokenPattern = new(@"\$\{env:([^}]+)\}", RegexOptions.Compiled);

    public EnvironmentManager(EnvironmentSettings settings)
    {
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
    }

    /// <summary>
    /// Gets an environment configuration by name
    /// </summary>
    /// <param name="environmentName">Name of the environment to retrieve</param>
    /// <returns>The environment configuration</returns>
    /// <exception cref="InvalidOperationException">Thrown when environment is not found</exception>
    public EnvironmentConfiguration GetEnvironment(string environmentName)
    {
        var environment = _settings.Environments
            .FirstOrDefault(e => e.Name.Equals(environmentName, StringComparison.OrdinalIgnoreCase));

        if (environment == null)
        {
            throw new InvalidOperationException(
                $"Environment '{environmentName}' not found. Available environments: {string.Join(", ", _settings.Environments.Select(e => e.Name))}");
        }

        return environment;
    }

    /// <summary>
    /// Replaces ${env:VariableName} tokens in the input string with values from the environment
    /// </summary>
    /// <param name="input">String containing tokens to replace</param>
    /// <param name="environment">Environment configuration with variable values</param>
    /// <returns>String with tokens replaced by environment variable values</returns>
    /// <exception cref="InvalidOperationException">Thrown when a referenced variable is not found</exception>
    public string ReplaceTokens(string input, EnvironmentConfiguration environment)
    {
        if (string.IsNullOrEmpty(input))
        {
            return input;
        }

        return TokenPattern.Replace(input, match =>
        {
            var variableName = match.Groups[1].Value;

            if (!environment.Variables.TryGetValue(variableName, out var value))
            {
                throw new InvalidOperationException(
                    $"Variable '{variableName}' not found in environment '{environment.Name}'. " +
                    $"Available variables: {string.Join(", ", environment.Variables.Keys)}");
            }

            return value;
        });
    }
}
