using DataTransfer.Configuration;
using DataTransfer.Configuration.Models;
using Xunit;

namespace DataTransfer.Configuration.Tests;

public class EnvironmentConfigurationTests
{
    [Fact]
    public void Should_Store_Environment_Variables()
    {
        // Arrange
        var envConfig = new EnvironmentConfiguration
        {
            Name = "dev",
            Variables = new Dictionary<string, string>
            {
                { "ConnectionString", "Server=dev-server;Database=DevDB;" },
                { "OutputPath", "/data/dev" }
            }
        };

        // Act & Assert
        Assert.Equal("dev", envConfig.Name);
        Assert.Equal(2, envConfig.Variables.Count);
        Assert.Equal("Server=dev-server;Database=DevDB;", envConfig.Variables["ConnectionString"]);
        Assert.Equal("/data/dev", envConfig.Variables["OutputPath"]);
    }

    [Fact]
    public void Should_Support_Multiple_Environments()
    {
        // Arrange
        var settings = new EnvironmentSettings
        {
            Environments = new List<EnvironmentConfiguration>
            {
                new() { Name = "dev", Variables = new Dictionary<string, string> { { "Server", "dev-server" } } },
                new() { Name = "staging", Variables = new Dictionary<string, string> { { "Server", "staging-server" } } },
                new() { Name = "prod", Variables = new Dictionary<string, string> { { "Server", "prod-server" } } }
            }
        };

        // Act & Assert
        Assert.Equal(3, settings.Environments.Count);
        Assert.Contains(settings.Environments, e => e.Name == "dev");
        Assert.Contains(settings.Environments, e => e.Name == "staging");
        Assert.Contains(settings.Environments, e => e.Name == "prod");
    }
}

public class EnvironmentManagerTests
{
    [Fact]
    public void Should_Get_Environment_By_Name()
    {
        // Arrange
        var settings = new EnvironmentSettings
        {
            Environments = new List<EnvironmentConfiguration>
            {
                new() { Name = "dev", Variables = new Dictionary<string, string> { { "Server", "dev-server" } } },
                new() { Name = "prod", Variables = new Dictionary<string, string> { { "Server", "prod-server" } } }
            }
        };
        var manager = new EnvironmentManager(settings);

        // Act
        var devEnv = manager.GetEnvironment("dev");

        // Assert
        Assert.NotNull(devEnv);
        Assert.Equal("dev", devEnv.Name);
        Assert.Equal("dev-server", devEnv.Variables["Server"]);
    }

    [Fact]
    public void Should_Throw_When_Environment_Not_Found()
    {
        // Arrange
        var settings = new EnvironmentSettings
        {
            Environments = new List<EnvironmentConfiguration>
            {
                new() { Name = "dev", Variables = new Dictionary<string, string>() }
            }
        };
        var manager = new EnvironmentManager(settings);

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => manager.GetEnvironment("prod"));
        Assert.Contains("Environment 'prod' not found", exception.Message);
    }

    [Fact]
    public void Should_Replace_Simple_Token()
    {
        // Arrange
        var settings = new EnvironmentSettings
        {
            Environments = new List<EnvironmentConfiguration>
            {
                new()
                {
                    Name = "dev",
                    Variables = new Dictionary<string, string>
                    {
                        { "ConnectionString", "Server=dev-server;Database=DevDB;" }
                    }
                }
            }
        };
        var manager = new EnvironmentManager(settings);
        var env = manager.GetEnvironment("dev");

        // Act
        var result = manager.ReplaceTokens("${env:ConnectionString}", env);

        // Assert
        Assert.Equal("Server=dev-server;Database=DevDB;", result);
    }

    [Fact]
    public void Should_Replace_Multiple_Tokens()
    {
        // Arrange
        var settings = new EnvironmentSettings
        {
            Environments = new List<EnvironmentConfiguration>
            {
                new()
                {
                    Name = "dev",
                    Variables = new Dictionary<string, string>
                    {
                        { "Server", "dev-server" },
                        { "Database", "DevDB" }
                    }
                }
            }
        };
        var manager = new EnvironmentManager(settings);
        var env = manager.GetEnvironment("dev");

        // Act
        var result = manager.ReplaceTokens("Server=${env:Server};Database=${env:Database};", env);

        // Assert
        Assert.Equal("Server=dev-server;Database=DevDB;", result);
    }

    [Fact]
    public void Should_Leave_Text_Unchanged_When_No_Tokens()
    {
        // Arrange
        var settings = new EnvironmentSettings
        {
            Environments = new List<EnvironmentConfiguration>
            {
                new() { Name = "dev", Variables = new Dictionary<string, string>() }
            }
        };
        var manager = new EnvironmentManager(settings);
        var env = manager.GetEnvironment("dev");

        // Act
        var result = manager.ReplaceTokens("Server=localhost;Database=TestDB;", env);

        // Assert
        Assert.Equal("Server=localhost;Database=TestDB;", result);
    }

    [Fact]
    public void Should_Throw_When_Token_Variable_Not_Found()
    {
        // Arrange
        var settings = new EnvironmentSettings
        {
            Environments = new List<EnvironmentConfiguration>
            {
                new()
                {
                    Name = "dev",
                    Variables = new Dictionary<string, string>
                    {
                        { "Server", "dev-server" }
                    }
                }
            }
        };
        var manager = new EnvironmentManager(settings);
        var env = manager.GetEnvironment("dev");

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(
            () => manager.ReplaceTokens("${env:MissingVariable}", env));
        Assert.Contains("Variable 'MissingVariable' not found in environment 'dev'", exception.Message);
    }

    [Fact]
    public void Should_Handle_Case_Sensitive_Variable_Names()
    {
        // Arrange
        var settings = new EnvironmentSettings
        {
            Environments = new List<EnvironmentConfiguration>
            {
                new()
                {
                    Name = "dev",
                    Variables = new Dictionary<string, string>
                    {
                        { "ConnectionString", "value1" },
                        { "connectionstring", "value2" }
                    }
                }
            }
        };
        var manager = new EnvironmentManager(settings);
        var env = manager.GetEnvironment("dev");

        // Act
        var result1 = manager.ReplaceTokens("${env:ConnectionString}", env);
        var result2 = manager.ReplaceTokens("${env:connectionstring}", env);

        // Assert
        Assert.Equal("value1", result1);
        Assert.Equal("value2", result2);
    }
}
