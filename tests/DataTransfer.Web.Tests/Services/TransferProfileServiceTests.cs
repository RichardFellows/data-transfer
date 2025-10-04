using Microsoft.Extensions.Logging.Abstractions;
using DataTransfer.Configuration.Services;
using DataTransfer.Configuration.Models;
using DataTransfer.Core.Models;
using Xunit;

namespace DataTransfer.Web.Tests.Services;

/// <summary>
/// Unit tests for TransferProfileService (file-based storage)
/// Following TDD: RED → GREEN → REFACTOR
/// </summary>
public class TransferProfileServiceTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly TransferProfileService _service;

    public TransferProfileServiceTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"profiles_test_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDirectory);
        _service = new TransferProfileService(NullLogger<TransferProfileService>.Instance, _testDirectory);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDirectory))
        {
            Directory.Delete(_testDirectory, true);
        }
    }

    [Fact]
    public async Task SaveProfileAsync_ValidProfile_ReturnsSavedProfile()
    {
        // Arrange
        var config = CreateTestTransferConfiguration();
        var profileName = "Test Profile";
        var description = "Test Description";
        var createdBy = "testuser";

        // Act
        var profile = await _service.SaveProfileAsync(profileName, description, config, createdBy);

        // Assert
        Assert.NotNull(profile);
        Assert.NotEmpty(profile.ProfileId);
        Assert.Equal(profileName, profile.ProfileName);
        Assert.Equal(description, profile.Description);
        Assert.Equal(createdBy, profile.CreatedBy);
        Assert.True(profile.CreatedDate > DateTime.MinValue);
        Assert.True(profile.IsActive);
        Assert.Equal(TransferType.SqlToParquet, profile.Configuration.TransferType);
    }

    [Fact]
    public async Task SaveProfileAsync_CreatesProfilesJsonFile()
    {
        // Arrange
        var config = CreateTestTransferConfiguration();

        // Act
        await _service.SaveProfileAsync("Test", null, config, "user");

        // Assert
        var profilesFile = Path.Combine(_testDirectory, "profiles.json");
        Assert.True(File.Exists(profilesFile));
    }

    [Fact]
    public async Task SaveProfileAsync_DuplicateName_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = CreateTestTransferConfiguration();
        await _service.SaveProfileAsync("Duplicate", null, config, "user");

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await _service.SaveProfileAsync("Duplicate", null, config, "user");
        });
    }

    [Fact]
    public async Task GetProfileAsync_ExistingProfile_ReturnsProfile()
    {
        // Arrange
        var config = CreateTestTransferConfiguration();
        var saved = await _service.SaveProfileAsync("Test Profile", "Description", config, "user");

        // Act
        var retrieved = await _service.GetProfileAsync(saved.ProfileId);

        // Assert
        Assert.NotNull(retrieved);
        Assert.Equal(saved.ProfileId, retrieved!.ProfileId);
        Assert.Equal(saved.ProfileName, retrieved.ProfileName);
        Assert.Equal(saved.Description, retrieved.Description);
    }

    [Fact]
    public async Task GetProfileAsync_NonExistentProfile_ReturnsNull()
    {
        // Act
        var result = await _service.GetProfileAsync("non-existent-id");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task GetAllProfilesAsync_NoProfiles_ReturnsEmptyList()
    {
        // Act
        var profiles = await _service.GetAllProfilesAsync();

        // Assert
        Assert.Empty(profiles);
    }

    [Fact]
    public async Task GetAllProfilesAsync_MultipleProfiles_ReturnsAll()
    {
        // Arrange
        var config = CreateTestTransferConfiguration();
        await _service.SaveProfileAsync("Profile 1", null, config, "user");
        await _service.SaveProfileAsync("Profile 2", null, config, "user");
        await _service.SaveProfileAsync("Profile 3", null, config, "user");

        // Act
        var profiles = await _service.GetAllProfilesAsync();

        // Assert
        Assert.Equal(3, profiles.Count);
        Assert.Contains(profiles, p => p.ProfileName == "Profile 1");
        Assert.Contains(profiles, p => p.ProfileName == "Profile 2");
        Assert.Contains(profiles, p => p.ProfileName == "Profile 3");
    }

    [Fact]
    public async Task GetAllProfilesAsync_ActiveOnlyTrue_ReturnsOnlyActiveProfiles()
    {
        // Arrange
        var config = CreateTestTransferConfiguration();
        var profile1 = await _service.SaveProfileAsync("Active Profile", null, config, "user");
        var profile2 = await _service.SaveProfileAsync("Inactive Profile", null, config, "user");

        // Mark profile2 as inactive
        await _service.DeleteProfileAsync(profile2.ProfileId);

        // Act
        var profiles = await _service.GetAllProfilesAsync(activeOnly: true);

        // Assert
        Assert.Single(profiles);
        Assert.Equal("Active Profile", profiles[0].ProfileName);
    }

    [Fact]
    public async Task UpdateProfileAsync_ExistingProfile_UpdatesSuccessfully()
    {
        // Arrange
        var config = CreateTestTransferConfiguration();
        var saved = await _service.SaveProfileAsync("Original Name", "Original Description", config, "user1");

        var updatedConfig = CreateTestTransferConfiguration();
        updatedConfig.Source.Table = new TableIdentifier
        {
            Database = "TestDB",
            Schema = "dbo",
            Table = "UpdatedTable"
        };

        // Act
        await _service.UpdateProfileAsync(saved.ProfileId, "Updated Name", "Updated Description", updatedConfig, "user2");

        // Assert
        var retrieved = await _service.GetProfileAsync(saved.ProfileId);
        Assert.NotNull(retrieved);
        Assert.Equal("Updated Name", retrieved!.ProfileName);
        Assert.Equal("Updated Description", retrieved.Description);
        Assert.Equal("user2", retrieved.ModifiedBy);
        Assert.NotNull(retrieved.ModifiedDate);
        Assert.Equal("UpdatedTable", retrieved.Configuration.Source.Table!.Table);
    }

    [Fact]
    public async Task UpdateProfileAsync_NonExistentProfile_ThrowsInvalidOperationException()
    {
        // Arrange
        var config = CreateTestTransferConfiguration();

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await _service.UpdateProfileAsync("non-existent-id", "Name", null, config, "user");
        });
    }

    [Fact]
    public async Task DeleteProfileAsync_ExistingProfile_SoftDeletes()
    {
        // Arrange
        var config = CreateTestTransferConfiguration();
        var saved = await _service.SaveProfileAsync("To Delete", null, config, "user");

        // Act
        await _service.DeleteProfileAsync(saved.ProfileId);

        // Assert
        var allProfiles = await _service.GetAllProfilesAsync(activeOnly: false);
        var deletedProfile = allProfiles.FirstOrDefault(p => p.ProfileId == saved.ProfileId);
        Assert.NotNull(deletedProfile);
        Assert.False(deletedProfile!.IsActive);

        // Should not appear in active-only list
        var activeProfiles = await _service.GetAllProfilesAsync(activeOnly: true);
        Assert.DoesNotContain(activeProfiles, p => p.ProfileId == saved.ProfileId);
    }

    [Fact]
    public async Task SaveProfileAsync_WithTags_SavesTagsCorrectly()
    {
        // Arrange
        var config = CreateTestTransferConfiguration();
        var tags = new List<string> { "daily", "production", "orders" };

        // Act
        var profile = await _service.SaveProfileAsync("Tagged Profile", null, config, "user", tags);

        // Assert
        Assert.Equal(3, profile.Tags.Count);
        Assert.Contains("daily", profile.Tags);
        Assert.Contains("production", profile.Tags);
        Assert.Contains("orders", profile.Tags);
    }

    [Fact]
    public async Task GetProfileAsync_LoadsFullConfiguration()
    {
        // Arrange
        var config = CreateTestTransferConfiguration();
        config.Source.ConnectionString = "Server=localhost;Database=Source;";
        config.Source.Table = new TableIdentifier
        {
            Database = "SourceDB",
            Schema = "dbo",
            Table = "Orders"
        };
        config.Destination.ParquetPath = "/data/parquet/orders.parquet";
        config.Partitioning = new PartitioningConfiguration
        {
            Type = PartitionType.Date,
            Column = "OrderDate"
        };

        var saved = await _service.SaveProfileAsync("Complex Profile", null, config, "user");

        // Act
        var retrieved = await _service.GetProfileAsync(saved.ProfileId);

        // Assert
        Assert.NotNull(retrieved);
        Assert.Equal("Server=localhost;Database=Source;", retrieved!.Configuration.Source.ConnectionString);
        Assert.Equal("Orders", retrieved.Configuration.Source.Table!.Table);
        Assert.Equal("/data/parquet/orders.parquet", retrieved.Configuration.Destination.ParquetPath);
        Assert.NotNull(retrieved.Configuration.Partitioning);
        Assert.Equal(PartitionType.Date, retrieved.Configuration.Partitioning.Type);
        Assert.Equal("OrderDate", retrieved.Configuration.Partitioning.Column);
    }

    /// <summary>
    /// Helper method to create a test transfer configuration
    /// </summary>
    private static TransferConfiguration CreateTestTransferConfiguration()
    {
        return new TransferConfiguration
        {
            TransferType = TransferType.SqlToParquet,
            Source = new SourceConfiguration
            {
                Type = SourceType.SqlServer,
                ConnectionString = "Server=localhost;Database=TestDB;",
                Table = new TableIdentifier
                {
                    Database = "TestDB",
                    Schema = "dbo",
                    Table = "TestTable"
                }
            },
            Destination = new DestinationConfiguration
            {
                Type = DestinationType.Parquet,
                ParquetPath = "/data/parquet/test.parquet"
            }
        };
    }
}
