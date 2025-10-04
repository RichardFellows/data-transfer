using System.Text.Json;
using DataTransfer.Core.Models;
using DataTransfer.Configuration.Models;
using Microsoft.Extensions.Logging;

namespace DataTransfer.Configuration.Services;

/// <summary>
/// Service for managing transfer profiles (templates) with file-based storage
/// Profiles are stored as JSON files for portability and version control
/// </summary>
public class TransferProfileService
{
    private readonly ILogger<TransferProfileService> _logger;
    private readonly string _profilesDirectory;
    private readonly string _profilesFilePath;
    private readonly SemaphoreSlim _fileLock = new(1, 1);

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNameCaseInsensitive = true
    };

    public TransferProfileService(ILogger<TransferProfileService> logger, string? profilesDirectory = null)
    {
        _logger = logger;
        _profilesDirectory = profilesDirectory ?? Path.Combine(Directory.GetCurrentDirectory(), "profiles");
        _profilesFilePath = Path.Combine(_profilesDirectory, "profiles.json");

        // Ensure directory exists
        if (!Directory.Exists(_profilesDirectory))
        {
            Directory.CreateDirectory(_profilesDirectory);
        }
    }

    /// <summary>
    /// Saves a new transfer profile
    /// </summary>
    public async Task<TransferProfile> SaveProfileAsync(
        string profileName,
        string? description,
        TransferConfiguration configuration,
        string createdBy,
        List<string>? tags = null)
    {
        if (string.IsNullOrWhiteSpace(profileName))
        {
            throw new ArgumentException("Profile name cannot be empty", nameof(profileName));
        }

        await _fileLock.WaitAsync();
        try
        {
            var collection = await LoadProfilesCollectionAsync();

            // Check for duplicate names
            if (collection.Profiles.Any(p => p.ProfileName == profileName && p.IsActive))
            {
                throw new InvalidOperationException($"A profile with the name '{profileName}' already exists");
            }

            var profile = new TransferProfile
            {
                ProfileId = Guid.NewGuid().ToString(),
                ProfileName = profileName,
                Description = description,
                Configuration = configuration,
                Tags = tags ?? new List<string>(),
                CreatedBy = createdBy,
                CreatedDate = DateTime.UtcNow,
                IsActive = true
            };

            collection.Profiles.Add(profile);
            await SaveProfilesCollectionAsync(collection);

            _logger.LogInformation("Saved transfer profile: {ProfileName} (ID: {ProfileId})", profileName, profile.ProfileId);

            return profile;
        }
        finally
        {
            _fileLock.Release();
        }
    }

    /// <summary>
    /// Retrieves a profile by ID
    /// </summary>
    public async Task<TransferProfile?> GetProfileAsync(string profileId)
    {
        var collection = await LoadProfilesCollectionAsync();
        return collection.Profiles.FirstOrDefault(p => p.ProfileId == profileId);
    }

    /// <summary>
    /// Retrieves all profiles, optionally filtering to active only
    /// </summary>
    public async Task<List<TransferProfile>> GetAllProfilesAsync(bool activeOnly = true)
    {
        var collection = await LoadProfilesCollectionAsync();

        var profiles = activeOnly
            ? collection.Profiles.Where(p => p.IsActive).ToList()
            : collection.Profiles.ToList();

        return profiles.OrderByDescending(p => p.CreatedDate).ToList();
    }

    /// <summary>
    /// Updates an existing profile
    /// </summary>
    public async Task<TransferProfile> UpdateProfileAsync(
        string profileId,
        string profileName,
        string? description,
        TransferConfiguration configuration,
        string modifiedBy,
        List<string>? tags = null)
    {
        await _fileLock.WaitAsync();
        try
        {
            var collection = await LoadProfilesCollectionAsync();
            var profile = collection.Profiles.FirstOrDefault(p => p.ProfileId == profileId);

            if (profile == null)
            {
                throw new InvalidOperationException($"Profile with ID '{profileId}' not found");
            }

            // Check for duplicate names (excluding current profile)
            if (collection.Profiles.Any(p => p.ProfileName == profileName && p.ProfileId != profileId && p.IsActive))
            {
                throw new InvalidOperationException($"A profile with the name '{profileName}' already exists");
            }

            profile.ProfileName = profileName;
            profile.Description = description;
            profile.Configuration = configuration;
            profile.Tags = tags ?? profile.Tags;
            profile.ModifiedBy = modifiedBy;
            profile.ModifiedDate = DateTime.UtcNow;

            await SaveProfilesCollectionAsync(collection);

            _logger.LogInformation("Updated transfer profile: {ProfileName} (ID: {ProfileId})", profileName, profileId);

            return profile;
        }
        finally
        {
            _fileLock.Release();
        }
    }

    /// <summary>
    /// Soft deletes a profile (sets IsActive = false)
    /// </summary>
    public async Task DeleteProfileAsync(string profileId)
    {
        await _fileLock.WaitAsync();
        try
        {
            var collection = await LoadProfilesCollectionAsync();
            var profile = collection.Profiles.FirstOrDefault(p => p.ProfileId == profileId);

            if (profile == null)
            {
                throw new InvalidOperationException($"Profile with ID '{profileId}' not found");
            }

            profile.IsActive = false;
            await SaveProfilesCollectionAsync(collection);

            _logger.LogInformation("Deleted transfer profile: {ProfileName} (ID: {ProfileId})", profile.ProfileName, profileId);
        }
        finally
        {
            _fileLock.Release();
        }
    }

    /// <summary>
    /// Searches profiles by name, description, or tags
    /// </summary>
    public async Task<List<TransferProfile>> SearchProfilesAsync(string searchTerm, bool activeOnly = true)
    {
        var profiles = await GetAllProfilesAsync(activeOnly);

        if (string.IsNullOrWhiteSpace(searchTerm))
        {
            return profiles;
        }

        var lowerSearch = searchTerm.ToLowerInvariant();

        return profiles.Where(p =>
            (p.ProfileName?.ToLowerInvariant().Contains(lowerSearch) ?? false) ||
            (p.Description?.ToLowerInvariant().Contains(lowerSearch) ?? false) ||
            p.Tags.Any(t => t.ToLowerInvariant().Contains(lowerSearch))
        ).ToList();
    }

    /// <summary>
    /// Loads the profiles collection from disk
    /// </summary>
    private async Task<ProfilesCollection> LoadProfilesCollectionAsync()
    {
        if (!File.Exists(_profilesFilePath))
        {
            return new ProfilesCollection();
        }

        try
        {
            var json = await File.ReadAllTextAsync(_profilesFilePath);
            var collection = JsonSerializer.Deserialize<ProfilesCollection>(json, JsonOptions);
            return collection ?? new ProfilesCollection();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load profiles from {FilePath}", _profilesFilePath);
            throw new InvalidOperationException("Failed to load profiles file", ex);
        }
    }

    /// <summary>
    /// Saves the profiles collection to disk
    /// </summary>
    private async Task SaveProfilesCollectionAsync(ProfilesCollection collection)
    {
        try
        {
            var json = JsonSerializer.Serialize(collection, JsonOptions);
            await File.WriteAllTextAsync(_profilesFilePath, json);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save profiles to {FilePath}", _profilesFilePath);
            throw new InvalidOperationException("Failed to save profiles file", ex);
        }
    }
}
