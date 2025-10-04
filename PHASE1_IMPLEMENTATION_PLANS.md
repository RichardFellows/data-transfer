# Phase 1: Core Automation - Implementation Plans

**Goal:** Enable daily automated production extracts for UAT testing

**Duration:** 2 weeks
**Priority:** High
**Dependencies:** Current Web UI implementation

---

## Overview

Phase 1 focuses on transforming the DataTransfer system from a manual, one-time tool into an automated solution suitable for daily production-to-UAT data synchronization. This phase establishes the foundation for scheduled, repeatable transfers with proper persistence and notification.

---

## Implementation Items

### Item #2: Transfer Profiles/Templates ⭐
**Priority:** CRITICAL (foundation for other features)
**Effort:** M (3-5 days)
**Impact:** HIGH - Enables reusability and consistency

#### Current State
- Transfers are configured manually each time
- No way to save configurations
- Users must remember connection details, table selections, etc.

#### Proposed Solution

##### 1. Database Schema
Create `TransferProfiles` table:

```sql
CREATE TABLE TransferProfiles (
    ProfileId INT IDENTITY(1,1) PRIMARY KEY,
    ProfileName NVARCHAR(100) NOT NULL UNIQUE,
    Description NVARCHAR(500) NULL,
    TransferType VARCHAR(50) NOT NULL, -- 'SqlToParquet', 'ParquetToSql'

    -- Source configuration (JSON for flexibility)
    SourceConfig NVARCHAR(MAX) NOT NULL,

    -- Destination configuration (JSON)
    DestinationConfig NVARCHAR(MAX) NOT NULL,

    -- Metadata
    CreatedBy NVARCHAR(100) NOT NULL,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    ModifiedBy NVARCHAR(100) NULL,
    ModifiedDate DATETIME2 NULL,

    -- Tags for organization
    Tags NVARCHAR(500) NULL,

    -- Enable/disable
    IsActive BIT NOT NULL DEFAULT 1,

    INDEX IX_ProfileName (ProfileName),
    INDEX IX_TransferType (TransferType),
    INDEX IX_CreatedDate (CreatedDate DESC)
);
```

##### 2. C# Models

```csharp
// src/DataTransfer.Web/Models/TransferProfile.cs
namespace DataTransfer.Web.Models;

public class TransferProfile
{
    public int ProfileId { get; set; }
    public string ProfileName { get; set; } = string.Empty;
    public string? Description { get; set; }
    public TransferType TransferType { get; set; }

    // Serialized as JSON in database
    public SourceConfiguration SourceConfig { get; set; } = new();
    public DestinationConfiguration DestinationConfig { get; set; } = new();

    public string CreatedBy { get; set; } = string.Empty;
    public DateTime CreatedDate { get; set; }
    public string? ModifiedBy { get; set; }
    public DateTime? ModifiedDate { get; set; }

    public List<string> Tags { get; set; } = new();
    public bool IsActive { get; set; } = true;
}

public class ProfileSourceConfig
{
    public string? ConnectionString { get; set; }
    public string? Database { get; set; }
    public string? Schema { get; set; }
    public string? Table { get; set; }
    public string? ParquetPath { get; set; }
}

public class ProfileDestinationConfig
{
    public string? ConnectionString { get; set; }
    public string? Database { get; set; }
    public string? Schema { get; set; }
    public string? Table { get; set; }
    public string? ParquetPath { get; set; }
}
```

##### 3. Service Layer

```csharp
// src/DataTransfer.Web/Services/TransferProfileService.cs
public class TransferProfileService
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<TransferProfileService> _logger;

    public async Task<TransferProfile> SaveProfileAsync(
        string profileName,
        TransferConfiguration config,
        string createdBy)
    {
        // Validate profile name is unique
        // Serialize SourceConfig and DestinationConfig to JSON
        // Save to database
        // Return saved profile with generated ID
    }

    public async Task<TransferProfile?> GetProfileAsync(int profileId)
    {
        // Load from database
        // Deserialize JSON configs
        // Return profile or null
    }

    public async Task<List<TransferProfile>> GetAllProfilesAsync(bool activeOnly = true)
    {
        // Load all profiles
        // Filter by IsActive if requested
        // Order by most recently used
    }

    public async Task<TransferConfiguration> LoadProfileConfigurationAsync(int profileId)
    {
        // Load profile
        // Convert to TransferConfiguration
        // Return ready-to-execute config
    }

    public async Task UpdateProfileAsync(
        int profileId,
        string profileName,
        TransferConfiguration config,
        string modifiedBy)
    {
        // Update existing profile
        // Update ModifiedBy and ModifiedDate
    }

    public async Task DeleteProfileAsync(int profileId)
    {
        // Soft delete (set IsActive = false)
        // Or hard delete based on requirements
    }

    public async Task<List<TransferProfile>> SearchProfilesAsync(string searchTerm)
    {
        // Search by name, description, tags
        // Return matching profiles
    }
}
```

##### 4. UI Components

**Save Profile Dialog:**
```razor
<!-- Components/SaveProfileDialog.razor -->
<div class="modal" @if="@ShowDialog">
    <div class="modal-content">
        <h3>Save Transfer Profile</h3>

        <div class="form-group">
            <label>Profile Name *</label>
            <input @bind="ProfileName" class="form-control"
                   placeholder="Daily Orders Extract" />
        </div>

        <div class="form-group">
            <label>Description</label>
            <textarea @bind="Description" class="form-control"
                      placeholder="Extracts last 30 days of orders for UAT testing"></textarea>
        </div>

        <div class="form-group">
            <label>Tags (comma-separated)</label>
            <input @bind="Tags" class="form-control"
                   placeholder="orders, daily, uat" />
        </div>

        <div class="modal-actions">
            <button @onclick="SaveProfile" class="btn btn-primary">Save</button>
            <button @onclick="Cancel" class="btn btn-secondary">Cancel</button>
        </div>
    </div>
</div>
```

**Load Profile Dropdown:**
```razor
<!-- On NewTransfer.razor page -->
<div class="mb-3">
    <label class="form-label">Load From Profile</label>
    <select @bind="_selectedProfileId" @bind:after="OnProfileSelected" class="form-select">
        <option value="0">-- Start from scratch --</option>
        @foreach (var profile in _profiles)
        {
            <option value="@profile.ProfileId">@profile.ProfileName</option>
        }
    </select>
    @if (_selectedProfileId > 0)
    {
        <div class="form-text text-success">
            ✓ Profile loaded: @_selectedProfile?.Description
        </div>
    }
</div>

<div class="mb-3">
    <button @onclick="ShowSaveProfileDialog" class="btn btn-outline-primary">
        💾 Save Current Configuration as Profile
    </button>
</div>
```

##### 5. Testing Strategy

**Unit Tests:**
```csharp
[Fact]
public async Task SaveProfile_ValidProfile_ReturnsSavedProfile()
{
    // Arrange
    var service = new TransferProfileService(_config, _logger);
    var config = CreateTestTransferConfiguration();

    // Act
    var profile = await service.SaveProfileAsync("Test Profile", config, "testuser");

    // Assert
    Assert.NotEqual(0, profile.ProfileId);
    Assert.Equal("Test Profile", profile.ProfileName);
    Assert.Equal("testuser", profile.CreatedBy);
}

[Fact]
public async Task LoadProfileConfiguration_ValidProfileId_ReturnsConfiguration()
{
    // Test profile can be loaded and converted back to TransferConfiguration
}
```

**Playwright E2E Tests:**
```csharp
[Fact]
public async Task SaveProfile_FillFormAndSave_ProfileAppears InDropdown()
{
    // Navigate to New Transfer
    // Fill out form
    // Click "Save as Profile"
    // Fill profile name
    // Save
    // Reload page
    // Verify profile appears in dropdown
}
```

#### Implementation Steps (TDD)

1. **[RED]** Write failing test for `TransferProfileService.SaveProfileAsync`
2. **[GREEN]** Implement database schema and save logic
3. **[REFACTOR]** Extract JSON serialization logic
4. **[RED]** Write test for `GetProfileAsync`
5. **[GREEN]** Implement profile loading
6. **[REFACTOR]** Add error handling
7. **[RED]** Write Blazor component tests
8. **[GREEN]** Implement UI components
9. **[REFACTOR]** Improve UX with loading states
10. **[GREEN]** Add Playwright E2E test

#### Database Migration

```sql
-- migrations/001_create_transfer_profiles.sql
-- Run this before deploying code changes
-- Idempotent (safe to run multiple times)

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'TransferProfiles')
BEGIN
    CREATE TABLE TransferProfiles (
        ProfileId INT IDENTITY(1,1) PRIMARY KEY,
        ProfileName NVARCHAR(100) NOT NULL UNIQUE,
        Description NVARCHAR(500) NULL,
        TransferType VARCHAR(50) NOT NULL,
        SourceConfig NVARCHAR(MAX) NOT NULL,
        DestinationConfig NVARCHAR(MAX) NOT NULL,
        CreatedBy NVARCHAR(100) NOT NULL,
        CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        ModifiedBy NVARCHAR(100) NULL,
        ModifiedDate DATETIME2 NULL,
        Tags NVARCHAR(500) NULL,
        IsActive BIT NOT NULL DEFAULT 1
    );

    CREATE INDEX IX_ProfileName ON TransferProfiles(ProfileName);
    CREATE INDEX IX_TransferType ON TransferProfiles(TransferType);
    CREATE INDEX IX_CreatedDate ON TransferProfiles(CreatedDate DESC);
END
```

#### Success Criteria
- ✅ Users can save current transfer configuration as a named profile
- ✅ Users can load saved profiles from dropdown
- ✅ Profiles persist across sessions
- ✅ Profiles can be edited and deleted
- ✅ 10+ unit tests passing
- ✅ 3+ Playwright tests demonstrating save/load/reuse

---

### Item #34: Save Transfer Configurations ⭐
**Priority:** CRITICAL (enables #2)
**Effort:** S (included in Item #2)
**Impact:** HIGH

#### Note
This item is implemented as part of Item #2 (Transfer Profiles). The distinction is:
- Item #34: Technical capability to persist configurations
- Item #2: User-facing feature to manage profiles

Both are delivered together in the TransferProfile implementation above.

---

### Item #1: Scheduled Transfers 🔥
**Priority:** CRITICAL
**Effort:** L (1-2 weeks)
**Impact:** HIGH - Core automation requirement

#### Current State
- All transfers are manually triggered
- No way to schedule daily/weekly transfers
- Users must remember to run transfers

#### Proposed Solution

##### 1. Technology Choice: Quartz.NET

**Why Quartz.NET:**
- Industry-standard .NET scheduler
- Cron expression support
- Persistent job storage (SQL Server)
- Clustering support (multiple web servers)
- Misfire handling
- Job history tracking

**Dependencies:**
```xml
<PackageReference Include="Quartz" Version="3.8.0" />
<PackageReference Include="Quartz.Extensions.Hosting" Version="3.8.0" />
<PackageReference Include="Quartz.Serialization.Json" Version="3.8.0" />
```

##### 2. Database Schema

```sql
-- Quartz.NET creates these automatically, but we add our custom table
CREATE TABLE TransferSchedules (
    ScheduleId INT IDENTITY(1,1) PRIMARY KEY,
    ProfileId INT NOT NULL FOREIGN KEY REFERENCES TransferProfiles(ProfileId),

    ScheduleName NVARCHAR(100) NOT NULL,
    CronExpression VARCHAR(100) NOT NULL, -- "0 2 * * *" = 2 AM daily
    TimeZone VARCHAR(50) NOT NULL DEFAULT 'UTC',

    IsEnabled BIT NOT NULL DEFAULT 1,

    -- Quartz job details
    QuartzJobKey VARCHAR(200) NULL,
    QuartzTriggerKey VARCHAR(200) NULL,

    -- Execution settings
    MaxRetries INT NOT NULL DEFAULT 3,
    RetryDelayMinutes INT NOT NULL DEFAULT 5,
    TimeoutMinutes INT NOT NULL DEFAULT 60,

    -- Metadata
    CreatedBy NVARCHAR(100) NOT NULL,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    ModifiedBy NVARCHAR(100) NULL,
    ModifiedDate DATETIME2 NULL,

    -- Statistics (updated by jobs)
    LastExecutionDate DATETIME2 NULL,
    LastExecutionStatus VARCHAR(50) NULL, -- 'Success', 'Failed', 'Running'
    LastExecutionMessage NVARCHAR(MAX) NULL,
    NextScheduledDate DATETIME2 NULL,
    TotalExecutions INT NOT NULL DEFAULT 0,
    SuccessfulExecutions INT NOT NULL DEFAULT 0,
    FailedExecutions INT NOT NULL DEFAULT 0,

    INDEX IX_ProfileId (ProfileId),
    INDEX IX_IsEnabled (IsEnabled),
    INDEX IX_NextScheduledDate (NextScheduledDate)
);
```

##### 3. Quartz Job Implementation

```csharp
// src/DataTransfer.Web/Jobs/TransferExecutionJob.cs
using Quartz;

[DisallowConcurrentExecution] // Don't run same job twice simultaneously
public class TransferExecutionJob : IJob
{
    private readonly TransferExecutionService _executionService;
    private readonly TransferProfileService _profileService;
    private readonly ILogger<TransferExecutionJob> _logger;

    public TransferExecutionJob(
        TransferExecutionService executionService,
        TransferProfileService profileService,
        ILogger<TransferExecutionJob> logger)
    {
        _executionService = executionService;
        _profileService = profileService;
        _logger = logger;
    }

    public async Task Execute(IJobExecutionContext context)
    {
        var scheduleId = context.JobDetail.JobDataMap.GetInt("ScheduleId");
        var profileId = context.JobDetail.JobDataMap.GetInt("ProfileId");

        _logger.LogInformation("Starting scheduled transfer: ScheduleId={ScheduleId}, ProfileId={ProfileId}",
            scheduleId, profileId);

        try
        {
            // Load profile configuration
            var config = await _profileService.LoadProfileConfigurationAsync(profileId);

            // Execute transfer
            var transferId = $"scheduled-{scheduleId}-{Guid.NewGuid()}";
            var result = await _executionService.ExecuteAsync(config, transferId, context.CancellationToken);

            // Update schedule statistics
            await UpdateScheduleStatsAsync(scheduleId, result.Success, result.ErrorMessage);

            if (result.Success)
            {
                _logger.LogInformation("Scheduled transfer completed successfully: {TransferId}", transferId);
            }
            else
            {
                _logger.LogError("Scheduled transfer failed: {TransferId}, Error: {Error}",
                    transferId, result.ErrorMessage);

                // This will trigger Quartz retry if configured
                throw new JobExecutionException(result.ErrorMessage);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Scheduled transfer failed with exception");
            await UpdateScheduleStatsAsync(scheduleId, false, ex.Message);
            throw new JobExecutionException(ex);
        }
    }

    private async Task UpdateScheduleStatsAsync(int scheduleId, bool success, string? message)
    {
        // Update LastExecutionDate, LastExecutionStatus, TotalExecutions, etc.
    }
}
```

##### 4. Scheduler Service

```csharp
// src/DataTransfer.Web/Services/TransferSchedulerService.cs
public class TransferSchedulerService
{
    private readonly ISchedulerFactory _schedulerFactory;
    private readonly ILogger<TransferSchedulerService> _logger;

    public async Task<ScheduleInfo> CreateScheduleAsync(
        int profileId,
        string scheduleName,
        string cronExpression,
        string timeZone = "UTC")
    {
        var scheduler = await _schedulerFactory.GetScheduler();

        // Create job
        var jobKey = new JobKey($"transfer-{profileId}-{Guid.NewGuid()}", "transfers");
        var job = JobBuilder.Create<TransferExecutionJob>()
            .WithIdentity(jobKey)
            .UsingJobData("ProfileId", profileId)
            .Build();

        // Create trigger with cron schedule
        var triggerKey = new TriggerKey($"trigger-{profileId}-{Guid.NewGuid()}", "transfers");
        var trigger = TriggerBuilder.Create()
            .WithIdentity(triggerKey)
            .WithCronSchedule(cronExpression, x => x.InTimeZone(TimeZoneInfo.FindSystemTimeZoneById(timeZone)))
            .Build();

        // Schedule job
        await scheduler.ScheduleJob(job, trigger);

        // Save to database
        return await SaveScheduleToDatabaseAsync(profileId, scheduleName, cronExpression,
            timeZone, jobKey.ToString(), triggerKey.ToString());
    }

    public async Task PauseScheduleAsync(int scheduleId)
    {
        var schedule = await LoadScheduleFromDatabaseAsync(scheduleId);
        var scheduler = await _schedulerFactory.GetScheduler();
        await scheduler.PauseJob(JobKey.Create(schedule.QuartzJobKey));
    }

    public async Task ResumeScheduleAsync(int scheduleId)
    {
        var schedule = await LoadScheduleFromDatabaseAsync(scheduleId);
        var scheduler = await _schedulerFactory.GetScheduler();
        await scheduler.ResumeJob(JobKey.Create(schedule.QuartzJobKey));
    }

    public async Task DeleteScheduleAsync(int scheduleId)
    {
        var schedule = await LoadScheduleFromDatabaseAsync(scheduleId);
        var scheduler = await _schedulerFactory.GetScheduler();
        await scheduler.DeleteJob(JobKey.Create(schedule.QuartzJobKey));
        await DeleteScheduleFromDatabaseAsync(scheduleId);
    }

    public async Task<List<ScheduleInfo>> GetAllSchedulesAsync()
    {
        // Load from database with statistics
    }

    public async Task<DateTime?> GetNextExecutionTimeAsync(int scheduleId)
    {
        var schedule = await LoadScheduleFromDatabaseAsync(scheduleId);
        var scheduler = await _schedulerFactory.GetScheduler();
        var trigger = await scheduler.GetTrigger(TriggerKey.Create(schedule.QuartzTriggerKey));
        return trigger?.GetNextFireTimeUtc()?.LocalDateTime;
    }
}
```

##### 5. Program.cs Configuration

```csharp
// src/DataTransfer.Web/Program.cs
builder.Services.AddQuartz(q =>
{
    q.UseMicrosoftDependencyInjectionJobFactory();

    // Use persistent store (SQL Server)
    q.UsePersistentStore(store =>
    {
        store.UseProperties = true;
        store.UseSqlServer(builder.Configuration.GetConnectionString("DefaultConnection"));
        store.UseJsonSerializer();
    });

    // Job configuration
    q.AddJob<TransferExecutionJob>(opts => opts.WithIdentity("transfer-job-template"));
});

builder.Services.AddQuartzHostedService(q => q.WaitForJobsToComplete = true);
builder.Services.AddSingleton<TransferSchedulerService>();
```

##### 6. UI Components

**Schedule Management Page:**
```razor
<!-- Pages/Schedules.razor -->
@page "/schedules"

<h1>Transfer Schedules</h1>

<button @onclick="ShowCreateScheduleDialog" class="btn btn-primary mb-3">
    ➕ Create New Schedule
</button>

<table class="table">
    <thead>
        <tr>
            <th>Name</th>
            <th>Profile</th>
            <th>Schedule (Cron)</th>
            <th>Next Run</th>
            <th>Last Run</th>
            <th>Status</th>
            <th>Success Rate</th>
            <th>Actions</th>
        </tr>
    </thead>
    <tbody>
        @foreach (var schedule in _schedules)
        {
            <tr>
                <td>@schedule.ScheduleName</td>
                <td>@schedule.ProfileName</td>
                <td>
                    <code>@schedule.CronExpression</code>
                    <small class="text-muted d-block">@FormatCronDescription(schedule.CronExpression)</small>
                </td>
                <td>@schedule.NextScheduledDate?.ToString("yyyy-MM-dd HH:mm")</td>
                <td>
                    @schedule.LastExecutionDate?.ToString("yyyy-MM-dd HH:mm")
                    @if (schedule.LastExecutionStatus == "Failed")
                    {
                        <span class="badge bg-danger">Failed</span>
                    }
                </td>
                <td>
                    @if (schedule.IsEnabled)
                    {
                        <span class="badge bg-success">Enabled</span>
                    }
                    else
                    {
                        <span class="badge bg-secondary">Paused</span>
                    }
                </td>
                <td>
                    @if (schedule.TotalExecutions > 0)
                    {
                        var successRate = (schedule.SuccessfulExecutions * 100 / schedule.TotalExecutions);
                        <span class="@(successRate >= 90 ? "text-success" : "text-warning")">
                            @successRate% (@schedule.SuccessfulExecutions/@schedule.TotalExecutions)
                        </span>
                    }
                    else
                    {
                        <span class="text-muted">No executions</span>
                    }
                </td>
                <td>
                    @if (schedule.IsEnabled)
                    {
                        <button @onclick="() => PauseSchedule(schedule.ScheduleId)" class="btn btn-sm btn-warning">
                            ⏸ Pause
                        </button>
                    }
                    else
                    {
                        <button @onclick="() => ResumeSchedule(schedule.ScheduleId)" class="btn btn-sm btn-success">
                            ▶ Resume
                        </button>
                    }
                    <button @onclick="() => EditSchedule(schedule)" class="btn btn-sm btn-primary">
                        ✏ Edit
                    </button>
                    <button @onclick="() => DeleteSchedule(schedule.ScheduleId)" class="btn btn-sm btn-danger">
                        🗑 Delete
                    </button>
                </td>
            </tr>
        }
    </tbody>
</table>
```

**Create/Edit Schedule Dialog:**
```razor
<div class="modal">
    <h3>@(_isEditMode ? "Edit" : "Create") Schedule</h3>

    <div class="form-group">
        <label>Schedule Name *</label>
        <input @bind="_scheduleName" class="form-control"
               placeholder="Daily Production Sync" />
    </div>

    <div class="form-group">
        <label>Transfer Profile *</label>
        <select @bind="_selectedProfileId" class="form-select">
            <option value="0">-- Select Profile --</option>
            @foreach (var profile in _profiles)
            {
                <option value="@profile.ProfileId">@profile.ProfileName</option>
            }
        </select>
    </div>

    <div class="form-group">
        <label>Schedule *</label>
        <select @bind="_schedulePreset" @bind:after="OnPresetChanged" class="form-select">
            <option value="">Custom</option>
            <option value="0 2 * * *">Daily at 2:00 AM</option>
            <option value="0 3 * * MON-FRI">Weekdays at 3:00 AM</option>
            <option value="0 4 * * SUN">Weekly on Sunday at 4:00 AM</option>
            <option value="0 0 1 * *">Monthly on 1st at midnight</option>
        </select>
    </div>

    <div class="form-group">
        <label>Cron Expression *</label>
        <input @bind="_cronExpression" class="form-control"
               placeholder="0 2 * * *" />
        <div class="form-text">
            @GetCronDescription(_cronExpression)
            <a href="https://crontab.guru/" target="_blank">Cron expression help</a>
        </div>
    </div>

    <div class="form-group">
        <label>Time Zone</label>
        <select @bind="_timeZone" class="form-select">
            <option value="UTC">UTC</option>
            <option value="Eastern Standard Time">Eastern (US)</option>
            <option value="Central Standard Time">Central (US)</option>
            <option value="Pacific Standard Time">Pacific (US)</option>
        </select>
    </div>

    <div class="modal-actions">
        <button @onclick="SaveSchedule" class="btn btn-primary">
            @(_isEditMode ? "Update" : "Create") Schedule
        </button>
        <button @onclick="Cancel" class="btn btn-secondary">Cancel</button>
    </div>
</div>
```

##### 7. Testing Strategy

**Unit Tests:**
```csharp
[Fact]
public async Task CreateSchedule_ValidCron_CreatesJobAndTrigger()
{
    // Test that Quartz job and trigger are created correctly
}

[Fact]
public async Task ExecuteJob_ValidProfile_CompletesSuccessfully()
{
    // Test TransferExecutionJob executes and updates stats
}

[Fact]
public async Task ParseCronExpression_DailyAt2AM_ReturnsCorrectNextRun()
{
    // Test cron parsing logic
}
```

**Integration Tests:**
```csharp
[Fact]
public async Task ScheduledJob_TriggersAtScheduledTime_ExecutesTransfer()
{
    // Create schedule with cron that fires in 5 seconds
    // Wait for execution
    // Verify transfer completed
    // Check history entry created
}
```

#### Implementation Steps (TDD)

1. **[RED]** Write test for schedule creation
2. **[GREEN]** Add Quartz.NET dependencies and basic configuration
3. **[GREEN]** Implement TransferExecutionJob
4. **[REFACTOR]** Extract statistics update logic
5. **[RED]** Write test for schedule management service
6. **[GREEN]** Implement TransferSchedulerService
7. **[RED]** Write UI component tests
8. **[GREEN]** Implement schedule management UI
9. **[GREEN]** Add integration test with actual job execution
10. **[REFACTOR]** Improve error handling and logging

#### Success Criteria
- ✅ Users can create schedules with cron expressions
- ✅ Schedules execute transfers at specified times
- ✅ Failed transfers trigger retries (configurable)
- ✅ Schedule statistics tracked (success rate, last run, next run)
- ✅ Schedules can be paused/resumed/deleted
- ✅ Comprehensive logging of scheduled executions
- ✅ 15+ unit tests passing
- ✅ 3+ integration tests demonstrating scheduled execution

---

### Item #3: Background Job System ⭐
**Priority:** CRITICAL (part of #1)
**Effort:** S (included in Item #1)
**Impact:** HIGH

#### Note
This item is implemented as part of Item #1 (Scheduled Transfers). Quartz.NET provides the background job system infrastructure including:
- Job scheduling and execution
- Persistent job storage
- Retry mechanisms
- Clustering support
- Job history

No additional implementation needed beyond what's in Item #1.

---

### Item #4: Batch/Bulk Operations 🔥
**Priority:** HIGH
**Effort:** M (3-5 days)
**Impact:** HIGH - Dramatically improves daily workflow

#### Current State
- Users can only transfer one table at a time
- Must manually repeat process for each table
- No way to transfer related tables together

#### Proposed Solution

##### 1. Database Schema

```sql
CREATE TABLE TransferBatches (
    BatchId INT IDENTITY(1,1) PRIMARY KEY,
    BatchName NVARCHAR(100) NOT NULL,
    Description NVARCHAR(500) NULL,

    -- Execution mode
    ExecutionMode VARCHAR(50) NOT NULL DEFAULT 'Sequential', -- 'Sequential', 'Parallel'
    MaxParallelism INT NULL, -- For parallel mode

    -- Metadata
    CreatedBy NVARCHAR(100) NOT NULL,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    IsActive BIT NOT NULL DEFAULT 1,

    INDEX IX_BatchName (BatchName)
);

CREATE TABLE TransferBatchItems (
    BatchItemId INT IDENTITY(1,1) PRIMARY KEY,
    BatchId INT NOT NULL FOREIGN KEY REFERENCES TransferBatches(BatchId) ON DELETE CASCADE,
    ProfileId INT NOT NULL FOREIGN KEY REFERENCES TransferProfiles(ProfileId),

    -- Execution order (for sequential mode)
    ExecutionOrder INT NOT NULL,

    -- Dependencies (future enhancement)
    DependsOnBatchItemId INT NULL FOREIGN KEY REFERENCES TransferBatchItems(BatchItemId),

    -- Continue on error?
    ContinueOnError BIT NOT NULL DEFAULT 1,

    INDEX IX_BatchId (BatchId),
    INDEX IX_ExecutionOrder (BatchId, ExecutionOrder)
);

CREATE TABLE TransferBatchExecutions (
    BatchExecutionId INT IDENTITY(1,1) PRIMARY KEY,
    BatchId INT NOT NULL FOREIGN KEY REFERENCES TransferBatches(BatchId),

    StartTime DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    EndTime DATETIME2 NULL,
    Status VARCHAR(50) NOT NULL, -- 'Running', 'Completed', 'PartiallyFailed', 'Failed'

    -- Statistics
    TotalItems INT NOT NULL,
    CompletedItems INT NOT NULL DEFAULT 0,
    FailedItems INT NOT NULL DEFAULT 0,
    TotalRowsTransferred BIGINT NOT NULL DEFAULT 0,

    ErrorMessage NVARCHAR(MAX) NULL,

    INDEX IX_BatchId (BatchId),
    INDEX IX_StartTime (StartTime DESC)
);

CREATE TABLE TransferBatchExecutionItems (
    BatchExecutionItemId INT IDENTITY(1,1) PRIMARY KEY,
    BatchExecutionId INT NOT NULL FOREIGN KEY REFERENCES TransferBatchExecutions(BatchExecutionId) ON DELETE CASCADE,
    BatchItemId INT NOT NULL FOREIGN KEY REFERENCES TransferBatchItems(BatchItemId),

    TransferId NVARCHAR(100) NOT NULL, -- Links to TransferHistoryEntry

    StartTime DATETIME2 NULL,
    EndTime DATETIME2 NULL,
    Status VARCHAR(50) NOT NULL, -- 'Pending', 'Running', 'Completed', 'Failed', 'Skipped'
    RowsTransferred BIGINT NULL,
    ErrorMessage NVARCHAR(MAX) NULL,

    INDEX IX_BatchExecutionId (BatchExecutionId),
    INDEX IX_Status (Status)
);
```

##### 2. Models

```csharp
public class TransferBatch
{
    public int BatchId { get; set; }
    public string BatchName { get; set; } = string.Empty;
    public string? Description { get; set; }
    public BatchExecutionMode ExecutionMode { get; set; }
    public int? MaxParallelism { get; set; }
    public List<TransferBatchItem> Items { get; set; } = new();
    public string CreatedBy { get; set; } = string.Empty;
    public DateTime CreatedDate { get; set; }
    public bool IsActive { get; set; } = true;
}

public class TransferBatchItem
{
    public int BatchItemId { get; set; }
    public int BatchId { get; set; }
    public int ProfileId { get; set; }
    public string ProfileName { get; set; } = string.Empty; // Denormalized for display
    public int ExecutionOrder { get; set; }
    public int? DependsOnBatchItemId { get; set; }
    public bool ContinueOnError { get; set; } = true;
}

public enum BatchExecutionMode
{
    Sequential, // One at a time, in order
    Parallel    // Multiple concurrent transfers
}

public class BatchExecutionResult
{
    public int BatchExecutionId { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime? EndTime { get; set; }
    public string Status { get; set; } = string.Empty;
    public int TotalItems { get; set; }
    public int CompletedItems { get; set; }
    public int FailedItems { get; set; }
    public long TotalRowsTransferred { get; set; }
    public List<BatchItemExecutionResult> ItemResults { get; set; } = new();
}
```

##### 3. Service Implementation

```csharp
// src/DataTransfer.Web/Services/TransferBatchService.cs
public class TransferBatchService
{
    private readonly TransferExecutionService _executionService;
    private readonly TransferProfileService _profileService;
    private readonly ILogger<TransferBatchService> _logger;

    public async Task<BatchExecutionResult> ExecuteBatchAsync(
        int batchId,
        CancellationToken cancellationToken = default)
    {
        var batch = await LoadBatchAsync(batchId);
        var executionId = await CreateBatchExecutionAsync(batchId, batch.Items.Count);

        var result = new BatchExecutionResult
        {
            BatchExecutionId = executionId,
            StartTime = DateTime.UtcNow,
            TotalItems = batch.Items.Count
        };

        try
        {
            if (batch.ExecutionMode == BatchExecutionMode.Sequential)
            {
                await ExecuteSequentiallyAsync(batch, executionId, result, cancellationToken);
            }
            else
            {
                await ExecuteInParallelAsync(batch, executionId, result, cancellationToken);
            }

            result.EndTime = DateTime.UtcNow;
            result.Status = result.FailedItems == 0 ? "Completed" :
                           result.CompletedItems > 0 ? "PartiallyFailed" : "Failed";
        }
        catch (Exception ex)
        {
            result.EndTime = DateTime.UtcNow;
            result.Status = "Failed";
            _logger.LogError(ex, "Batch execution failed: BatchId={BatchId}", batchId);
        }

        await UpdateBatchExecutionAsync(result);
        return result;
    }

    private async Task ExecuteSequentiallyAsync(
        TransferBatch batch,
        int executionId,
        BatchExecutionResult result,
        CancellationToken cancellationToken)
    {
        foreach (var item in batch.Items.OrderBy(x => x.ExecutionOrder))
        {
            cancellationToken.ThrowIfCancellationRequested();

            _logger.LogInformation("Executing batch item {Order}/{Total}: Profile={ProfileName}",
                item.ExecutionOrder, batch.Items.Count, item.ProfileName);

            var itemResult = await ExecuteBatchItemAsync(item, executionId, cancellationToken);
            result.ItemResults.Add(itemResult);

            if (itemResult.Success)
            {
                result.CompletedItems++;
                result.TotalRowsTransferred += itemResult.RowsTransferred;
            }
            else
            {
                result.FailedItems++;

                if (!item.ContinueOnError)
                {
                    _logger.LogWarning("Batch item failed and ContinueOnError=false, stopping batch");
                    break;
                }
            }
        }
    }

    private async Task ExecuteInParallelAsync(
        TransferBatch batch,
        int executionId,
        BatchExecutionResult result,
        CancellationToken cancellationToken)
    {
        var maxDegreeOfParallelism = batch.MaxParallelism ?? Environment.ProcessorCount;

        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = maxDegreeOfParallelism,
            CancellationToken = cancellationToken
        };

        var results = new ConcurrentBag<BatchItemExecutionResult>();

        await Parallel.ForEachAsync(batch.Items, options, async (item, ct) =>
        {
            var itemResult = await ExecuteBatchItemAsync(item, executionId, ct);
            results.Add(itemResult);
        });

        result.ItemResults.AddRange(results);
        result.CompletedItems = results.Count(x => x.Success);
        result.FailedItems = results.Count(x => !x.Success);
        result.TotalRowsTransferred = results.Sum(x => x.RowsTransferred);
    }

    private async Task<BatchItemExecutionResult> ExecuteBatchItemAsync(
        TransferBatchItem item,
        int executionId,
        CancellationToken cancellationToken)
    {
        var itemExecutionId = await CreateBatchItemExecutionAsync(executionId, item.BatchItemId);
        var transferId = $"batch-{executionId}-item-{item.BatchItemId}";

        try
        {
            var config = await _profileService.LoadProfileConfigurationAsync(item.ProfileId);
            var transferResult = await _executionService.ExecuteAsync(config, transferId, cancellationToken);

            await UpdateBatchItemExecutionAsync(itemExecutionId, transferResult);

            return new BatchItemExecutionResult
            {
                BatchItemId = item.BatchItemId,
                ProfileName = item.ProfileName,
                Success = transferResult.Success,
                RowsTransferred = transferResult.RowsLoaded,
                ErrorMessage = transferResult.ErrorMessage
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Batch item execution failed: ItemId={ItemId}", item.BatchItemId);

            await UpdateBatchItemExecutionAsync(itemExecutionId, new TransferResult
            {
                Success = false,
                ErrorMessage = ex.Message
            });

            return new BatchItemExecutionResult
            {
                BatchItemId = item.BatchItemId,
                ProfileName = item.ProfileName,
                Success = false,
                ErrorMessage = ex.Message
            };
        }
    }

    public async Task<TransferBatch> CreateBatchAsync(
        string batchName,
        List<int> profileIds,
        BatchExecutionMode executionMode = BatchExecutionMode.Sequential)
    {
        // Create batch and items
        // Assign execution order
        // Save to database
    }

    public async Task<List<TransferBatch>> GetAllBatchesAsync()
    {
        // Load all batches with items
    }

    public async Task<List<BatchExecutionResult>> GetBatchHistoryAsync(int batchId)
    {
        // Load execution history for a batch
    }
}
```

##### 4. UI Components

**Batch Management Page:**
```razor
@page "/batches"

<h1>Transfer Batches</h1>

<button @onclick="ShowCreateBatchDialog" class="btn btn-primary mb-3">
    ➕ Create New Batch
</button>

<div class="row">
    @foreach (var batch in _batches)
    {
        <div class="col-md-6 mb-3">
            <div class="card">
                <div class="card-header">
                    <h5>@batch.BatchName</h5>
                    <small class="text-muted">@batch.Description</small>
                </div>
                <div class="card-body">
                    <div class="mb-2">
                        <strong>Items:</strong> @batch.Items.Count transfers
                    </div>
                    <div class="mb-2">
                        <strong>Mode:</strong>
                        <span class="badge @(batch.ExecutionMode == BatchExecutionMode.Sequential ? "bg-info" : "bg-warning")">
                            @batch.ExecutionMode
                        </span>
                    </div>
                    <div class="mb-3">
                        <strong>Profiles:</strong>
                        <ul class="list-unstyled">
                            @foreach (var item in batch.Items.OrderBy(x => x.ExecutionOrder))
                            {
                                <li>@item.ExecutionOrder. @item.ProfileName</li>
                            }
                        </ul>
                    </div>

                    <div class="btn-group">
                        <button @onclick="() => ExecuteBatch(batch.BatchId)"
                                class="btn btn-success btn-sm"
                                disabled="@_isExecuting">
                            ▶ Execute Now
                        </button>
                        <button @onclick="() => EditBatch(batch)" class="btn btn-primary btn-sm">
                            ✏ Edit
                        </button>
                        <button @onclick="() => ViewHistory(batch.BatchId)" class="btn btn-info btn-sm">
                            📊 History
                        </button>
                        <button @onclick="() => DeleteBatch(batch.BatchId)" class="btn btn-danger btn-sm">
                            🗑 Delete
                        </button>
                    </div>
                </div>
            </div>
        </div>
    }
</div>
```

**Create Batch Dialog:**
```razor
<div class="modal">
    <h3>Create Transfer Batch</h3>

    <div class="form-group">
        <label>Batch Name *</label>
        <input @bind="_batchName" class="form-control"
               placeholder="Daily UAT Sync" />
    </div>

    <div class="form-group">
        <label>Description</label>
        <textarea @bind="_batchDescription" class="form-control"
                  placeholder="Transfers all order-related tables to UAT"></textarea>
    </div>

    <div class="form-group">
        <label>Execution Mode</label>
        <select @bind="_executionMode" class="form-select">
            <option value="@BatchExecutionMode.Sequential">Sequential (one at a time)</option>
            <option value="@BatchExecutionMode.Parallel">Parallel (multiple concurrent)</option>
        </select>
    </div>

    @if (_executionMode == BatchExecutionMode.Parallel)
    {
        <div class="form-group">
            <label>Max Parallelism</label>
            <input type="number" @bind="_maxParallelism" class="form-control"
                   min="1" max="10" value="4" />
            <div class="form-text">Number of concurrent transfers (1-10)</div>
        </div>
    }

    <div class="form-group">
        <label>Select Profiles to Include</label>
        <div class="profile-selector" style="max-height: 300px; overflow-y: auto;">
            @foreach (var profile in _availableProfiles)
            {
                <div class="form-check">
                    <input type="checkbox"
                           checked="@_selectedProfiles.Contains(profile.ProfileId)"
                           @onchange="e => ToggleProfile(profile.ProfileId, e.Value)"
                           class="form-check-input"
                           id="profile-@profile.ProfileId" />
                    <label class="form-check-label" for="profile-@profile.ProfileId">
                        @profile.ProfileName
                        <small class="text-muted d-block">@profile.Description</small>
                    </label>
                </div>
            }
        </div>
    </div>

    @if (_executionMode == BatchExecutionMode.Sequential && _selectedProfiles.Any())
    {
        <div class="form-group">
            <label>Execution Order (drag to reorder)</label>
            <div class="order-list">
                @for (int i = 0; i < _selectedProfilesOrdered.Count; i++)
                {
                    var index = i;
                    <div class="order-item">
                        <span class="order-number">@(i + 1).</span>
                        <span>@_selectedProfilesOrdered[i].ProfileName</span>
                        @if (i > 0)
                        {
                            <button @onclick="() => MoveUp(index)" class="btn btn-sm btn-outline-secondary">
                                ▲
                            </button>
                        }
                        @if (i < _selectedProfilesOrdered.Count - 1)
                        {
                            <button @onclick="() => MoveDown(index)" class="btn btn-sm btn-outline-secondary">
                                ▼
                            </button>
                        }
                    </div>
                }
            </div>
        </div>
    }

    <div class="modal-actions">
        <button @onclick="CreateBatch" class="btn btn-primary">Create Batch</button>
        <button @onclick="Cancel" class="btn btn-secondary">Cancel</button>
    </div>
</div>
```

**Batch Execution Progress:**
```razor
<div class="batch-execution-progress">
    <h4>Executing: @_currentBatch?.BatchName</h4>

    <div class="progress mb-3">
        <div class="progress-bar"
             style="width: @(_executionProgress)%"
             role="progressbar">
            @_completedItems / @_totalItems
        </div>
    </div>

    <div class="execution-log">
        @foreach (var item in _executionLog)
        {
            <div class="log-entry @(item.Success ? "text-success" : "text-danger")">
                <span class="timestamp">[@item.Timestamp.ToString("HH:mm:ss")]</span>
                <span class="status">@(item.Success ? "✓" : "✗")</span>
                <span class="profile">@item.ProfileName</span>
                <span class="rows">@item.RowsTransferred rows</span>
                @if (!string.IsNullOrEmpty(item.ErrorMessage))
                {
                    <span class="error">- @item.ErrorMessage</span>
                }
            </div>
        }
    </div>
</div>
```

##### 5. Testing Strategy

**Unit Tests:**
```csharp
[Fact]
public async Task ExecuteBatch_Sequential_ExecutesInOrder()
{
    // Verify items execute in specified order
}

[Fact]
public async Task ExecuteBatch_Parallel_ExecutesConcurrently()
{
    // Verify multiple items run simultaneously
}

[Fact]
public async Task ExecuteBatch_ContinueOnErrorFalse_StopsOnFailure()
{
    // Verify batch stops when item fails and ContinueOnError=false
}

[Fact]
public async Task ExecuteBatch_ContinueOnErrorTrue_ContinuesAfterFailure()
{
    // Verify batch continues when item fails and ContinueOnError=true
}
```

#### Implementation Steps (TDD)

1. **[RED]** Write test for batch creation
2. **[GREEN]** Implement database schema and models
3. **[GREEN]** Implement TransferBatchService.CreateBatchAsync
4. **[RED]** Write test for sequential execution
5. **[GREEN]** Implement ExecuteSequentiallyAsync
6. **[REFACTOR]** Extract common execution logic
7. **[RED]** Write test for parallel execution
8. **[GREEN]** Implement ExecuteInParallelAsync
9. **[RED]** Write UI component tests
10. **[GREEN]** Implement batch management UI
11. **[GREEN]** Add Playwright E2E test

#### Success Criteria
- ✅ Users can create batches of multiple transfers
- ✅ Batches execute in sequential or parallel mode
- ✅ Individual failures don't stop entire batch (if configured)
- ✅ Progress tracking during execution
- ✅ Execution history tracked per batch
- ✅ 12+ unit tests passing
- ✅ 2+ Playwright tests demonstrating batch execution

---

### Item #28: Email Notifications 📧
**Priority:** HIGH
**Effort:** S (1-2 days)
**Impact:** HIGH - Critical for unattended operations

#### Current State
- No notifications when transfers complete/fail
- Users must manually check history
- No alerts for scheduled transfer failures

#### Proposed Solution

##### 1. Dependencies

```xml
<PackageReference Include="MailKit" Version="4.3.0" />
<PackageReference Include="MimeKit" Version="4.3.0" />
```

##### 2. Configuration

```json
// appsettings.json
{
  "Email": {
    "Smtp": {
      "Host": "smtp.gmail.com",
      "Port": 587,
      "UseSsl": true,
      "Username": "your-email@gmail.com",
      "Password": "your-app-password",
      "FromName": "DataTransfer System",
      "FromAddress": "datatransfer@yourcompany.com"
    },
    "Notifications": {
      "Enabled": true,
      "SendOnSuccess": false,
      "SendOnFailure": true,
      "SendBatchSummaries": true,
      "DefaultRecipients": [
        "devteam@yourcompany.com"
      ]
    }
  }
}
```

##### 3. Models

```csharp
public class EmailSettings
{
    public SmtpSettings Smtp { get; set; } = new();
    public NotificationSettings Notifications { get; set; } = new();
}

public class SmtpSettings
{
    public string Host { get; set; } = string.Empty;
    public int Port { get; set; } = 587;
    public bool UseSsl { get; set; } = true;
    public string Username { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public string FromName { get; set; } = string.Empty;
    public string FromAddress { get; set; } = string.Empty;
}

public class NotificationSettings
{
    public bool Enabled { get; set; } = true;
    public bool SendOnSuccess { get; set; }
    public bool SendOnFailure { get; set; } = true;
    public bool SendBatchSummaries { get; set; } = true;
    public List<string> DefaultRecipients { get; set; } = new();
}
```

##### 4. Service Implementation

```csharp
// src/DataTransfer.Web/Services/EmailNotificationService.cs
using MailKit.Net.Smtp;
using MimeKit;

public class EmailNotificationService
{
    private readonly EmailSettings _settings;
    private readonly ILogger<EmailNotificationService> _logger;

    public EmailNotificationService(
        IOptions<EmailSettings> settings,
        ILogger<EmailNotificationService> logger)
    {
        _settings = settings.Value;
        _logger = logger;
    }

    public async Task SendTransferNotificationAsync(
        TransferResult result,
        TransferConfiguration config,
        List<string>? recipients = null)
    {
        if (!_settings.Notifications.Enabled)
            return;

        if (result.Success && !_settings.Notifications.SendOnSuccess)
            return;

        if (!result.Success && !_settings.Notifications.SendOnFailure)
            return;

        recipients ??= _settings.Notifications.DefaultRecipients;
        if (!recipients.Any())
            return;

        var subject = result.Success
            ? $"✓ Transfer Completed: {config.TransferType}"
            : $"✗ Transfer Failed: {config.TransferType}";

        var body = BuildTransferEmailBody(result, config);

        await SendEmailAsync(recipients, subject, body, isHtml: true);
    }

    public async Task SendBatchNotificationAsync(
        BatchExecutionResult result,
        TransferBatch batch,
        List<string>? recipients = null)
    {
        if (!_settings.Notifications.Enabled || !_settings.Notifications.SendBatchSummaries)
            return;

        recipients ??= _settings.Notifications.DefaultRecipients;
        if (!recipients.Any())
            return;

        var subject = result.Status == "Completed"
            ? $"✓ Batch Completed: {batch.BatchName}"
            : $"⚠ Batch {result.Status}: {batch.BatchName}";

        var body = BuildBatchEmailBody(result, batch);

        await SendEmailAsync(recipients, subject, body, isHtml: true);
    }

    public async Task SendScheduleFailureAlertAsync(
        ScheduleInfo schedule,
        string errorMessage,
        List<string>? recipients = null)
    {
        if (!_settings.Notifications.Enabled)
            return;

        recipients ??= _settings.Notifications.DefaultRecipients;
        if (!recipients.Any())
            return;

        var subject = $"🚨 Scheduled Transfer Failed: {schedule.ScheduleName}";
        var body = BuildScheduleFailureEmailBody(schedule, errorMessage);

        await SendEmailAsync(recipients, subject, body, isHtml: true);
    }

    private async Task SendEmailAsync(
        List<string> recipients,
        string subject,
        string body,
        bool isHtml = false)
    {
        try
        {
            var message = new MimeMessage();
            message.From.Add(new MailboxAddress(
                _settings.Smtp.FromName,
                _settings.Smtp.FromAddress));

            foreach (var recipient in recipients)
            {
                message.To.Add(MailboxAddress.Parse(recipient));
            }

            message.Subject = subject;
            message.Body = new TextPart(isHtml ? "html" : "plain")
            {
                Text = body
            };

            using var client = new SmtpClient();
            await client.ConnectAsync(
                _settings.Smtp.Host,
                _settings.Smtp.Port,
                _settings.Smtp.UseSsl);

            await client.AuthenticateAsync(
                _settings.Smtp.Username,
                _settings.Smtp.Password);

            await client.SendAsync(message);
            await client.DisconnectAsync(true);

            _logger.LogInformation("Email sent successfully: {Subject}", subject);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send email: {Subject}", subject);
            // Don't throw - email failure shouldn't stop transfers
        }
    }

    private string BuildTransferEmailBody(TransferResult result, TransferConfiguration config)
    {
        return $@"
            <html>
            <body style='font-family: Arial, sans-serif;'>
                <div style='background-color: {(result.Success ? "#d4edda" : "#f8d7da")};
                            color: {(result.Success ? "#155724" : "#721c24")};
                            padding: 15px; border-radius: 5px; margin-bottom: 20px;'>
                    <h2 style='margin: 0;'>
                        {(result.Success ? "✓ Transfer Completed Successfully" : "✗ Transfer Failed")}
                    </h2>
                </div>

                <table style='width: 100%; border-collapse: collapse;'>
                    <tr>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd;'><strong>Transfer Type:</strong></td>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd;'>{config.TransferType}</td>
                    </tr>
                    <tr>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd;'><strong>Source:</strong></td>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd;'>{GetSourceDescription(config)}</td>
                    </tr>
                    <tr>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd;'><strong>Destination:</strong></td>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd;'>{GetDestinationDescription(config)}</td>
                    </tr>
                    <tr>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd;'><strong>Start Time:</strong></td>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd;'>{result.StartTime:yyyy-MM-dd HH:mm:ss}</td>
                    </tr>
                    <tr>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd;'><strong>Duration:</strong></td>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd;'>{result.Duration.TotalSeconds:F2} seconds</td>
                    </tr>
                    <tr>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd;'><strong>Rows Transferred:</strong></td>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd;'>{result.RowsLoaded:N0}</td>
                    </tr>
                    {(string.IsNullOrEmpty(result.ErrorMessage) ? "" : $@"
                    <tr>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd;'><strong>Error:</strong></td>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd; color: #d9534f;'>{result.ErrorMessage}</td>
                    </tr>
                    ")}
                </table>

                <p style='margin-top: 20px; color: #666;'>
                    This is an automated notification from the DataTransfer system.
                </p>
            </body>
            </html>
        ";
    }

    private string BuildBatchEmailBody(BatchExecutionResult result, TransferBatch batch)
    {
        var itemsHtml = string.Join("", result.ItemResults.Select(item => $@"
            <tr style='background-color: {(item.Success ? "#f0f8ff" : "#fff0f0")};'>
                <td style='padding: 8px; border-bottom: 1px solid #ddd;'>{item.ProfileName}</td>
                <td style='padding: 8px; border-bottom: 1px solid #ddd;'>
                    {(item.Success ? "✓ Success" : "✗ Failed")}
                </td>
                <td style='padding: 8px; border-bottom: 1px solid #ddd;'>{item.RowsTransferred:N0}</td>
                <td style='padding: 8px; border-bottom: 1px solid #ddd;'>{item.ErrorMessage}</td>
            </tr>
        "));

        return $@"
            <html>
            <body style='font-family: Arial, sans-serif;'>
                <h2>{batch.BatchName} - Execution Summary</h2>

                <div style='background-color: #e7f3ff; padding: 15px; border-radius: 5px; margin-bottom: 20px;'>
                    <h3 style='margin: 0 0 10px 0;'>Overall Status: {result.Status}</h3>
                    <p style='margin: 5px 0;'><strong>Total Items:</strong> {result.TotalItems}</p>
                    <p style='margin: 5px 0;'><strong>Completed:</strong> {result.CompletedItems}</p>
                    <p style='margin: 5px 0;'><strong>Failed:</strong> {result.FailedItems}</p>
                    <p style='margin: 5px 0;'><strong>Total Rows Transferred:</strong> {result.TotalRowsTransferred:N0}</p>
                    <p style='margin: 5px 0;'><strong>Duration:</strong> {(result.EndTime - result.StartTime).Value.TotalMinutes:F1} minutes</p>
                </div>

                <h3>Individual Transfer Results:</h3>
                <table style='width: 100%; border-collapse: collapse;'>
                    <thead>
                        <tr style='background-color: #f0f0f0;'>
                            <th style='padding: 8px; text-align: left; border-bottom: 2px solid #ddd;'>Profile</th>
                            <th style='padding: 8px; text-align: left; border-bottom: 2px solid #ddd;'>Status</th>
                            <th style='padding: 8px; text-align: left; border-bottom: 2px solid #ddd;'>Rows</th>
                            <th style='padding: 8px; text-align: left; border-bottom: 2px solid #ddd;'>Error</th>
                        </tr>
                    </thead>
                    <tbody>
                        {itemsHtml}
                    </tbody>
                </table>

                <p style='margin-top: 20px; color: #666;'>
                    This is an automated notification from the DataTransfer system.
                </p>
            </body>
            </html>
        ";
    }

    private string BuildScheduleFailureEmailBody(ScheduleInfo schedule, string errorMessage)
    {
        return $@"
            <html>
            <body style='font-family: Arial, sans-serif;'>
                <div style='background-color: #f8d7da; color: #721c24; padding: 15px; border-radius: 5px; margin-bottom: 20px;'>
                    <h2 style='margin: 0;'>🚨 Scheduled Transfer Failed</h2>
                </div>

                <table style='width: 100%; border-collapse: collapse;'>
                    <tr>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd;'><strong>Schedule Name:</strong></td>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd;'>{schedule.ScheduleName}</td>
                    </tr>
                    <tr>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd;'><strong>Profile:</strong></td>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd;'>{schedule.ProfileName}</td>
                    </tr>
                    <tr>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd;'><strong>Cron Schedule:</strong></td>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd;'><code>{schedule.CronExpression}</code></td>
                    </tr>
                    <tr>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd;'><strong>Failed At:</strong></td>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd;'>{DateTime.Now:yyyy-MM-dd HH:mm:ss}</td>
                    </tr>
                    <tr>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd;'><strong>Error:</strong></td>
                        <td style='padding: 8px; border-bottom: 1px solid #ddd; color: #d9534f;'>{errorMessage}</td>
                    </tr>
                </table>

                <div style='margin-top: 20px; padding: 15px; background-color: #fff3cd; border-radius: 5px;'>
                    <p style='margin: 0;'><strong>Action Required:</strong></p>
                    <p style='margin: 5px 0 0 0;'>Please review the schedule and resolve the issue to prevent future failures.</p>
                </div>

                <p style='margin-top: 20px; color: #666;'>
                    This is an automated alert from the DataTransfer system.
                </p>
            </body>
            </html>
        ";
    }
}
```

##### 5. Integration Points

Update existing services to send notifications:

```csharp
// In TransferExecutionService.ExecuteAsync:
var result = await _orchestrator.ExecuteTransferAsync(config, cancellationToken);

// Add notification
await _emailService.SendTransferNotificationAsync(result, config);

return result;

// In TransferBatchService.ExecuteBatchAsync:
var result = await ExecuteBatch(...);

// Add notification
await _emailService.SendBatchNotificationAsync(result, batch);

return result;

// In TransferExecutionJob.Execute (Quartz):
try
{
    var result = await _executionService.ExecuteAsync(config, transferId, context.CancellationToken);

    if (!result.Success)
    {
        await _emailService.SendScheduleFailureAlertAsync(schedule, result.ErrorMessage);
    }
}
```

##### 6. Testing Strategy

**Unit Tests:**
```csharp
[Fact]
public async Task SendTransferNotification_Success_SendsEmail()
{
    // Mock SMTP
    // Verify email sent with correct subject and body
}

[Fact]
public async Task SendTransferNotification_Failure_SendsErrorEmail()
{
    // Verify failure email includes error message
}

[Fact]
public async Task SendTransferNotification_DisabledInSettings_DoesNotSend()
{
    // Verify email not sent when Enabled=false
}
```

#### Implementation Steps (TDD)

1. **[RED]** Write test for email sending
2. **[GREEN]** Add MailKit dependencies
3. **[GREEN]** Implement EmailNotificationService
4. **[REFACTOR]** Extract email template logic
5. **[GREEN]** Add configuration support
6. **[GREEN]** Integrate with TransferExecutionService
7. **[GREEN]** Integrate with batch and schedule services
8. **[GREEN]** Add unit tests

#### Success Criteria
- ✅ Email notifications sent on transfer completion/failure
- ✅ Batch summary emails with item-level details
- ✅ Schedule failure alerts
- ✅ Configurable recipients per profile/schedule
- ✅ HTML-formatted professional emails
- ✅ 8+ unit tests passing
- ✅ SMTP configuration documented

---

## Phase 1 Summary

### Total Effort
- **Item #2 (Transfer Profiles):** 3-5 days
- **Item #34 (Save Configurations):** Included in #2
- **Item #1 (Scheduled Transfers):** 7-10 days
- **Item #3 (Background Jobs):** Included in #1
- **Item #4 (Batch Operations):** 3-5 days
- **Item #28 (Email Notifications):** 1-2 days

**Total Phase 1:** 14-22 days (~3-4 weeks with testing)

### Key Deliverables
1. Persistent transfer profiles with UI management
2. Cron-based scheduling with Quartz.NET
3. Batch/bulk transfer execution (sequential and parallel)
4. Email notifications for all transfer events
5. Comprehensive test coverage (50+ new tests)
6. Database migrations and schema updates
7. Updated documentation

### Dependencies
- SQL Server for persistence (Profiles, Schedules, Batches)
- SMTP server for email notifications
- Quartz.NET for job scheduling

### Risks & Mitigations
1. **Risk:** Quartz.NET learning curve
   - **Mitigation:** Start with simple schedules, add complexity incrementally

2. **Risk:** Database schema changes in production
   - **Mitigation:** Idempotent migration scripts, test thoroughly

3. **Risk:** Email delivery issues
   - **Mitigation:** Comprehensive logging, don't fail transfers on email errors

4. **Risk:** Parallel execution concurrency bugs
   - **Mitigation:** Thorough testing with concurrent batches, use thread-safe collections

### Next Steps After Phase 1
- Phase 2: Safety & Reliability (data filtering, validation, rollback)
- Phase 3: Performance (incremental transfers, streaming)
- Phase 4: Advanced features (data masking, comparison tools)

---

**Document Version:** 1.0
**Created:** 2025-10-04
**Last Updated:** 2025-10-04
