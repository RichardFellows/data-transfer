# Implementation Plan: Dynamic Table Selection UI

## Overview
Add database-driven dropdown for table selection in the web UI, replacing manual text input with actual tables from connected SQL Server databases.

## Current State Analysis
- **NewTransfer.razor**: Uses text inputs for Database, Schema, and Table
- **Connection strings**: Manually entered each time
- **No validation**: Users can mistype table names
- **No discovery**: Can't see available tables

## Proposed Solution

### 1. Configuration Management (appsettings.json)
**File**: `src/DataTransfer.Web/appsettings.json`

Add default connection string presets:
```json
{
  "ConnectionStrings": {
    "LocalDemo": "Server=localhost,1433;User Id=sa;Password=YourStrong@Passw0rd;TrustServerCertificate=true",
    "LocalIntegrated": "Server=localhost;Integrated Security=true;TrustServerCertificate=true"
  },
  "DefaultDatabases": ["SalesSource", "SalesDestination", "master"]
}
```

### 2. New Service: DatabaseMetadataService
**File**: `src/DataTransfer.Web/Services/DatabaseMetadataService.cs`

**Responsibilities**:
- Query SQL Server metadata (INFORMATION_SCHEMA.TABLES)
- Cache table lists per connection string
- Provide database/schema/table dropdowns data

**Key Methods**:
```csharp
Task<List<string>> GetDatabasesAsync(string connectionString)
Task<List<string>> GetSchemasAsync(string connectionString, string database)
Task<List<TableInfo>> GetTablesAsync(string connectionString, string database, string schema)
bool TestConnection(string connectionString)
```

**SQL Queries**:
- Databases: `SELECT name FROM sys.databases WHERE database_id > 4 ORDER BY name`
- Schemas: `SELECT DISTINCT TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = @database ORDER BY TABLE_SCHEMA`
- Tables: `SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = @database AND TABLE_SCHEMA = @schema ORDER BY TABLE_NAME`

### 3. UI Enhancement: NewTransfer.razor
**Changes**:
1. **Connection String Presets Dropdown**
   - Dropdown with named presets from appsettings
   - "Custom" option to show text input

2. **Database Dropdown (Cascading)**
   - Loads when connection is valid
   - Includes default databases + discovered ones
   - Shows loading spinner while querying

3. **Schema Dropdown (Cascading)**
   - Loads after database selected
   - Pre-selects "dbo" if available

4. **Table Dropdown (Cascading)**
   - Loads after schema selected
   - Shows table type (TABLE/VIEW) as badge
   - Includes search/filter capability

5. **Connection Test Button**
   - Validates connection before enabling dropdowns
   - Shows success/error feedback

### 4. New Model: TableInfo
**File**: `src/DataTransfer.Web/Models/TableInfo.cs`

```csharp
public class TableInfo
{
    public string Name { get; set; }
    public string Type { get; set; } // TABLE or VIEW
    public string FullName => $"{Name} ({Type})";
}
```

### 5. Configuration Model Enhancement
**File**: `src/DataTransfer.Web/Models/ConnectionPreset.cs`

```csharp
public class ConnectionPreset
{
    public string Name { get; set; }
    public string ConnectionString { get; set; }
}
```

## Implementation Phases

### Phase 1: Backend Infrastructure [TDD]
1. Create `DatabaseMetadataService` with tests
2. Add `TableInfo` and `ConnectionPreset` models
3. Configure appsettings.json with presets
4. Register service in Program.cs
5. **Tests**:
   - Connection string validation
   - Metadata queries against test database
   - Caching behavior

### Phase 2: UI Components [TDD]
1. Add connection preset dropdown to NewTransfer.razor
2. Replace text inputs with cascading dropdowns
3. Add connection test button and feedback
4. Add loading states and error handling
5. **Tests**:
   - Playwright tests for dropdown interactions
   - Test cascading behavior
   - Test connection validation flow

### Phase 3: UX Enhancements
1. Add table search/filter in dropdown
2. Add "recently used" connections
3. Add connection string save to localStorage
4. Add table preview (row count, columns)
5. **Tests**:
   - Playwright tests for search functionality
   - Test localStorage persistence

### Phase 4: Polish & Documentation
1. Update demo scripts to showcase new UI
2. Add README section for connection presets
3. Add inline help text and tooltips
4. Performance optimization (debouncing, caching)

## Technical Considerations

### Security
- **Never log passwords**: Sanitize connection strings in logs
- **SQL Injection**: Use parameterized queries for all metadata queries
- **Connection pooling**: Reuse connections, proper disposal

### Performance
- **Caching**: Cache metadata for 5 minutes per connection
- **Debouncing**: 500ms delay on connection string changes
- **Lazy loading**: Only load next dropdown after previous selected

### Error Handling
- Network timeouts (5 second default)
- Invalid credentials (clear error message)
- Database not found (helpful suggestion)
- No tables found (show empty state with instructions)

## Files to Create/Modify

### New Files (6)
1. `src/DataTransfer.Web/Services/DatabaseMetadataService.cs`
2. `src/DataTransfer.Web/Models/TableInfo.cs`
3. `src/DataTransfer.Web/Models/ConnectionPreset.cs`
4. `tests/DataTransfer.Web.Tests/DatabaseMetadataServiceTests.cs`
5. `tests/DataTransfer.Web.Tests/NewTransferDropdownTests.cs`
6. `docs/UI_TABLE_SELECTION.md`

### Modified Files (4)
1. `src/DataTransfer.Web/Components/Pages/NewTransfer.razor` (major refactor)
2. `src/DataTransfer.Web/appsettings.json` (add presets)
3. `src/DataTransfer.Web/Program.cs` (register service)
4. `tests/DataTransfer.Web.Tests/WebUITests.cs` (update existing tests)

## Estimated Effort
- **Phase 1**: 3-4 hours (backend + tests)
- **Phase 2**: 4-5 hours (UI + Playwright tests)
- **Phase 3**: 2-3 hours (enhancements)
- **Phase 4**: 1-2 hours (documentation)
- **Total**: 10-14 hours

## Success Criteria
- ✅ Users can select from preset connections
- ✅ Tables load dynamically from selected database
- ✅ Cascading dropdowns work smoothly
- ✅ Connection test provides clear feedback
- ✅ All existing Playwright tests still pass
- ✅ New tests cover dropdown interactions
- ✅ No manual table name typing required (unless custom connection)

## Risks & Mitigations
- **Risk**: Slow metadata queries on large databases
  - **Mitigation**: Add timeout, pagination, caching
- **Risk**: Breaking existing functionality
  - **Mitigation**: TDD approach, comprehensive Playwright tests
- **Risk**: Complex UI state management
  - **Mitigation**: Use Blazor's built-in state management, clear component lifecycle
