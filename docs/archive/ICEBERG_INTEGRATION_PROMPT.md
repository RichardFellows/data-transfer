# Iceberg Integration - Session Initialization Prompt

## Context Setup

I'm working on integrating Apache Iceberg table format into an existing .NET 8 DataTransfer application that currently exports SQL Server data to Parquet files. The full implementation plan is documented in `docs/ICEBERG_INTEGRATION_PLAN.md`.

## Project Overview

**Current Application**: .NET 8 solution that transfers data between SQL Server instances using Parquet as intermediate storage, with support for partitioned tables, SCD2 tables, and static tables.

**Goal**: Add support for exporting SQL Server data to Apache Iceberg tables (Parquet-backed with ACID transactions) using a filesystem catalog.

## Key Technical Requirements

1. **Library Migration**: Replace `Parquet.Net` with `ParquetSharp` to support Iceberg field-id metadata
2. **Critical Workaround**: Custom Avro schema wrappers required (Apache.Avro strips field-id attributes during serialization)
3. **Architecture**: Iceberg three-tier metadata (Table Metadata JSON → Manifest List Avro → Manifest Avro → Parquet Data)
4. **Validation**: Must be readable by PyIceberg and queryable by DuckDB

## Implementation Approach

**Methodology**: Strict TDD workflow (RED → GREEN → REFACTOR)

**Commit Format**:
```
<type>(<scope>): <description> [TDD_PHASE]

Examples:
feat(iceberg): add IcebergSchema model [RED]
feat(iceberg): implement field-id assignment logic [GREEN]
refactor(iceberg): optimize type mapping [REFACTOR]
```

## Starting Point

We're beginning at **Phase 1: Core Iceberg Infrastructure**.

Please start by:

1. **Review** the full plan in `docs/ICEBERG_INTEGRATION_PLAN.md`
2. **Create** the `src/DataTransfer.Iceberg/` project with NuGet packages:
   - ParquetSharp (14.0.1)
   - Apache.Avro (1.11.3)
3. **Begin Phase 1.1** (RED): Create failing tests for IcebergSchema model in `tests/DataTransfer.Core.Tests/Models/Iceberg/IcebergSchemaTests.cs`

## Key Files to Reference

- Implementation Plan: `docs/ICEBERG_INTEGRATION_PLAN.md`
- Gemini Guide: `gemini_-gemini-sql-to-iceberg-parquet-demo-plan_2025-10-05T21-05-25+0100.md`
- Project Instructions: `CLAUDE.md`
- Existing Architecture: Review `src/DataTransfer.Core/Strategies/PartitionStrategy.cs` for integration pattern

## Success Criteria for Phase 1

- ✅ IcebergSchema model with field-id support
- ✅ SQL Server → Iceberg type mapping
- ✅ All tests passing (80%+ coverage)
- ✅ Committed with TDD phase annotations

## Important Notes

- **DO NOT** skip the RED phase - write failing tests first
- **DO** commit after each TDD phase (red, green, refactor)
- **DO** use the existing test infrastructure (TestContainers for SQL Server)
- **DO** maintain existing functionality (adapter pattern for Parquet project)

## Next Steps After Phase 1

Once Phase 1 is complete and committed:
1. Move to Phase 2: ParquetSharp Integration
2. Implement IcebergParquetWriter with GroupNode API
3. Continue through phases as documented in the plan

---

**Ready to begin? Start with Phase 1.1: Create failing tests for IcebergSchema.**
