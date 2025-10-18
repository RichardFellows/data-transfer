# Documentation Guide

This document provides a guide to all documentation in the DataTransfer project.

## Quick Links

### Getting Started
- **[README.md](README.md)** - Project overview, features, and quick start
- **[GETTING_STARTED.md](GETTING_STARTED.md)** - Step-by-step setup guide with SQL Server configuration
- **[QUICK_START.md](QUICK_START.md)** - Quick reference for common operations

### User Guides
- **[COMMAND_REFERENCE.md](COMMAND_REFERENCE.md)** - Complete command reference mapping docs to implementations
- **[docs/ICEBERG_QUICKSTART.md](docs/ICEBERG_QUICKSTART.md)** - Iceberg integration guide with examples
- **[docs/SCHEMA_DISCOVERY_TEST_GUIDE.md](docs/SCHEMA_DISCOVERY_TEST_GUIDE.md)** - Testing schema discovery features
- **[docs/iceberg-validation-guide.md](docs/iceberg-validation-guide.md)** - Validating Iceberg table operations

### Technical Documentation
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - System architecture and design patterns
- **[CONSOLE_APP_SPEC.md](CONSOLE_APP_SPEC.md)** - Console application specification
- **[TEST_COVERAGE_SUMMARY.md](TEST_COVERAGE_SUMMARY.md)** - Test coverage information

### Project Configuration
- **[CLAUDE.md](CLAUDE.md)** - Instructions for Claude Code (AI assistant)
- **[requirements.md](requirements.md)** - Project requirements

### Demo Documentation
- **[demo/README.md](demo/README.md)** - Demo scripts overview
- **[demo/ICEBERG_DEMO_README.md](demo/ICEBERG_DEMO_README.md)** - Iceberg feature demonstrations
- **[demo/INCREMENTAL_SYNC_README.md](demo/INCREMENTAL_SYNC_README.md)** - Incremental sync demonstrations

## Archived Documentation

Historical implementation planning and analysis documents have been moved to **[docs/archive/](docs/archive/)** to keep the repository organized. These include:

- Implementation plans (Bidirectional transfers, UI table selection, Iceberg integration)
- Analysis documents (Incremental sync analysis, investigation summaries)
- Development prompts and continuation guides
- Historical status documents

See **[docs/archive/README.md](docs/archive/README.md)** for a complete list and descriptions.

## Documentation Structure

```
/
├── README.md                          # Main project documentation
├── GETTING_STARTED.md                 # Setup guide
├── QUICK_START.md                     # Quick reference
├── COMMAND_REFERENCE.md               # Complete command reference
├── ARCHITECTURE.md                    # Technical architecture
├── CONSOLE_APP_SPEC.md                # Console app specification
├── TEST_COVERAGE_SUMMARY.md           # Test coverage info
├── CLAUDE.md                          # AI assistant instructions
├── requirements.md                    # Project requirements
│
├── docs/
│   ├── ICEBERG_QUICKSTART.md          # Iceberg user guide
│   ├── SCHEMA_DISCOVERY_TEST_GUIDE.md # Schema discovery testing
│   ├── iceberg-validation-guide.md    # Iceberg validation
│   │
│   └── archive/                       # Historical documents
│       ├── README.md                  # Archive index
│       └── [implementation plans...]
│
└── demo/
    ├── README.md                      # Demo overview
    ├── ICEBERG_DEMO_README.md         # Iceberg demos
    └── INCREMENTAL_SYNC_README.md     # Incremental sync demos
```

## Contributing to Documentation

When adding new documentation:

1. **User-facing guides** → Root directory or `docs/`
2. **Implementation plans** → `docs/archive/` (for historical reference)
3. **Demo-specific docs** → `demo/` directory
4. **Test-specific docs** → `tests/[project]/` directory

Keep documentation:
- Clear and concise
- With practical examples
- Up-to-date with code changes
- Properly cross-referenced
