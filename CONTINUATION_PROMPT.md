# Continuation Prompt for New Context

Copy and paste this into a new Claude Code session:

---

I'm continuing work on a .NET 8 data transfer solution. The project is **~80% complete** with **111 tests passing** across all layers including E2E integration tests.

**Current Status:**
- ✅ Core layers (Core, Configuration, SqlServer, Parquet, Pipeline): 106 unit tests passing
- ✅ Console application: Complete with DI, logging, configuration
- ✅ Integration tests: 5 E2E tests with Testcontainers + Respawn (optimized: ~19s execution)
- 🔨 Docker deployment: NEXT TASK

**IMMEDIATE TASK: Update Docker Deployment**

1. **Read context files (in order):**
   - `QUICK_START.md` - Current status and quick reference
   - `IMPLEMENTATION_STATUS.md` - Full project context
   - `docker/Dockerfile` - Existing file (needs .NET 8 update)

2. **Update `docker/Dockerfile` for .NET 8:**
   - Multi-stage build with .NET 8 SDK
   - Use UBI8 runtime or .NET 8 runtime
   - Configure volumes: `/config`, `/parquet-output`, `/logs`
   - Ensure all project references are correct
   - ENTRYPOINT: `dotnet DataTransfer.Console.dll`

3. **Test:**
   ```bash
   docker build -f docker/Dockerfile -t datatransfer:latest .
   docker run -v $(pwd)/config:/config -v $(pwd)/output:/parquet-output datatransfer:latest
   ```

4. **Commit:**
   Follow TDD format from `CLAUDE.md`:
   ```
   feat(docker): update Dockerfile for .NET 8 [GREEN]

   Updated Docker deployment:
   - .NET 8 SDK and runtime
   - Multi-stage build for smaller image
   - Configured volumes for config, output, logs
   - Tested build and run successfully

   🤖 Generated with [Claude Code](https://claude.com/claude-code)

   Co-Authored-By: Claude <noreply@anthropic.com>
   ```

**Key Points:**
- 111 tests passing - don't break them!
- All dependencies already configured
- Follow TDD strictly: RED → GREEN → REFACTOR
- Integration tests optimized: shared container + Respawn (57% faster)
- Project follows layered architecture: Console → Pipeline → (SqlServer + Parquet) → Core

**After Docker is complete:**
- Update README.md with comprehensive documentation
- Optional: Add performance benchmarks with BenchmarkDotNet

**Git Status:**
```
Branch: main
Last commits:
- 5f87562 docs: update project status to reflect integration tests completion
- e0201ab perf(integration): optimize tests with shared container and Respawn [REFACTOR]
- b5f4b6b test(integration): add end-to-end tests with Testcontainers [GREEN]
```

**Important Files:**
- `CLAUDE.md` - Project instructions (TDD workflow, commit format)
- `QUICK_START.md` - Quick reference guide
- `IMPLEMENTATION_STATUS.md` - Detailed status and remaining work
- `ARCHITECTURE.md` - Technical architecture
- `docker/Dockerfile` - Target file for this task

Start by reading `QUICK_START.md` to get oriented!
