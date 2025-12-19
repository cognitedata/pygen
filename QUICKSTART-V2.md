# Pygen v2 Quick Start Guide

## Current Status

**Phase 0: Foundation & Setup** ✅ **COMPLETE**

All Phase 0 deliverables have been met. The project is ready for Phase 1 development.

## Project Structure

```
pygen/
├── cognite/pygen/       # V2 source code (active development)
├── tests/               # V2 tests
├── legacy/              # V1 code (preserved, read-only)
├── docs/v2/             # V2 documentation
└── scripts/             # Helper scripts
```

## Quick Commands

### Running Tests

```bash
# V2 tests only
pytest tests/

# V1 tests only (legacy)
pytest legacy/tests/

# All tests
pytest

# With coverage
pytest tests/ --cov=cognite.pygen
```

### Code Quality

```bash
# Format code
ruff format .

# Check linting
ruff check .

# Type checking
mypy cognite/

# All pre-commit hooks
pre-commit run --all-files
```

### Validation

```bash
# Validate Phase 0 completion
python scripts/validate-phase0.py
```

## Development Workflow

1. **Check the roadmap**: See `plan/implementation-roadmap.md`
2. **Create a branch**: For your feature/phase work
3. **Write tests first**: Test-driven development
4. **Implement**: Follow architecture in `docs/v2/architecture.md`
5. **Run tests**: Ensure both v1 and v2 tests pass
6. **Format & lint**: Run `ruff format .` and `mypy cognite/`
7. **Commit**: Use clear messages with phase prefix
8. **Create PR**: For review

## Key Files

- **Implementation Plan**: `plan/implementation-roadmap.md`
- **Architecture**: `docs/v2/architecture.md`
- **Development Guide**: `DEVELOPMENT.md`
- **Workflow Guide**: `docs/v2/development-workflow.md`
- **Phase 0 Report**: `plan/PHASE0-COMPLETION.md`

## Next Phase

**Phase 1: Pygen Client Core**
- Duration: 3-4 weeks
- Goal: Build HTTP client for CDF Data Modeling API
- See `plan/implementation-roadmap.md` section "Phase 1" for details

## Getting Help

- Check documentation in `docs/v2/`
- Review implementation roadmap
- Look at examples (coming in later phases)

## Important Notes

- ✅ V2 is in active development
- ✅ V1 code is in `legacy/` (preserved, read-only)
- ✅ Both can coexist during development
- ✅ V1 will be removed after v2.0.0 release

---

Last Updated: December 19, 2025
Phase 0 Status: ✅ COMPLETE

