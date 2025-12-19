# Development Workflow for Pygen v2

This document outlines the development workflow for Pygen v2 during the parallel v1/v2 development period.

## Project Structure

```
pygen/
├── cognite/pygen/        # V2 source code (active development)
│   ├── client/           # HTTP client for CDF API
│   ├── ir/               # Intermediate representation
│   ├── generators/       # Code generators
│   ├── validation/       # Validation layer
│   ├── runtime/          # Runtime support for generated SDKs
│   └── utils/            # Utilities
├── tests/                # V2 tests
│   ├── unit/            # V2 unit tests
│   ├── integration/     # V2 integration tests
│   └── fixtures/        # Test fixtures
├── examples/            # V2 generated examples
├── legacy/              # V1 code (preserved, read-only)
│   ├── cognite/pygen/   # V1 source code
│   ├── tests/           # V1 tests
│   └── examples/        # V1 examples
└── docs/
    ├── v2/              # V2-specific documentation
    └── ...              # Shared/v1 documentation
```

## Running Tests

### V2 Tests Only
```bash
# Unix/Linux/macOS
./scripts/test-v2.sh

# Windows PowerShell
.\scripts\test-v2.ps1

# Or directly with pytest
pytest tests/
```

### V1 Tests Only (Legacy)
```bash
# Unix/Linux/macOS
./scripts/test-v1.sh

# Windows PowerShell
.\scripts\test-v1.ps1

# Or directly with pytest
pytest legacy/tests/
```

### All Tests (V1 + V2)
```bash
# Unix/Linux/macOS
./scripts/test-all.sh

# Windows PowerShell
.\scripts\test-all.ps1

# Or directly with pytest
pytest
```

## Development Guidelines

### For V2 Development

1. **All new code goes in `cognite/pygen/`** (not in `legacy/`)
2. **Write tests in `tests/`** following the v2 structure
3. **Follow the phase roadmap** in `plan/implementation-roadmap.md`
4. **Run linters and type checkers**:
   ```bash
   ruff format .
   ruff check .
   mypy cognite/
   ```
5. **Update documentation** in `docs/v2/` as you go

### For V1 Maintenance

1. **Avoid modifying legacy code** unless fixing critical bugs
2. **If fixing v1 bugs**, make changes in `legacy/` and clearly document
3. **V1 tests should continue passing** throughout v2 development

## CI/CD Pipeline

The CI/CD pipeline runs:
- ✅ Linting (ruff) on v2 code only
- ✅ Type checking (mypy) on v2 code only  
- ✅ V2 unit and integration tests
- ✅ V1 tests to ensure backward compatibility
- ✅ Coverage reporting for v2

## Commit Guidelines

- Use clear, descriptive commit messages
- Prefix commits with the phase: `[Phase 0]`, `[Phase 1]`, etc.
- Reference issues when applicable: `[Phase 1] Implement HTTP client #123`

## Pull Request Guidelines

1. Ensure all tests pass (both v1 and v2)
2. Maintain or improve code coverage (>90% target)
3. Update documentation if adding features
4. Add or update tests for new functionality
5. Run pre-commit hooks before submitting

## Pre-commit Hooks

Pre-commit hooks are configured to:
- Run `uv-lock` to keep dependencies in sync
- Run `ruff` formatting and linting
- Run `mypy` type checking (v2 only)

Install pre-commit hooks:
```bash
pre-commit install
```

Run hooks manually:
```bash
pre-commit run --all-files
```

## Version Management

- **V2 version**: `2.0.0-dev` (during development)
- **V1 version**: Frozen at last stable release
- Package version in `pyproject.toml` remains `0.0.0` during development

## Timeline

Phase 0 is estimated at **1 week**. After Phase 0:
- V2 structure is ready
- V1 tests still pass
- CI/CD supports both versions
- Development can proceed to Phase 1

## Questions?

Refer to:
- `plan/implementation-roadmap.md` for overall roadmap
- `docs/v2/` for v2-specific documentation
- `legacy/README.md` for v1 preservation details

