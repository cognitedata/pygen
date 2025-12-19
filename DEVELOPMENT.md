# Pygen Development Guide

This guide is for developers working on Pygen itself (not users generating SDKs).

## Project Status

**Currently in Phase 0 of the v2 rewrite.**

Pygen v1 is preserved in `legacy/` while v2 is being developed in the root directories.

## Quick Start

### Prerequisites
- Python 3.10+
- uv (recommended) or pip

### Setup
```bash
# Clone the repository
git clone https://github.com/cognitedata/pygen.git
cd pygen

# Install dependencies (using uv)
uv sync --all-extras

# Or using pip
pip install -e ".[cli,format]" -r <(uv export --frozen --no-hashes --only-dependencies)

# Install pre-commit hooks
pre-commit install
```

### Running Tests

```bash
# V2 tests only
pytest tests/

# V1 tests only
pytest legacy/tests/

# All tests
pytest

# With coverage
pytest --cov=cognite.pygen --cov-report=html
```

### Code Quality

```bash
# Format code
ruff format .

# Lint code
ruff check .

# Type check
mypy cognite/

# Run all pre-commit hooks
pre-commit run --all-files
```

## Project Structure

See [Development Workflow](docs/v2/development-workflow.md) for detailed structure.

```
pygen/
├── cognite/pygen/        # V2 source code
├── tests/                # V2 tests
├── legacy/               # V1 code (read-only)
├── docs/                 # Documentation
├── plan/                 # Implementation roadmap
└── scripts/              # Development scripts
```

## Development Workflow

1. **Check the roadmap**: See `plan/implementation-roadmap.md`
2. **Create a branch**: `git checkout -b phase-X-feature-name`
3. **Write tests**: Test-driven development is encouraged
4. **Implement**: Follow the architecture in `docs/v2/architecture.md`
5. **Run tests**: Ensure both v1 and v2 tests pass
6. **Run linters**: `ruff format .` and `mypy cognite/`
7. **Commit**: Use clear commit messages with phase prefix
8. **Push and PR**: Create a pull request for review

## Phase Guidelines

### Phase 0: Foundation (Current)
- ✅ Project reorganization
- ✅ Dual v1/v2 structure
- ✅ CI/CD updates
- ✅ Documentation structure

### Phase 1: Pygen Client Core (Next)
- HTTP client implementation
- API resource clients
- Query builder
- Error handling

See `plan/implementation-roadmap.md` for complete phase details.

## Contributing

1. Read [CONTRIBUTING.md](CONTRIBUTING.md)
2. Follow the [Code of Conduct](CODE_OF_CONDUCT.md)
3. Check open issues and PRs
4. Ask questions in discussions

## Testing Guidelines

- Aim for >90% test coverage
- Write unit tests for all new functions
- Write integration tests for API interactions
- Use fixtures for common test data
- Mock external dependencies

## Documentation Guidelines

- Document all public APIs
- Use Google-style docstrings
- Add examples to docstrings
- Update docs/ when adding features
- Keep docs/v2/ in sync with code

## Release Process

(To be defined in Phase 9)

## Resources

- [Implementation Roadmap](plan/implementation-roadmap.md)
- [Architecture](docs/v2/architecture.md)
- [Development Workflow](docs/v2/development-workflow.md)
- [API Documentation](https://cognite-pygen.readthedocs-hosted.com/)

## Getting Help

- **Issues**: GitHub Issues
- **Discussions**: GitHub Discussions
- **Slack**: #pygen channel (Cognite internal)

## License

Apache 2.0 - See [LICENSE](LICENSE)

