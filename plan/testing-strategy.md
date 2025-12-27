# Pygen Rewrite - Testing Strategy

## Overview

This document outlines the comprehensive testing strategy for the Pygen rewrite. The goal is to achieve >90% test coverage while ensuring quality, reliability, and maintainability.

## Testing Principles

1. **Test-Driven Development**: Write tests before or alongside implementation
2. **Coverage >90%**: Enforce minimum coverage threshold in CI
3. **Pyramid Approach**: Many unit tests, fewer integration tests, minimal e2e tests
4. **Fast Feedback**: Tests should run quickly to enable rapid iteration
5. **Isolated Tests**: No dependencies between tests, can run in any order
6. **Reproducible**: Same inputs always produce same outputs
7. **Clear Assertions**: Test one thing at a time with clear failure messages

## Test Pyramid

```
                 /\
                /  \
               /E2E \         ~5% (Slow, expensive)
              /------\
             /        \
            /Integration\     ~25% (Medium speed)
           /------------\
          /              \
         /  Unit Tests    \   ~70% (Fast, isolated)
        /------------------\
```

## Testing Levels

### 1. Unit Tests

**Purpose**: Test individual functions, classes, and methods in isolation.

**Characteristics**:
- Fast (< 1ms per test)
- Isolated (no external dependencies)
- Focused (test one thing)
- Mocked dependencies

**Coverage Areas**:
- All public methods
- Error handling paths
- Edge cases
- Type conversions
- Validation logic

**Tools**:
- pytest
- pytest-mock (for mocking)
- pytest-cov (coverage)
- hypothesis (property-based testing)

**Example Structure**:
```
tests/unit/
├── client/
│   ├── test_auth.py
│   ├── test_http.py
│   ├── test_core.py
│   └── resources/
│       ├── test_data_models.py
│       ├── test_views.py
│       └── ...
├── ir/
│   ├── test_models.py
│   ├── test_parser.py
│   ├── test_validator.py
│   └── test_transformer.py
├── generation/
│   ├── test_base.py
│   └── python/
│       ├── test_generator.py
│       └── test_formatters.py
└── runtime/
    ├── test_base.py
    ├── test_lazy.py
    ├── test_query.py
    └── test_filters.py
```

**Best Practices**:
- Use descriptive test names: `test_should_raise_error_when_invalid_credentials()`
- Arrange-Act-Assert pattern
- One logical assertion per test
- Use fixtures for common setup
- Mock external dependencies

### 2. Integration Tests

**Purpose**: Test interaction between components.

**Characteristics**:
- Medium speed (< 100ms per test)
- Test component boundaries
- May use test databases or mock servers
- Focus on data flow

**Coverage Areas**:
- Client → API interaction (with mock server)
- Parser → IR → Generator flow
- Generator → Generated code → Runtime
- End-to-end generation workflow

**Tools**:
- pytest
- pytest-httpx (mock HTTP)
- pytest-docker (if needed)
- Factory Boy (test data generation)

**Example Structure**:
```
tests/integration/
├── test_client_integration.py
├── test_parsing_flow.py
├── test_generation_flow.py
├── test_runtime_with_client.py
└── fixtures/
    ├── mock_api_responses.py
    ├── sample_data_models.py
    └── test_data_factory.py
```

**Best Practices**:
- Use realistic test data
- Test happy path and error paths
- Verify data transformations
- Test with multiple scenarios
- Clean up resources after tests

### 3. End-to-End Tests

**Purpose**: Test complete user workflows.

**Characteristics**:
- Slow (seconds to minutes)
- Test real scenarios
- May require real API access
- Minimal number of tests

**Coverage Areas**:
- Full generation workflow (fetch → parse → generate)
- Generated SDK usage in real scenarios
- CLI commands end-to-end
- Migration workflows

**Tools**:
- pytest
- pytest-env (environment setup)
- Custom test fixtures for CDF instance

**Example Structure**:
```
tests/e2e/
├── test_complete_generation.py
├── test_cli_workflows.py
├── test_real_data_models.py
└── fixtures/
    ├── test_cdf_instance.py
    └── sample_schemas/
        ├── simple_model.yaml
        ├── complex_model.yaml
        └── real_world_model.yaml
```

**Best Practices**:
- Run in separate CI job (slower)
- Use real test environment
- Test critical paths only
- Comprehensive validation of results
- Clear setup/teardown

## Specialized Testing

### 4. Property-Based Testing

**Purpose**: Test with randomly generated inputs to find edge cases.

**Tool**: Hypothesis

**Use Cases**:
- IR validation logic
- Type conversions
- Serialization/deserialization
- Parser robustness

**Example**:
```python
from hypothesis import given, strategies as st

@given(
    st.text(min_size=1, max_size=100),
    st.integers(),
    st.booleans()
)
def test_ir_property_handles_any_valid_input(name, value, required):
    """Test that IRProperty handles various valid inputs"""
    prop = IRProperty(name=name, value=value, required=required)
    assert prop.name == name
    assert prop.value == value
    assert prop.required == required
```

### 5. Performance Tests

**Purpose**: Ensure performance targets are met.

**Tool**: pytest-benchmark

**Coverage Areas**:
- Client request performance
- Generation speed
- IR parsing performance
- Lazy evaluation overhead
- Memory usage

**Example Structure**:
```
tests/performance/
├── test_client_benchmarks.py
├── test_generation_benchmarks.py
├── test_query_benchmarks.py
└── benchmarks/
    └── baseline_results.json
```

**Metrics**:
- Requests per second
- Generation time for various model sizes
- Memory usage during lazy iteration
- Query building overhead

### 6. Regression Tests

**Purpose**: Ensure new changes don't break existing functionality.

**Strategy**:
- Golden file tests (generated code comparison)
- API compatibility tests
- Performance regression tests

**Example**:
```python
def test_generated_code_matches_golden_file(data_model, tmp_path):
    """Ensure generated code hasn't changed unexpectedly"""
    generator = PythonGenerator()
    result = generator.generate(data_model)
    
    golden_file = Path(__file__).parent / "golden" / "expected_output.py"
    
    if UPDATE_GOLDEN:
        golden_file.write_text(result)
    else:
        expected = golden_file.read_text()
        assert result == expected
```

### 7. Security Tests

**Purpose**: Identify security vulnerabilities.

**Coverage Areas**:
- Credential handling
- Input validation
- SQL injection (if applicable)
- Code injection in templates
- Dependency vulnerabilities

**Tools**:
- bandit (Python security linter)
- safety (dependency checker)
- Custom security tests

**Example**:
```python
def test_credentials_not_logged():
    """Ensure credentials never appear in logs"""
    with capture_logs() as logs:
        client = PygenClient(credentials="secret_token")
        client.data_models.list()
    
    for log in logs:
        assert "secret_token" not in log
```

### 8. Mutation Tests

**Purpose**: Test the quality of tests themselves.

**Tool**: mutmut

**Strategy**:
- Run periodically (not in regular CI)
- Identify untested code paths
- Improve test quality

## Test Organization

### Directory Structure

```
tests/
├── __init__.py
├── conftest.py              # Shared fixtures
├── unit/                    # Unit tests (70%)
│   ├── __init__.py
│   ├── conftest.py
│   └── ...
├── integration/             # Integration tests (25%)
│   ├── __init__.py
│   ├── conftest.py
│   └── ...
├── e2e/                     # End-to-end tests (5%)
│   ├── __init__.py
│   ├── conftest.py
│   └── ...
├── performance/             # Performance benchmarks
│   └── ...
├── security/                # Security tests
│   └── ...
├── fixtures/                # Test data and fixtures
│   ├── __init__.py
│   ├── sample_data_models.py
│   ├── mock_responses.py
│   └── test_data/
│       ├── models/
│       └── schemas/
└── utils/                   # Test utilities
    ├── __init__.py
    ├── assertions.py
    ├── mock_server.py
    └── helpers.py
```

### Naming Conventions

**Test Files**: `test_<module_name>.py`
**Test Classes**: `Test<ClassName>`
**Test Functions**: `test_<method>_<scenario>_<expected_result>`

**Examples**:
- `test_parser_with_invalid_view_raises_validation_error()`
- `test_generator_creates_valid_python_code()`
- `test_lazy_iterator_fetches_data_in_chunks()`

## Test Fixtures

### Common Fixtures

```python
# conftest.py

import pytest
from pygen.client import PygenClient
from tests import MockCDFServer


@pytest.fixture
def mock_cdf_server():
    """Provide mock CDF API server"""
    server = MockCDFServer()
    server.start()
    yield server
    server.stop()


@pytest.fixture
def pygen_client(mock_cdf_server):
    """Provide PygenClient connected to mock server"""
    return PygenClient(
        base_url=mock_cdf_server.url,
        credentials="test_token"
    )


@pytest.fixture
def sample_view():
    """Provide sample View object"""
    return View(
        space="test_space",
        external_id="test_view",
        version="1",
        properties={...}
    )


@pytest.fixture
def sample_ir_model():
    """Provide sample IR model"""
    return IRModel(
        name="TestModel",
        classes=[...],
        metadata={...}
    )
```

### Fixture Strategies

1. **Scope**: Use appropriate scope (function, class, module, session)
2. **Parameterization**: Use `@pytest.mark.parametrize` for multiple scenarios
3. **Factories**: Use factory fixtures for variations
4. **Auto-use**: Use `autouse=True` for setup/teardown only

## Mock Strategies

### What to Mock

- External APIs (CDF API)
- HTTP requests
- File system operations (when testing logic, not I/O)
- Time-dependent operations
- Random number generation

### What NOT to Mock

- Internal functions (test real implementation)
- Simple data structures
- Pure functions
- Code under test

### Mock Tools

**pytest-mock**: Simple mocking
```python
def test_client_retries_on_failure(mocker):
    mock_request = mocker.patch("httpx.Client.request")
    mock_request.side_effect = [
        httpx.TimeoutException,
        httpx.TimeoutException,
        Response(200, json={"success": True})
    ]
    
    client = PygenClient(...)
    result = client.data_models.list()
    
    assert mock_request.call_count == 3
```

**pytest-httpx**: Mock HTTP
```python
def test_client_makes_correct_request(httpx_mock):
    httpx_mock.add_response(
        url="https://api.cognite.com/api/v1/projects/test/models/datamodels",
        json={"items": [...]}
    )
    
    client = PygenClient(...)
    result = client.data_models.list()
    
    assert len(result) > 0
```

## Coverage Strategy

### Coverage Targets

- **Overall**: >90%
- **Critical paths**: 100%
- **New code**: 100%
- **Bug fixes**: 100% (regression test required)

### Coverage Tools

- pytest-cov
- coverage.py
- codecov.io (for visualization)

### Configuration

```ini
# pyproject.toml
[tool.coverage.run]
source = ["cognite/pygen"]
omit = [
    "*/tests/*",
    "*/test_*.py",
    "*/__init__.py",
]

[tool.coverage.report]
exclude_lines = [
    "pragma: no cover",
    "def __repr__",
    "raise AssertionError",
    "raise NotImplementedError",
    "if __name__ == .__main__.:",
    "if TYPE_CHECKING:",
    "@abstractmethod",
]
fail_under = 90
```

### Coverage Enforcement

- CI fails if coverage < 90%
- PR reviews check coverage delta
- Coverage report in PR comments
- Identify untested code paths

## Continuous Integration

### CI Pipeline

```yaml
# .github/workflows/test.yml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.10", "3.11", "3.12"]
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: ${{ matrix.python-version }}
      
      - name: Install dependencies
        run: |
          pip install uv
          uv pip install -e ".[dev]"
      
      - name: Run unit tests
        run: pytest tests/unit --cov --cov-report=xml
      
      - name: Run integration tests
        run: pytest tests/integration
      
      - name: Upload coverage
        uses: codecov/codecov-action@v3

  test-e2e:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Run E2E tests
        run: pytest tests/e2e
        env:
          CDF_TEST_TOKEN: ${{ secrets.CDF_TEST_TOKEN }}

  security:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Run security checks
        run: |
          bandit -r cognite/pygen
          safety check
```

### Test Execution Strategy

**On every commit**:
- Unit tests
- Integration tests
- Linting (ruff)
- Type checking (mypy)

**On pull request**:
- Full test suite
- Coverage report
- Performance benchmarks (if changed)

**Nightly**:
- E2E tests
- Mutation tests
- Security scans
- Performance regression tests

**Pre-release**:
- Full test suite
- Manual testing
- Load testing
- Security audit

## Test Data Management

### Sample Data Models

Create representative sample data models:
- Simple (few views, basic properties)
- Medium (multiple views, relationships)
- Complex (inheritance, circular refs, many types)
- Real-world (actual customer models, anonymized)

### Test Data Location

```
tests/fixtures/test_data/
├── models/
│   ├── simple_model.yaml
│   ├── medium_model.yaml
│   ├── complex_model.yaml
│   └── real_world/
│       ├── model1.yaml
│       └── model2.yaml
├── responses/
│   ├── data_models_list.json
│   ├── view_retrieve.json
│   └── ...
└── golden/
    ├── simple_model/
    │   ├── data_classes.py
    │   └── api_classes.py
    └── ...
```

### Data Factories

```python
# tests/fixtures/factories.py

from factory import Factory, Faker, SubFactory
from pygen.client.models import View, Property

class PropertyFactory(Factory):
    class Meta:
        model = Property
    
    name = Faker("word")
    type = "string"
    nullable = False

class ViewFactory(Factory):
    class Meta:
        model = View
    
    space = "test_space"
    external_id = Faker("uuid4")
    version = "1"
    properties = SubFactory(PropertyFactory)
```

## Quality Gates

### Pre-Commit Checks

- ruff format (auto-fix)
- ruff check (auto-fix)
- mypy (type checking)
- Unit tests (fast subset)

### Pull Request Requirements

- All tests pass
- Coverage ≥90%
- No mypy errors
- No ruff errors
- Code review approved
- Documentation updated

### Release Requirements

- All tests pass (including E2E)
- Coverage ≥90%
- Performance benchmarks met
- Security scan clean
- Documentation complete
- Changelog updated

## Debugging Failed Tests

### Best Practices

1. **Isolate**: Run single failing test
2. **Verbose**: Use `-vv` flag
3. **Debug**: Use `--pdb` flag
4. **Logs**: Enable debug logging
5. **Reproduce**: Ensure test failure is reproducible

### Useful Commands

```bash
# Run single test
pytest tests/unit/test_parser.py::test_parse_view -vv

# Debug test
pytest tests/unit/test_parser.py::test_parse_view --pdb

# Show print statements
pytest tests/unit/test_parser.py::test_parse_view -s

# Show slow tests
pytest --durations=10

# Run only failed tests
pytest --lf

# Run failed tests first
pytest --ff
```

## Documentation

### Test Documentation

Each test module should have:
- Module docstring explaining what's being tested
- Comments for complex test setup
- Clear test names that explain intent

### Testing Guide

Create `docs/developer_docs/testing.md`:
- How to run tests
- How to write tests
- How to debug tests
- Coverage guidelines
- CI/CD process

## Maintenance

### Regular Activities

**Weekly**:
- Review test failures
- Check coverage trends
- Update test data if needed

**Monthly**:
- Run mutation tests
- Review slow tests
- Refactor duplicated test code
- Update fixtures

**Quarterly**:
- Security audit
- Performance benchmark review
- Test strategy review
- Tool updates

### Test Debt

Track and address test debt:
- Flaky tests
- Slow tests
- Skipped tests
- TODO comments in tests

## Success Metrics

### Quantitative
- Coverage >90%
- Test execution time <5 minutes (unit + integration)
- Zero flaky tests
- All tests passing
- Mutation score >80%

### Qualitative
- Tests are easy to understand
- Tests are easy to write
- Tests catch bugs early
- Confidence in refactoring
- Fast feedback loop

## Conclusion

This comprehensive testing strategy ensures the Pygen rewrite will be:
- **Reliable**: High coverage catches bugs
- **Maintainable**: Good tests enable refactoring
- **Fast**: Quick feedback enables rapid iteration
- **Secure**: Security tests catch vulnerabilities
- **Performant**: Benchmarks prevent regression

The investment in testing infrastructure and practices will pay dividends throughout the project lifecycle and beyond.

