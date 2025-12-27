# Pygen Rewrite - Architecture Design

## Overview

This document outlines the proposed architecture for the Pygen rewrite. The architecture is designed to address the core problems: performance, lazy evaluation, multi-language support, maintainability, API service support, and upfront validation with graceful degradation.

## Core Architectural Principles

1. **Separation of Concerns**: Clear boundaries between client operations, code generation, and runtime behavior
2. **Pydantic-First**: All data models built on pydantic for validation, serialization, and performance
3. **Language-Agnostic Core**: Internal representation that can target multiple output languages
4. **Lazy by Default**: All data access patterns support lazy evaluation
5. **Testability**: Design for >90% test coverage from the start
6. **Extensibility**: Easy to add new languages, features, and API endpoints

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Pygen Core System                        │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌─────────────────────────────────────────────────────┐   │
│  │         1. Pygen Client (Runtime)                    │   │
│  │  - HTTPClient wrapper (httpx-based)                 │   │
│  │  - Authentication                                    │   │
│  │  - CRUD operations for DM concepts                  │   │
│  │  - Pydantic models for API objects                  │   │
│  │  - Query builder/optimizer                          │   │
│  └─────────────────────────────────────────────────────┘   │
│                           │                                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │    2. Generic Instance API (Python)                  │   │
│  │  - InstanceModel, Instance, InstanceWrite           │   │
│  │  - InstanceClient (CRUD operations)                 │   │
│  │  - InstanceAPI (view-specific operations)           │   │
│  │  - Example SDK showing extension patterns           │   │
│  └─────────────────────────────────────────────────────┘   │
│                           │                                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │    3. Generic Instance API (TypeScript)              │   │
│  │  - Instance interfaces/classes                      │   │
│  │  - InstanceClient (CRUD operations)                 │   │
│  │  - InstanceAPI (view-specific operations)           │   │
│  │  - Example SDK showing extension patterns           │   │
│  └─────────────────────────────────────────────────────┘   │
│                           │                                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │    4. Intermediate Representation (IR)               │   │
│  │  - Validation layer (before IR creation)            │   │
│  │  - Language-agnostic model representation           │   │
│  │  - Type system abstraction                          │   │
│  │  - Parser (CDF → IR)                                │   │
│  │  - Transformer (IR → Language-specific IR)          │   │
│  └─────────────────────────────────────────────────────┘   │
│                           │                                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │         5. Code Generation Engine                    │   │
│  │  - Template system (Jinja2 for Python)              │   │
│  │  - Language-specific generators:                    │   │
│  │    - Python Generator (from IR)                     │   │
│  │    - TypeScript Generator (from IR)                 │   │
│  │  - Generates code matching Phases 2-3 patterns      │   │
│  │  - Formatting & linting integration                 │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. Pygen Client (Runtime)

**Purpose**: Replace `cognite-sdk` with a lightweight, purpose-built client for data modeling operations. This
lightweight client will not be exposed to end users directly but will be used to fetch data models, views,
containers, and spaces from the CDF API to drive code generation and runtime operations.

**Key Features**:
- Internal HTTPClient wrapper around `httpx` for modern async/sync support
- Pydantic models for all API objects (DataModel, View, Container, Space)
- Connection pooling and rate limiting. Note that read, write and delete will have different concurrency limits.
- Automatic retry logic with exponential backoff
- Streaming support for large datasets
- Query builder/optimizer for simplifying complex API interactions
- Full type hints for IDE support

**Structure**:
This is a simplified representation of the client structure, the actual implementation will be more detailed.
```
pygen/client/
├── __init__.py
├── core.py              # Main PygenClient class
├── auth/              # Authentication handlers
├── http_client/       # Internal HTTPClient wrapper (httpx)
├── query_builder.py     # Query builder/optimizer
├── models/              # Pydantic models for API objects
│   ├── __init__.py
│   ├── data_model.py
│   ├── view.py
│   ├── container.py
│   └── space.py
├── resources/           # Resource-specific clients
│   ├── __init__.py
│   ├── resources.py    # This file contains the common logic for the iterate, list, create, delete, retrieve methods for all resources.
│   ├── data_models.py
│   ├── views.py
│   ├── containers.py
│   └── spaces.py
└── exceptions.py        # Custom exceptions
```

**API Design Example**:

```python
from cognite.pygen._client import PygenClient
from cognite.pygen._generation.python.instance_api.config import PygenClientConfig
from cognite.pygen._client.models import DataModelReference, DataModelRequest

client = PygenClient(config=PygenClientConfig(...))

# CRUD operations with pydantic models, all CRUD methods support Sequence inputs
data_model = client.data_models.retrieve(
    [
        DataModelReference(
            space="my_space",
            external_id="my_model",
            version="v1",
        )
    ]
)
delete_result = client.data_models.delete(
    [
        DataModelReference(
            space="my_space",
            external_id="my_model",
            version="v1",
        )
    ]
)
create_result = client.data_models.create(
    [
        DataModelRequest(
            space="my_space",
            external_id="my_model",
            version="v1",

        )
    ]
)
page = client.data_models.iterate(
    space="my_space",
    include_global=True,
    cursor=None,
    limit=100,
)
print(page.cursor)
print(page.items)

all_models = client.data_models.list(
    space="my_space",
    include_global=True,
    limit=None,
)




```

### 2. Generic Instance API (Python)

**Purpose**: Provide generic base classes for instance CRUD operations that can be extended for specific views.

**Key Features**:
- Generic `InstanceModel`, `Instance`, `InstanceWrite` base classes
- `InstanceClient` for CRUD operations (upsert, delete)
- `InstanceAPI` base class for view-specific operations (retrieve, list, iterate, search, aggregate)
- Support for both node and edge instance types
- Pagination and lazy iteration
- Pandas integration for data analysis

**Structure**:
```
pygen/_generation/python/_instance_api/
├── __init__.py
├── _instance.py         # Base models (Instance, InstanceWrite, InstanceList)
├── _client.py           # InstanceClient for CRUD
├── _api.py              # InstanceAPI base class
└── _utils.py            # Helper utilities

pygen/_generation/python/example/
├── __init__.py
├── _data_class.py       # Example data classes extending Instance
├── _api.py              # Example API classes extending InstanceAPI
└── _client.py           # Example client extending InstanceClient
```

**Design Pattern**:
```python
# Generic base (in _instance_api/)
class InstanceAPI(Generic[T_InstanceWrite, T_Instance, T_InstanceList]):
    def retrieve(self, id: ...) -> T_Instance | T_InstanceList | None: ...
    def list(self) -> T_InstanceList: ...
    def iterate(self) -> Page[T_InstanceList]: ...

# View-specific API (generated or hand-written example)
class PrimitiveNullableAPI(InstanceAPI[PrimitiveNullableWrite, PrimitiveNullable, PrimitiveNullableList]):
    # Inherits all methods with proper types
```

### 3. Generic Instance API (TypeScript)

**Purpose**: TypeScript equivalent of the Python generic instance API.

**Key Features**:
- Generic instance interfaces/classes
- InstanceClient for CRUD operations
- InstanceAPI base class for view-specific operations
- TypeScript generics for type safety
- Async/await patterns for API calls

**Structure**:
```
pygen/_generation/typescript/_instance_api/
├── index.ts
├── instance.ts          # Base interfaces/classes
├── client.ts            # InstanceClient
└── api.ts               # InstanceAPI base class

pygen/_generation/typescript/example/
├── index.ts
├── dataClasses.ts       # Example data classes
├── api.ts               # Example API classes
└── client.ts            # Example client
```

### 4. Intermediate Representation (IR)

**Purpose**: Create a language-agnostic representation of CDF data models after validation, which can be transformed into Python or TypeScript code.

**Key Features**:
- Validation layer upfront (before IR creation)
- Language-agnostic type system
- Property and connection mapping
- Parser: CDF API models → IR
- Transformer: IR → Language-specific IR (Python or TypeScript)

**Structure**:
```
pygen/ir/
├── __init__.py
├── validation/          # Validation before IR
│   ├── __init__.py
│   ├── validator.py     # Main validation logic
│   ├── rules.py         # Validation rules
│   └── warnings.py      # Warning types
├── models.py            # IR model definitions
├── types.py             # Type system abstraction
├── parser.py            # CDF → IR parser
└── transformer.py       # IR → Language-specific IR
```

**IR Model Structure**:
```python
class IRType:
    """Language-agnostic type representation"""
    name: str
    nullable: bool
    default: Any | None
    
    def as_python_type() -> str: ...
    def as_typescript_type() -> str: ...

class IRProperty:
    """Property representation"""
    name: str
    type: IRType
    description: str | None
    required: bool
    
class IRConnection:
    """Connection/relationship representation"""
    name: str
    target_class: str
    cardinality: Literal["one", "many"]
    connection_type: str  # direct_relation, edge, reverse
    
class IRClass:
    """View representation"""
    name: str
    properties: list[IRProperty]
    connections: list[IRConnection]
    parent: IRClass | None
    description: str | None
    
class IRModel:
    """Complete data model representation"""
    name: str
    classes: list[IRClass]
    metadata: dict[str, Any]
```

**IR Flow**:
```
CDF Data Model
   ↓ (validation)
Validated Model
   ↓ (parser)
Language-Agnostic IR
   ↓ (transformer)
Python-Specific IR  or  TypeScript-Specific IR
   ↓ (generator)
Python Code         or  TypeScript Code
```

### 5. Code Generation Engine

**Purpose**: Generate code from IR that follows the patterns established in Phases 2-3 (generic API + example SDK).

**Key Features**:
- Generates code that extends generic InstanceAPI/InstanceClient
- Template-based generation (Jinja2 for Python, custom for TypeScript)
- Post-processing (ruff for Python, prettier for TypeScript)
- Generates data classes extending Instance/InstanceWrite
- Generates API classes extending InstanceAPI
- Generates client classes extending InstanceClient

**Structure**:
```
pygen/generation/
├── __init__.py
├── base.py              # Base generator class
├── python/              # Python generator
│   ├── __init__.py
│   ├── generator.py     # Generates from IR
│   ├── templates/
│   │   ├── data_class.py.jinja    # Extends Instance/InstanceWrite
│   │   ├── api_class.py.jinja     # Extends InstanceAPI
│   │   ├── client.py.jinja        # Extends InstanceClient
│   │   └── __init__.py.jinja
│   └── formatter.py     # ruff integration
├── typescript/          # TypeScript generator
│   ├── generator.ts
│   ├── templates/
│   │   ├── dataClass.ts.jinja
│   │   ├── apiClass.ts.jinja
│   │   ├── client.ts.jinja
│   │   └── index.ts.jinja
│   └── formatter.ts     # prettier integration
└── config.py            # Generation configuration
```

**Generator Pattern**:
```python
class PythonGenerator(BaseGenerator):
    def generate(self, ir_model: IRModel) -> GenerationResult:
        """Generate Python SDK from IR"""
        # For each IRClass in ir_model:
        #   - Generate data class extending Instance/InstanceWrite
        #   - Generate API class extending InstanceAPI[Write, Read, List]
        #   - Generate client method on main client
        ...
```

**Generated Code Pattern** (matches Phase 2):
```python
# Generated data class
class MyView(Instance):
    _view_id = ViewRef(space="my_space", external_id="MyView", version="1")
    my_property: str
    # ... other properties

# Generated API class
class MyViewAPI(InstanceAPI[MyViewWrite, MyView, MyViewList]):
    # Inherits retrieve, list, iterate, search, aggregate
    pass

# Generated client
class MyClient(InstanceClient):
    def __init__(self, config: PygenClientConfig):
        super().__init__(config)
        self.my_view = MyViewAPI(self._http_client, MyView._view_id, "node")
```


## Data Flow

### Development Flow (Phases 2-3)
```
Phase 2: Build Generic Python API
   ↓
1. Create InstanceModel, Instance, InstanceWrite base classes
2. Create InstanceClient for CRUD
3. Create InstanceAPI base class
4. Build example SDK manually extending generic classes
   ↓
Phase 3: Build Generic TypeScript API
   ↓
5. Create equivalent TypeScript interfaces/classes
6. Create TypeScript InstanceClient and InstanceAPI
7. Build example TypeScript SDK
```

### Generation Flow (Phase 5+)
```
1. User provides data model specification
   ↓
2. Pygen Client fetches data model from CDF API
   ↓
3. Validation Layer validates model
   - Checks for incomplete models, missing relations
   - Generates warnings
   - Filters out problematic elements
   ↓
4. Parser converts validated CDF models to IR
   ↓
5. Transformer converts IR to language-specific IR
   - Apply Python or TypeScript naming conventions
   - Handle language-specific reserved words
   ↓
6. Generator creates code from language-specific IR
   - Generates data classes extending Instance/InstanceWrite
   - Generates API classes extending InstanceAPI
   - Generates client extending InstanceClient
   ↓
7. Formatter/linter processes generated code
   - ruff for Python
   - prettier for TypeScript
   ↓
8. Output files written to disk (or returned via API)
```

### Runtime Flow (Using Generated SDK)
```
1. User imports generated SDK
   ↓
2. User creates client instance (extends InstanceClient)
   client = MyGeneratedClient(config)
   ↓
3. User accesses view-specific API (extends InstanceAPI)
   api = client.my_view  # MyViewAPI instance
   ↓
4. User calls methods on API
   items = api.list()  # Returns MyViewList
   item = api.retrieve(id)  # Returns MyView or None
   ↓
5. InstanceAPI uses InstanceClient for CRUD
   ↓
6. InstanceClient uses HTTPClient to call CDF API
   ↓
7. Data deserialized into view-specific classes (extend Instance)
   ↓
8. User processes strongly-typed data
```

### API Service Flow (New Goal 5)
```
1. HTTP request to Pygen API service
   POST /generate with data model specification
   ↓
2. Service validates request
   ↓
3. Service runs generation flow (steps 2-6 from Generation Flow)
   ↓
4. Service returns generated SDK as:
   - ZIP file of source files, or
   - Package tarball, or
   - Direct code response
   ↓
5. Client downloads and uses generated SDK
```

## Key Design Decisions

### 1. Why httpx over requests?
- Async/sync support
- HTTP/2 support
- Better connection pooling
- Modern, actively maintained
- Better performance characteristics

### 2. Why pydantic v2?
- Performance improvements (rust core)
- Better validation
- JSON schema generation
- Type safety
- Serialization performance

### 3. Why lazy by default?
- Scalability with large datasets
- Reduced memory footprint
- Better user experience (faster initial response)
- Explicit eager loading when needed

### 4. Why build generic API before IR?
- Establishes concrete patterns for generated code
- Validates design with real examples
- Provides immediate value (can use generic API directly)
- Informs IR design based on actual needs
- Easier to test patterns before codifying in IR

### 5. Why IR layer after generic API?
- Can learn from concrete implementations (Phases 2-3)
- Decouples parsing from generation
- Enables multi-language support once patterns are proven
- Easier to test each stage
- Allows IR transformations
- Version compatibility layer

### 6. Why extend generic classes (not generate from scratch)?
- Reduces code duplication
- Centralizes CRUD logic in InstanceClient
- Easy to add features to all generated SDKs
- Clear separation: generic operations vs view-specific logic
- Type safety through generics
- Easier to maintain and evolve

### 7. Why validation before IR?
- Catches issues early in the pipeline
- Allows graceful degradation decisions before IR creation
- Prevents invalid models from entering IR
- Better error messages for users
- Enables partial generation for incomplete models

### 8. Why template-based generation?
- Easy to customize
- Language-specific conventions
- Maintainable
- Community can contribute templates
- Clear separation of logic and output
- Can generate code that extends generic classes

## Performance Considerations

### Client Performance
- Connection pooling (httpx default)
- Request batching where possible
- Streaming for large responses
- Compression support
- Efficient pagination

### Generation Performance
- Parallel generation of independent classes
- Incremental generation (only changed files)
- Template caching
- Efficient IR representation

### Runtime Performance
- Lazy evaluation reduces initial load
- Efficient serialization (pydantic v2)
- Query optimization
- Result caching where appropriate

## Extensibility Points

### Adding a New Language
1. Create language-specific generator in `pygen/generation/{language}/`
2. Implement `BaseGenerator` interface
3. Create Jinja2 templates
4. Add formatter integration
5. Add tests

### Adding New API Operations
1. Add pydantic model in `pygen/client/models/`
2. Add resource methods in `pygen/client/resources/`
3. Update validation rules if needed
4. Update IR if needed
5. Update generators if new patterns needed
6. Add tests

### Adding New Validation Rules
1. Add rule in `pygen/validation/rules.py`
2. Add warning type if needed
3. Update filtering logic
4. Add tests for edge cases

### Custom Templates
1. Users can provide custom template directory
2. Templates override defaults
3. Full access to IR objects in templates

### API Service Endpoints (Goal 5)
1. `/generate` - Generate SDK from specification
2. `/validate` - Validate data model
3. `/health` - Service health check
4. `/version` - Pygen version info

## Migration Strategy

### Backward Compatibility
- Generated code should maintain similar API surface
- Provide migration guide for breaking changes
- Version generated code with metadata
- Support both old and new client side-by-side initially

### Deprecation Path
1. Release new version with deprecation warnings
2. Provide migration tools where possible
3. Document all breaking changes
4. Support old version for defined period
5. Clear communication timeline

## Security Considerations

- Secure credential handling
- No secrets in generated code
- HTTPS-only by default
- Rate limiting to prevent abuse
- Input validation on all API calls
- Audit logging support

## Monitoring and Observability

- Structured logging throughout
- Performance metrics collection
- Error tracking integration
- Request/response logging (opt-in)
- Generation analytics

## Package Structure and Shipping

The SDK generator supports multiple languages. Each language has a generic part (Instance API) and templates for generating model-specific code. Tests are run for all languages but are not shipped with the package.

### Directory Structure

```
cognite/pygen/
├── _client/                           # Pygen Client (internal)
│   └── ...
├── _generation/                       # Code generation for all languages
│   ├── __init__.py
│   ├── python/
│   │   ├── instance_api/              # Generic Python SDK (SHIPPED)
│   │   │   ├── __init__.py
│   │   │   ├── _instance.py
│   │   │   ├── _client.py
│   │   │   ├── _api.py
│   │   │   └── _utils.py
│   │   ├── examples/                  # Basic examples (SHIPPED)
│   │   │   └── ...
│   │   └── templates/                 # Jinja2 templates (SHIPPED)
│   │       └── ...
│   ├── typescript/
│   │   ├── instance_api/              # Generic TypeScript SDK (SHIPPED)
│   │   │   ├── src/
│   │   │   │   ├── index.ts
│   │   │   │   ├── instance.ts
│   │   │   │   ├── client.ts
│   │   │   │   └── api.ts
│   │   │   ├── package.json
│   │   │   └── tsconfig.json
│   │   ├── examples/                  # Basic examples (SHIPPED)
│   │   │   └── ...
│   │   └── templates/                 # Code generation templates (SHIPPED)
│   │       └── ...
│   ├── csharp/                        # Future: C# support
│   │   ├── instance_api/
│   │   ├── examples/
│   │   └── templates/
│   └── ...                            # Future languages
├── _ir/                               # Intermediate Representation
│   └── ...
└── ...

tests/                                 # NOT SHIPPED - tests only
├── conftest.py                        # Shared fixtures
├── tests_python/                      # Python SDK tests
│   ├── test_instance_api.py
│   ├── test_generation.py
│   └── generated/                     # Temp generated code for testing
├── tests_typescript/                  # TypeScript SDK tests
│   ├── test_generation.py             # Python wrapper tests
│   ├── __tests__/                     # Vitest tests
│   │   ├── instance.test.ts
│   │   └── client.test.ts
│   └── generated/                     # Temp generated code for testing
├── tests_csharp/                      # Future: C# tests
│   ├── test_generation.py
│   └── Tests/                         # NUnit/xUnit tests
└── integration/                       # Cross-language integration tests
    └── ...

# Root level TypeScript configuration (shared)
package.json                           # TypeScript dev dependencies
tsconfig.json                          # TypeScript configuration
vitest.config.ts                       # Vitest test runner configuration
node_modules/                          # Shared dependencies (gitignored)
```

### What Gets Shipped

The Python package ships:
- `instance_api/` - Generic runtime code for each language
- `examples/` - Basic examples showing extension patterns
- `templates/` - Code generation templates

The package does NOT ship:
- `tests/` - Test code stays in the repository
- `generated/` - Temporary generated code for testing
- Language-specific test files (`__tests__/`, `Tests/`, etc.)

### pyproject.toml Configuration

```toml
[tool.setuptools.packages.find]
where = ["."]
include = ["cognite.pygen*"]

[tool.setuptools.package-data]
"cognite.pygen._generation.python" = [
    "instance_api/**/*.py",
    "examples/**/*.py",
    "templates/**/*.jinja",
]
"cognite.pygen._generation.typescript" = [
    "instance_api/**/*",
    "examples/**/*",
    "templates/**/*",
]
"cognite.pygen._generation.csharp" = [
    "instance_api/**/*",
    "examples/**/*",
    "templates/**/*",
]

[tool.setuptools.exclude-package-data]
"*" = [
    "tests/*",
    "**/generated/*",
    "**/__tests__/*",
    "**/Tests/*",
    "**/*.test.ts",
    "**/*.test.js",
    "**/test_*.py",
]
```

## Multi-Language Testing Strategy

All language SDKs are tested, but tests are orchestrated through Python/pytest for consistency.

### Testing Architecture

```
pytest (orchestrator)
    │
    ├── Python tests (direct)
    │   └── test_*.py files using pytest
    │
    ├── TypeScript tests (via subprocess, from repository root)
    │   ├── Generate SDK → temp directory
    │   ├── npm install (runs from root, uses root package.json)
    │   ├── npm run build (tsc compilation from root)
    │   └── npm test (vitest from root)
    │
    ├── C# tests (via subprocess)
    │   ├── Generate SDK → temp directory
    │   ├── dotnet restore
    │   ├── dotnet build
    │   └── dotnet test
    │
    └── Integration tests
        └── Cross-language consistency checks
```

### Python Test Wrappers for Other Languages

```python
# tests/tests_typescript/test_generation.py
import subprocess
from pathlib import Path
import pytest


# Root directory where package.json and node_modules are located
ROOT_DIR = Path(__file__).parent.parent.parent


@pytest.fixture(scope="session")
def generated_ts_sdk(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Generate TypeScript SDK once per test session."""
    output_dir = tmp_path_factory.mktemp("ts_sdk")
    # Generate SDK using pygen
    from cognite.pygen._generation.typescript import generate_typescript_sdk
    generate_typescript_sdk(data_model=..., output_dir=output_dir)
    return output_dir


def test_typescript_compiles(generated_ts_sdk: Path) -> None:
    """Verify generated TypeScript compiles without errors."""
    result = subprocess.run(
        ["npm", "run", "build"],
        cwd=ROOT_DIR,  # Run from root where tsconfig.json is
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"TypeScript compilation failed: {result.stderr}"


def test_typescript_tests_pass(generated_ts_sdk: Path) -> None:
    """Run TypeScript unit tests on generated SDK."""
    result = subprocess.run(
        ["npm", "test"],
        cwd=ROOT_DIR,  # Run from root where vitest.config.ts is
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"TypeScript tests failed: {result.stderr}"


def test_typescript_lint_passes(generated_ts_sdk: Path) -> None:
    """Verify generated TypeScript passes linting."""
    result = subprocess.run(
        ["npm", "run", "lint"],
        cwd=ROOT_DIR,  # Run from root
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"TypeScript linting failed: {result.stderr}"
```

### TypeScript Test Project Structure

All TypeScript configuration files (package.json, tsconfig.json, etc.) are placed at the repository root level to share the same `node_modules` directory. TypeScript tests are placed in `tests/tests_typescript/` following the same pattern as Python tests.

```
# Root level (shared TypeScript configuration)
package.json                     # TypeScript dependencies (dev dependencies)
tsconfig.json                    # TypeScript config
vitest.config.ts                 # Test runner config
node_modules/                    # Shared node_modules (gitignored)

# Test files
tests/tests_typescript/
├── test_generation.py           # Python wrapper tests
├── __tests__/                   # TypeScript unit tests
│   ├── instance.test.ts         # Tests for instance_api
│   ├── client.test.ts           # Tests for client
│   └── api.test.ts              # Tests for API classes
└── generated/                   # .gitignore'd, temp generated code
```

### Root package.json (TypeScript Configuration)

The root `package.json` contains all TypeScript development dependencies. This allows sharing `node_modules` across all TypeScript code in the project.

```json
{
  "name": "@cognite/pygen",
  "private": true,
  "scripts": {
    "build": "tsc",
    "test": "vitest run",
    "lint": "eslint . --ext .ts"
  },
  "devDependencies": {
    "typescript": "^5.0.0",
    "vitest": "^1.0.0",
    "@types/node": "^20.0.0",
    "eslint": "^8.0.0",
    "@typescript-eslint/parser": "^6.0.0",
    "@typescript-eslint/eslint-plugin": "^6.0.0"
  }
}
```

Note: The `.gitignore` file is shared for both Python and TypeScript, including entries for `node_modules/`, `*.js` build artifacts, and other TypeScript-specific ignores alongside Python ignores.

### CI/CD Configuration

```yaml
# .github/workflows/test.yml
name: Test

on: [push, pull_request]

jobs:
  test-python:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.10"
      - run: pip install -e ".[dev]"
      - run: pytest tests/tests_python

  test-typescript:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.10"
      - uses: actions/setup-node@v4
        with:
          node-version: "20"
      - run: pip install -e ".[dev]"
      - run: npm install  # Uses root package.json
      - run: pytest tests/tests_typescript

  test-csharp:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.10"
      - uses: actions/setup-dotnet@v4
        with:
          dotnet-version: "8.0"
      - run: pip install -e ".[dev]"
      - run: pytest tests/tests_csharp
```

### Test Categories

1. **Unit Tests**: Test individual components in isolation
   - IR parsing and transformation
   - Template rendering
   - Type mappings

2. **Generation Tests**: Test that generated code is valid
   - Compiles without errors
   - Passes linting
   - Has correct structure

3. **Runtime Tests**: Test that generated SDK works correctly
   - CRUD operations
   - Serialization/deserialization
   - Type safety

4. **Integration Tests**: Test cross-language consistency
   - Same data model generates equivalent SDKs
   - API behavior is consistent across languages

## Documentation Strategy

- API reference (auto-generated from docstrings)
- User guides for each feature
- Architecture documentation (this doc)
- Contributing guide
- Examples and tutorials
- Migration guide
