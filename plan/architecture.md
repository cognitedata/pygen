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
│  │         2. Validation Layer                          │   │
│  │  - Schema validation (before IR)                    │   │
│  │  - Incomplete model handling                        │   │
│  │  - Warning generation                               │   │
│  │  - Graceful degradation decisions                   │   │
│  └─────────────────────────────────────────────────────┘   │
│                           │                                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │         3. Intermediate Representation (IR)          │   │
│  │  - Language-agnostic model representation           │   │
│  │  - Type system abstraction                          │   │
│  │  - Relationship mapping                             │   │
│  └─────────────────────────────────────────────────────┘   │
│                           │                                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │         4. Code Generation Engine                    │   │
│  │  - Template system (Jinja2)                         │   │
│  │  - Language-specific generators:                    │   │
│  │    - Python Generator                               │   │
│  │    - TypeScript Generator (future)                  │   │
│  │    - C# Generator (future)                          │   │
│  │    - PySpark Generator (future)                     │   │
│  │  - Formatting & linting integration                 │   │
│  └─────────────────────────────────────────────────────┘   │
│                           │                                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │         5. Generated SDK Runtime                     │   │
│  │  - Client-based design (not ORM)                    │   │
│  │  - API classes with PygenClient reference           │   │
│  │  - Lazy iteration via client methods                │   │
│  │  - Query builder helpers                            │   │
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
from cognite.pygen._client.models import DataModelReference

client = PygenClient(
    base_url="https://api.cognitedata.com",
    credentials=...
)

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



```

### 2. Validation Layer

**Purpose**: Validate data models upfront, handle incomplete models gracefully, and provide clear warnings.

**Key Features**:
- Pre-parsing validation of CDF data models
- Detection of incomplete models (e.g., missing reverse relations)
- Warning generation with actionable suggestions
- Graceful degradation decisions (what to include/exclude)
- Clear error messages for users

**Structure**:
```
pygen/validation/
├── __init__.py
├── validator.py         # Main validation logic
├── rules.py             # Validation rules
├── warnings.py          # Warning types and messages
└── filters.py           # Filtering logic for incomplete models
```

**Validation Flow**:
```
1. Fetch data model from CDF
   ↓
2. Run validation rules
   ↓
3. Generate warnings for issues
   ↓
4. Decide what to include/exclude
   ↓
5. Pass cleaned model to IR parser
```

### 3. Intermediate Representation (IR)

**Purpose**: Create a language-agnostic representation of validated data models that can be transformed into any target language.

**Key Features**:
- Schema-independent type system
- Relationship and inheritance mapping
- Property metadata (required, nullable, default values)
- Documentation strings
- Only contains valid, processable models

**Structure**:
```
pygen/ir/
├── __init__.py
├── models.py            # IR model definitions
├── types.py             # Type system abstraction
├── parser.py            # Parse validated API models to IR
└── transformer.py       # Transform IR (e.g., flattening)
```

**IR Model Structure**:
```python
class IRType:
    """Abstract type representation"""
    name: str
    nullable: bool
    default: Any | None

class IRProperty:
    """Property representation"""
    name: str
    type: IRType
    description: str | None
    required: bool
    
class IRClass:
    """Class/View representation"""
    name: str
    properties: list[IRProperty]
    relationships: list[IRRelationship]
    parent: IRClass | None
    description: str | None

class IRModel:
    """Complete model representation"""
    name: str
    classes: list[IRClass]
    enums: list[IREnum]
    metadata: dict[str, Any]
```

### 4. Code Generation Engine

**Purpose**: Transform IR into target language code with proper formatting and conventions.

**Key Features**:
- Plugin architecture for language generators
- Template-based generation (Jinja2)
- Post-processing (formatting, linting)
- Incremental generation support
- Custom template support

**Structure**:
```
pygen/generation/
├── __init__.py
├── base.py              # Base generator class
├── template_env.py      # Jinja2 environment setup
├── python/              # Python generator
│   ├── __init__.py
│   ├── generator.py
│   ├── templates/
│   │   ├── api_class.py.jinja
│   │   ├── data_class.py.jinja
│   │   ├── filter_class.py.jinja
│   │   └── __init__.py.jinja
│   └── formatters.py
├── typescript/          # Future: TypeScript generator
│   └── ...
├── csharp/              # Future: C# generator
│   └── ...
└── pyspark/             # Future: PySpark generator
    └── ...
```

**Generator Interface**:
```python
class BaseGenerator(ABC):
    """Abstract base for language generators"""
    
    @abstractmethod
    def generate_class(self, ir_class: IRClass) -> str:
        """Generate code for a single class"""
        
    @abstractmethod
    def generate_module(self, ir_classes: list[IRClass]) -> str:
        """Generate code for a module"""
        
    @abstractmethod
    def format_code(self, code: str) -> str:
        """Format generated code"""
        
    @abstractmethod
    def get_file_extension(self) -> str:
        """Get file extension for this language"""
```

### 5. Generated SDK Runtime (Client-Based Design)

**Purpose**: Provide runtime support for generated SDKs using client-based pattern (not ORM-style).

**Key Features**:
- Client-based design (following Pygen v1 patterns)
- Generated API classes that wrap PygenClient
- Lazy iteration through client methods
- Query builder helpers for filtering and pagination
- No database-style ORM patterns

**Structure**:
```
pygen/runtime/
├── __init__.py
├── base_api.py          # Base class for generated API classes
├── data_classes.py      # Base for generated data classes
├── iterators.py         # Lazy iteration helpers
└── query_helpers.py     # Query building helpers
```

**Client-Based Design Pattern**:
```python
# Generated API class (client-based, not ORM)
class MyModelAPI:
    """Generated API class for MyModel"""
    
    def __init__(self, client: PygenClient):
        self._client = client
        self._view_id = ViewReference(space="my_space", external_id="MyModel", version="1")
    
    def list(self, limit: int | None = None) -> Iterator[MyModel]:
        """List instances with lazy iteration"""
        for item in self._client.instances.list(
            space=self._view_id.space,
            view=self._view_id,
            limit=limit
        ):
            yield MyModel.model_validate(item)
    
    def retrieve(self, external_id: str) -> MyModel | None:
        """Retrieve single instance"""
        item = self._client.instances.retrieve(
            space=self._view_id.space,
            view=self._view_id,
            external_id=external_id
        )
        return MyModel.model_validate(item) if item else None
    
    def filter(self, **filters) -> Iterator[MyModel]:
        """Filter instances"""
        query = self._client.query_builder(
            space=self._view_id.space,
            view=self._view_id
        ).filter(**filters)
        
        for item in query.execute():
            yield MyModel.model_validate(item)

# Data class (simple pydantic model, no ORM behavior)
class MyModel(BaseModel):
    """Generated data class"""
    external_id: str
    name: str
    value: float
```

## Data Flow

### Generation Flow
```
1. User provides data model specification
   ↓
2. Pygen Client fetches data model from CDF API
   ↓
3. Validation Layer validates model
   - Checks for incomplete models
   - Generates warnings
   - Filters out problematic elements
   ↓
4. Parser converts validated API models to IR
   ↓
5. Generator transforms IR to target language
   ↓
6. Formatter/linter processes generated code
   ↓
7. Output files written to disk (or returned via API)
```

### Runtime Flow (Client-Based)
```
1. User imports generated SDK
   ↓
2. User creates PygenClient instance
   ↓
3. User creates generated API class (passing client)
   ↓
4. User calls methods on API class (e.g., list(), filter())
   ↓
5. API class uses PygenClient to build query
   ↓
6. User iterates over results (lazy)
   ↓
7. PygenClient fetches data in chunks via HTTPClient
   ↓
8. Data deserialized into generated data classes
   ↓
9. User processes data
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

### 4. Why validation before IR?
- Catches issues early in the pipeline
- Allows graceful degradation decisions before IR creation
- Prevents invalid models from entering IR
- Better error messages for users
- Enables partial generation for incomplete models

### 5. Why IR layer?
- Decouples parsing from generation
- Enables multi-language support
- Easier to test each stage
- Allows IR transformations
- Version compatibility layer

### 6. Why client-based design (not ORM)?
- Maintains compatibility with Pygen v1 patterns
- Simpler mental model (explicit client usage)
- No hidden database-style magic
- Clear separation between data and operations
- Easier to test and mock
- More flexible for API-centric operations

### 7. Why template-based generation?
- Easy to customize
- Language-specific conventions
- Maintainable
- Community can contribute templates
- Clear separation of logic and output

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

## Documentation Strategy

- API reference (auto-generated from docstrings)
- User guides for each feature
- Architecture documentation (this doc)
- Contributing guide
- Examples and tutorials
- Migration guide

