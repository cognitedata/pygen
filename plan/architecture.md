# Pygen Rewrite - Architecture Design

## Overview

This document outlines the proposed architecture for the Pygen rewrite. The architecture is designed to address the four core problems: performance, lazy evaluation, multi-language support, and maintainability.

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
│  │  - HTTP client (httpx-based)                        │   │
│  │  - Authentication                                    │   │
│  │  - CRUD operations for DM concepts                  │   │
│  │  - Pydantic models for API objects                  │   │
│  └─────────────────────────────────────────────────────┘   │
│                           │                                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │         2. Intermediate Representation (IR)          │   │
│  │  - Language-agnostic model representation           │   │
│  │  - Type system abstraction                          │   │
│  │  - Relationship mapping                             │   │
│  │  - Schema validation                                │   │
│  └─────────────────────────────────────────────────────┘   │
│                           │                                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │         3. Code Generation Engine                    │   │
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
│  │         4. Generated Runtime Support                 │   │
│  │  - Base classes for generated code                  │   │
│  │  - Lazy evaluation mechanisms                       │   │
│  │  - Query builders                                   │   │
│  │  - Filtering/pagination helpers                     │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. Pygen Client (Runtime)

**Purpose**: Replace `cognite-sdk` with a lightweight, purpose-built client for data modeling operations.

**Key Features**:
- Built on `httpx` for modern async/sync support
- Pydantic models for all API objects (DataModel, View, Container, Space)
- Connection pooling and rate limiting
- Automatic retry logic with exponential backoff
- Streaming support for large datasets
- Full type hints for IDE support

**Structure**:
```
pygen/client/
├── __init__.py
├── core.py              # Main PygenClient class
├── auth.py              # Authentication handlers
├── http.py              # HTTP client wrapper (httpx)
├── models/              # Pydantic models for API objects
│   ├── __init__.py
│   ├── data_model.py
│   ├── view.py
│   ├── container.py
│   ├── space.py
│   ├── instance.py
│   └── query.py
├── resources/           # Resource-specific clients
│   ├── __init__.py
│   ├── data_models.py
│   ├── views.py
│   ├── containers.py
│   ├── spaces.py
│   └── instances.py
└── exceptions.py        # Custom exceptions
```

**API Design Example**:
```python
from pygen.client import PygenClient

client = PygenClient(
    base_url="https://api.cognitedata.com",
    credentials=...
)

# CRUD operations with pydantic models
data_model = client.data_models.retrieve(
    space="my_space",
    external_id="my_model"
)

# Lazy iteration over instances
for instance in client.instances.list(
    space="my_space",
    view_id="my_view",
    chunk_size=1000
):
    process(instance)
```

### 2. Intermediate Representation (IR)

**Purpose**: Create a language-agnostic representation of data models that can be transformed into any target language.

**Key Features**:
- Schema-independent type system
- Relationship and inheritance mapping
- Property metadata (required, nullable, default values)
- Validation rules
- Documentation strings

**Structure**:
```
pygen/ir/
├── __init__.py
├── models.py            # IR model definitions
├── types.py             # Type system abstraction
├── parser.py            # Parse API models to IR
├── validator.py         # Validate IR consistency
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

### 3. Code Generation Engine

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

### 4. Generated Runtime Support

**Purpose**: Provide base classes and utilities that generated code depends on.

**Key Features**:
- Lazy evaluation support
- Query building
- Filtering and pagination
- Connection management
- Serialization helpers

**Structure**:
```
pygen/runtime/
├── __init__.py
├── base.py              # Base classes for generated code
├── lazy.py              # Lazy evaluation support
├── query.py             # Query builder
├── filters.py           # Filter classes
├── pagination.py        # Pagination helpers
└── serialization.py     # Serialization utilities
```

**Base Class Design**:
```python
class PygenResource(BaseModel):
    """Base class for all generated resources"""
    _client: PygenClient | None = None
    
    def refresh(self) -> Self:
        """Reload data from API"""
        
    def save(self) -> Self:
        """Persist changes to API"""
        
    def delete(self) -> None:
        """Delete this resource"""

class PygenRelation(Generic[T]):
    """Lazy relationship handler"""
    _client: PygenClient
    _query: Query
    _cached: list[T] | None = None
    
    def __iter__(self) -> Iterator[T]:
        """Lazy iteration over related objects"""
        
    def all(self) -> list[T]:
        """Eager load all related objects"""
        
    def filter(self, **kwargs) -> Self:
        """Add filters to query"""
```

## Data Flow

### Generation Flow
```
1. User provides data model specification
   ↓
2. Pygen Client fetches data model from API
   ↓
3. Parser converts API models to IR
   ↓
4. Validator checks IR consistency
   ↓
5. Generator transforms IR to target language
   ↓
6. Formatter/linter processes generated code
   ↓
7. Output files written to disk
```

### Runtime Flow
```
1. User imports generated SDK
   ↓
2. User creates client instance
   ↓
3. User calls API methods on generated classes
   ↓
4. Query is built (not executed)
   ↓
5. User iterates over results
   ↓
6. PygenClient fetches data in chunks
   ↓
7. Data deserialized into generated classes
   ↓
8. User processes data
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

### 4. Why IR layer?
- Decouples parsing from generation
- Enables multi-language support
- Easier to test each stage
- Allows IR transformations
- Version compatibility layer

### 5. Why template-based generation?
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
3. Update IR if needed
4. Update generators if new patterns needed
5. Add tests

### Custom Templates
1. Users can provide custom template directory
2. Templates override defaults
3. Full access to IR objects in templates

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

