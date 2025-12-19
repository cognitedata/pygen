# Pygen Rewrite - Technical Specifications

## Overview

This document provides detailed technical specifications for key components of the Pygen rewrite. It serves as a reference for implementation and ensures consistency across the codebase.

---

## 1. Pygen Client API

### 1.1 PygenClient Class

**Purpose**: Main entry point for interacting with CDF Data Modeling API.

**Interface**:

```python
class PygenClient:
    """
    Client for CDF Data Modeling API.
    
    Args:
        base_url: Base URL for CDF API (e.g., "https://api.cognitedata.com")
        project: CDF project name
        credentials: Authentication credentials (token, client credentials, etc.)
        timeout: Request timeout in seconds (default: 30)
        max_retries: Maximum number of retries for failed requests (default: 3)
        rate_limit: Maximum requests per second (default: None)
        connection_pool_size: HTTP connection pool size (default: 10)
        
    Example:
        >>> client = PygenClient(
        ...     base_url="https://api.cognitedata.com",
        ...     project="my-project",
        ...     credentials=TokenAuth("my-token")
        ... )
        >>> data_model = client.data_models.retrieve("my_space", "my_model", "1")
    """
    
    def __init__(
        self,
        base_url: str,
        project: str,
        credentials: Credentials,
        timeout: int = 30,
        max_retries: int = 3,
        rate_limit: int | None = None,
        connection_pool_size: int = 10,
    ) -> None: ...
    
    @property
    def spaces(self) -> SpacesAPI:
        """Access to Spaces API"""
        
    @property
    def data_models(self) -> DataModelsAPI:
        """Access to Data Models API"""
        
    @property
    def views(self) -> ViewsAPI:
        """Access to Views API"""
        
    @property
    def containers(self) -> ContainersAPI:
        """Access to Containers API"""
        
    @property
    def instances(self) -> InstancesAPI:
        """Access to Instances API"""
        
    def close(self) -> None:
        """Close HTTP connections"""
        
    def __enter__(self) -> Self:
        """Context manager support"""
        
    def __exit__(self, *args) -> None:
        """Context manager support"""
```

### 1.2 Authentication

**Supported Auth Methods**:

```python
class Credentials(ABC):
    """Base class for authentication"""
    
    @abstractmethod
    def get_headers(self) -> dict[str, str]:
        """Return authentication headers"""


class TokenAuth(Credentials):
    """Token-based authentication"""
    
    def __init__(self, token: str) -> None:
        self.token = token
    
    def get_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}


class ClientCredentials(Credentials):
    """OAuth2 client credentials flow"""
    
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_url: str,
        scopes: list[str] | None = None,
    ) -> None: ...
    
    def get_headers(self) -> dict[str, str]:
        # Handles token refresh automatically
        ...


class EnvironmentAuth(Credentials):
    """Read credentials from environment variables"""
    
    def __init__(self) -> None:
        # Reads from CDF_TOKEN, CDF_CLIENT_ID, etc.
        ...
```

### 1.3 Resource APIs

#### SpacesAPI

```python
class SpacesAPI:
    """API for managing Spaces"""
    
    def list(
        self,
        limit: int | None = None,
    ) -> Iterator[Space]:
        """
        List all spaces.
        
        Args:
            limit: Maximum number of spaces to return
            
        Returns:
            Iterator over Space objects
        """
    
    def retrieve(self, space: str) -> Space:
        """
        Retrieve a space by name.
        
        Args:
            space: Space name
            
        Returns:
            Space object
            
        Raises:
            SpaceNotFoundError: If space doesn't exist
        """
    
    def create(self, space: Space) -> Space:
        """
        Create a new space.
        
        Args:
            space: Space object to create
            
        Returns:
            Created Space object
            
        Raises:
            SpaceAlreadyExistsError: If space already exists
        """
    
    def delete(self, space: str) -> None:
        """
        Delete a space.
        
        Args:
            space: Space name
            
        Raises:
            SpaceNotFoundError: If space doesn't exist
        """
```

#### DataModelsAPI

```python
class DataModelsAPI:
    """API for managing Data Models"""
    
    def list(
        self,
        space: str | None = None,
        limit: int | None = None,
        include_global: bool = False,
    ) -> Iterator[DataModel]:
        """List data models"""
    
    def retrieve(
        self,
        space: str,
        external_id: str,
        version: str,
    ) -> DataModel:
        """Retrieve a data model"""
    
    def create(self, data_model: DataModel) -> DataModel:
        """Create a new data model"""
    
    def delete(
        self,
        space: str,
        external_id: str,
        version: str,
    ) -> None:
        """Delete a data model"""
```

#### ViewsAPI

```python
class ViewsAPI:
    """API for managing Views"""
    
    def list(
        self,
        space: str | None = None,
        limit: int | None = None,
    ) -> Iterator[View]:
        """List views"""
    
    def retrieve(
        self,
        space: str,
        external_id: str,
        version: str,
    ) -> View:
        """Retrieve a view"""
    
    def create(self, view: View) -> View:
        """Create a new view"""
    
    def delete(
        self,
        space: str,
        external_id: str,
        version: str,
    ) -> None:
        """Delete a view"""
```

### 1.4 Pydantic Models

#### Space

```python
class Space(BaseModel):
    """Represents a CDF Space"""
    
    space: str = Field(..., description="Space name")
    name: str | None = Field(None, description="Display name")
    description: str | None = Field(None, description="Space description")
    created_time: datetime = Field(..., description="Creation timestamp")
    last_updated_time: datetime = Field(..., description="Last update timestamp")
```

#### DataModel

```python
class DataModel(BaseModel):
    """Represents a CDF Data Model"""
    
    space: str
    external_id: str
    version: str
    name: str | None = None
    description: str | None = None
    views: list[ViewReference]
    created_time: datetime
    last_updated_time: datetime
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "space": "my_space",
                "external_id": "my_model",
                "version": "1",
                "name": "My Model",
                "views": [...]
            }
        }
    )
```

#### View

```python
class ViewReference(BaseModel):
    """Reference to a view"""
    space: str
    external_id: str
    version: str


class PropertyType(str, Enum):
    """Property types supported by CDF"""
    TEXT = "text"
    BOOLEAN = "boolean"
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    INT32 = "int32"
    INT64 = "int64"
    TIMESTAMP = "timestamp"
    DATE = "date"
    JSON = "json"
    TIMESERIES = "timeseries"
    FILE = "file"
    SEQUENCE = "sequence"


class PropertyDefinition(BaseModel):
    """Property definition"""
    type: PropertyType | ViewReference
    nullable: bool = False
    auto_increment: bool = False
    immutable: bool = False
    default_value: Any | None = None
    description: str | None = None
    name: str | None = None  # Display name


class View(BaseModel):
    """Represents a CDF View"""
    
    space: str
    external_id: str
    version: str
    name: str | None = None
    description: str | None = None
    implements: list[ViewReference] = Field(default_factory=list)
    properties: dict[str, PropertyDefinition]
    filter: dict[str, Any] | None = None
    created_time: datetime
    last_updated_time: datetime
```

---

## 2. Intermediate Representation (IR)

### 2.1 Type System

```python
class IRPrimitiveType(str, Enum):
    """Primitive types in IR"""
    STRING = "string"
    INT = "int"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    DATE = "date"
    JSON = "json"
    BYTES = "bytes"


@dataclass(frozen=True)
class IRType:
    """Base class for all IR types"""
    nullable: bool = False


@dataclass(frozen=True)
class IRPrimitive(IRType):
    """Primitive type"""
    type: IRPrimitiveType


@dataclass(frozen=True)
class IRList(IRType):
    """List type"""
    element_type: IRType


@dataclass(frozen=True)
class IRReference(IRType):
    """Reference to another class"""
    target: str  # Fully qualified class name


@dataclass(frozen=True)
class IREnum(IRType):
    """Enum type"""
    name: str
    values: list[str]
```

### 2.2 IR Models

```python
@dataclass
class IRProperty:
    """Represents a property in IR"""
    
    name: str
    type: IRType
    description: str | None = None
    required: bool = True
    default: Any | None = None
    
    # Metadata
    original_name: str | None = None  # Original name from API
    read_only: bool = False
    write_only: bool = False


@dataclass
class IRRelationship:
    """Represents a relationship between classes"""
    
    name: str
    target: str  # Target class name
    cardinality: Literal["one", "many"]
    description: str | None = None
    reverse_name: str | None = None  # Name of reverse relationship
    required: bool = False


@dataclass
class IRMethod:
    """Represents a method to be generated"""
    
    name: str
    parameters: list[IRProperty]
    return_type: IRType | None
    description: str | None = None
    
    # Method type
    is_query: bool = False
    is_mutation: bool = False
    is_class_method: bool = False
    is_static_method: bool = False


@dataclass
class IRClass:
    """Represents a class/view in IR"""
    
    name: str
    properties: list[IRProperty]
    relationships: list[IRRelationship]
    methods: list[IRMethod]
    parent: str | None = None  # Parent class name
    description: str | None = None
    
    # Metadata
    space: str
    external_id: str
    version: str
    is_abstract: bool = False


@dataclass
class IRModule:
    """Represents a module (collection of classes)"""
    
    name: str
    classes: list[IRClass]
    enums: list[IREnum]
    imports: list[str]
    description: str | None = None


@dataclass
class IRModel:
    """Top-level IR model"""
    
    name: str
    modules: list[IRModule]
    metadata: dict[str, Any] = field(default_factory=dict)
```

### 2.3 Parser

```python
class IRParser:
    """Parse CDF models to IR"""
    
    def parse_data_model(self, data_model: DataModel) -> IRModel:
        """
        Parse a DataModel to IR.
        
        Args:
            data_model: DataModel from API
            
        Returns:
            IRModel representation
            
        Raises:
            ParseError: If model cannot be parsed
        """
    
    def parse_view(self, view: View) -> IRClass:
        """Parse a View to IRClass"""
    
    def parse_property(
        self,
        name: str,
        prop: PropertyDefinition,
    ) -> IRProperty:
        """Parse a property definition"""
    
    def _map_type(self, cdf_type: PropertyType | ViewReference) -> IRType:
        """Map CDF type to IR type"""
```

### 2.4 Validator

```python
class IRValidator:
    """Validate IR consistency"""
    
    def validate(self, model: IRModel) -> list[ValidationError]:
        """
        Validate an IR model.
        
        Returns:
            List of validation errors (empty if valid)
        """
    
    def _check_naming_conflicts(self, model: IRModel) -> list[ValidationError]:
        """Check for naming conflicts"""
    
    def _check_type_references(self, model: IRModel) -> list[ValidationError]:
        """Check that all type references are valid"""
    
    def _check_relationships(self, model: IRModel) -> list[ValidationError]:
        """Check relationship consistency"""
    
    def _check_circular_dependencies(self, model: IRModel) -> list[ValidationError]:
        """Check for circular dependencies"""


@dataclass
class ValidationError:
    """Represents a validation error"""
    
    severity: Literal["error", "warning"]
    message: str
    location: str  # e.g., "module.class.property"
    suggestion: str | None = None
```

---

## 3. Code Generation

### 3.1 Generator Interface

```python
class BaseGenerator(ABC):
    """Abstract base class for language generators"""
    
    @abstractmethod
    def generate(
        self,
        model: IRModel,
        output_dir: Path,
        config: GeneratorConfig | None = None,
    ) -> GenerationResult:
        """
        Generate code from IR model.
        
        Args:
            model: IR model to generate from
            output_dir: Directory to write generated files
            config: Generator configuration
            
        Returns:
            Generation result with statistics
        """
    
    @abstractmethod
    def generate_class(self, ir_class: IRClass) -> str:
        """Generate code for a single class"""
    
    @abstractmethod
    def generate_module(self, ir_module: IRModule) -> str:
        """Generate code for a module"""
    
    @abstractmethod
    def format_code(self, code: str) -> str:
        """Format generated code"""
    
    @abstractmethod
    def get_file_extension(self) -> str:
        """Get file extension for this language"""


@dataclass
class GeneratorConfig:
    """Configuration for code generation"""
    
    # Naming conventions
    class_name_format: Literal["PascalCase", "snake_case", "camelCase"] = "PascalCase"
    method_name_format: Literal["snake_case", "camelCase"] = "snake_case"
    property_name_format: Literal["snake_case", "camelCase"] = "snake_case"
    
    # Code style
    line_length: int = 100
    indent: str = "    "  # 4 spaces
    
    # Features
    generate_docstrings: bool = True
    generate_type_hints: bool = True
    generate_examples: bool = False
    
    # Output
    single_file: bool = False
    flat_structure: bool = False
    
    # Custom templates
    template_dir: Path | None = None


@dataclass
class GenerationResult:
    """Result of code generation"""
    
    files_written: list[Path]
    lines_generated: int
    classes_generated: int
    duration: float  # seconds
    warnings: list[str] = field(default_factory=list)
```

### 3.2 Python Generator

```python
class PythonGenerator(BaseGenerator):
    """Generator for Python code"""
    
    def __init__(self, config: GeneratorConfig | None = None) -> None:
        self.config = config or GeneratorConfig()
        self.template_env = self._setup_template_env()
    
    def generate(
        self,
        model: IRModel,
        output_dir: Path,
        config: GeneratorConfig | None = None,
    ) -> GenerationResult:
        """Generate Python SDK"""
    
    def generate_class(self, ir_class: IRClass) -> str:
        """Generate Python class"""
    
    def generate_api_class(self, ir_class: IRClass) -> str:
        """Generate API client class"""
    
    def generate_data_class(self, ir_class: IRClass) -> str:
        """Generate data class (pydantic model)"""
    
    def generate_filter_class(self, ir_class: IRClass) -> str:
        """Generate filter class for queries"""
    
    def format_code(self, code: str) -> str:
        """Format using ruff"""
    
    def _setup_template_env(self) -> jinja2.Environment:
        """Setup Jinja2 environment"""
```

### 3.3 Template Structure

```
templates/python/
├── __init__.py.jinja
├── data_class.py.jinja
├── api_class.py.jinja
├── filter_class.py.jinja
├── query_class.py.jinja
├── macros/
│   ├── docstring.jinja
│   ├── type_hints.jinja
│   └── imports.jinja
└── custom/
    └── # User can override with custom templates
```

---

## 4. Runtime Support

### 4.1 Base Classes

```python
class PygenResource(BaseModel):
    """Base class for all generated data classes"""
    
    model_config = ConfigDict(
        validate_assignment=True,
        arbitrary_types_allowed=True,
    )
    
    # Private client reference (not serialized)
    _client: PygenClient | None = PrivateAttr(default=None)
    
    def set_client(self, client: PygenClient) -> Self:
        """Set client for lazy operations"""
        self._client = client
        return self
    
    def refresh(self) -> Self:
        """Reload from API"""
        if self._client is None:
            raise RuntimeError("Client not set")
        # Implementation depends on resource type
        ...
    
    def save(self) -> Self:
        """Save changes to API"""
        if self._client is None:
            raise RuntimeError("Client not set")
        ...
    
    def delete(self) -> None:
        """Delete this resource"""
        if self._client is None:
            raise RuntimeError("Client not set")
        ...


class PygenRelation(Generic[T]):
    """Lazy relationship handler"""
    
    def __init__(
        self,
        client: PygenClient,
        query: Query,
        result_type: type[T],
    ) -> None:
        self._client = client
        self._query = query
        self._result_type = result_type
        self._cached: list[T] | None = None
    
    def __iter__(self) -> Iterator[T]:
        """Lazy iteration"""
        for item in self._client.instances.query(self._query):
            yield self._result_type.model_validate(item)
    
    def __len__(self) -> int:
        """Get count (requires API call)"""
        return self._query.count()
    
    def __getitem__(self, index: int | slice) -> T | list[T]:
        """Get by index (loads if needed)"""
        if self._cached is None:
            self._cached = list(self)
        return self._cached[index]
    
    def all(self) -> list[T]:
        """Eager load all items"""
        if self._cached is None:
            self._cached = list(self)
        return self._cached
    
    def filter(self, **kwargs) -> Self:
        """Add filters to query"""
        new_query = self._query.filter(**kwargs)
        return PygenRelation(self._client, new_query, self._result_type)
    
    def first(self) -> T | None:
        """Get first item"""
        for item in self:
            return item
        return None
```

### 4.2 Query Builder

```python
class Query:
    """Query builder for instances"""
    
    def __init__(
        self,
        client: PygenClient,
        space: str,
        view: ViewReference,
    ) -> None:
        self._client = client
        self._space = space
        self._view = view
        self._filters: list[Filter] = []
        self._limit: int | None = None
        self._sort: list[Sort] = []
    
    def filter(self, **kwargs) -> Self:
        """Add filters"""
        for key, value in kwargs.items():
            self._filters.append(Filter(key, "equals", value))
        return self
    
    def limit(self, n: int) -> Self:
        """Limit results"""
        self._limit = n
        return self
    
    def sort(self, *fields: str) -> Self:
        """Sort results"""
        for field in fields:
            desc = field.startswith("-")
            field_name = field[1:] if desc else field
            self._filters.append(Sort(field_name, "desc" if desc else "asc"))
        return self
    
    def execute(self) -> Iterator[dict[str, Any]]:
        """Execute query and return results"""
        # Build CDF query
        cdf_query = self._build_query()
        
        # Execute with pagination
        yield from self._client.instances.query(cdf_query)
    
    def count(self) -> int:
        """Get count of matching items"""
        ...
    
    def _build_query(self) -> dict[str, Any]:
        """Build CDF API query"""
        ...
```

---

## 5. Configuration

### 5.1 Configuration File

```yaml
# pygen.yaml

# Client configuration
client:
  base_url: https://api.cognitedata.com
  project: my-project
  timeout: 30
  max_retries: 3

# Generation configuration
generation:
  output_dir: ./generated
  language: python
  naming:
    class_format: PascalCase
    method_format: snake_case
    property_format: snake_case
  style:
    line_length: 100
    indent: "    "
  features:
    docstrings: true
    type_hints: true
    examples: false

# Model specification
model:
  space: my_space
  external_id: my_model
  version: "1"
  include_views:
    - "*"
  exclude_views: []
```

### 5.2 Configuration Loading

```python
@dataclass
class PygenConfig:
    """Complete Pygen configuration"""
    
    client: ClientConfig
    generation: GeneratorConfig
    model: ModelConfig
    
    @classmethod
    def from_file(cls, path: Path) -> Self:
        """Load configuration from YAML file"""
        ...
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Load configuration from dictionary"""
        ...
```

---

## 6. CLI Interface

```python
# pygen/cli.py

import click

@click.group()
@click.version_option()
def cli():
    """Pygen - Generate SDKs from CDF Data Models"""
    pass


@cli.command()
@click.option("--config", "-c", type=click.Path(), help="Configuration file")
@click.option("--space", help="Space name")
@click.option("--model", help="Data model external ID")
@click.option("--version", help="Data model version")
@click.option("--output", "-o", type=click.Path(), help="Output directory")
@click.option("--language", type=click.Choice(["python", "typescript"]), default="python")
def generate(config, space, model, version, output, language):
    """Generate SDK from data model"""
    ...


@cli.command()
@click.argument("space")
@click.argument("model")
@click.option("--version", default="1")
def inspect(space, model, version):
    """Inspect a data model"""
    ...


@cli.command()
def validate():
    """Validate configuration file"""
    ...


if __name__ == "__main__":
    cli()
```

---

## 7. Error Handling

### 7.1 Exception Hierarchy

```python
class PygenError(Exception):
    """Base exception for Pygen"""
    pass


# Client errors
class ClientError(PygenError):
    """Base class for client errors"""
    pass


class AuthenticationError(ClientError):
    """Authentication failed"""
    pass


class APIError(ClientError):
    """API request failed"""
    
    def __init__(self, message: str, status_code: int, response: dict | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class ResourceNotFoundError(APIError):
    """Resource not found (404)"""
    pass


class RateLimitError(APIError):
    """Rate limit exceeded (429)"""
    pass


# Generation errors
class GenerationError(PygenError):
    """Base class for generation errors"""
    pass


class ParseError(GenerationError):
    """Failed to parse model"""
    pass


class ValidationError(GenerationError):
    """IR validation failed"""
    
    def __init__(self, errors: list[ValidationError]):
        self.errors = errors
        messages = [f"{e.location}: {e.message}" for e in errors]
        super().__init__("\n".join(messages))


class TemplateError(GenerationError):
    """Template rendering failed"""
    pass
```

---

## 8. Performance Targets

### 8.1 Client Performance

- **Requests per second**: >100 (with connection pooling)
- **Latency**: <100ms (p95) for single item retrieval
- **Memory**: <50MB for client instance
- **Concurrent requests**: Support 10+ concurrent requests

### 8.2 Generation Performance

- **Small model** (<10 views): <5 seconds
- **Medium model** (10-50 views): <30 seconds
- **Large model** (50-200 views): <2 minutes
- **Memory**: <500MB during generation

### 8.3 Runtime Performance

- **Lazy iteration overhead**: <5% vs direct API calls
- **Query building**: <1ms
- **Serialization**: >10,000 objects/second
- **Memory**: O(chunk_size) not O(total_size)

---

## 9. Versioning & Compatibility

### 9.1 Semantic Versioning

Follow semantic versioning (MAJOR.MINOR.PATCH):
- **MAJOR**: Breaking changes
- **MINOR**: New features (backward compatible)
- **PATCH**: Bug fixes

### 9.2 API Compatibility

- Maintain backward compatibility within major version
- Deprecation warnings for 1 major version
- Clear migration guides for breaking changes

### 9.3 Generated Code Versioning

Include version metadata in generated code:

```python
# Generated by Pygen v2.0.0
__pygen_version__ = "2.0.0"
__model_version__ = "1"
__generated_at__ = "2025-01-15T10:30:00Z"
```

---

## 10. Security

### 10.1 Credential Handling

- Never log credentials
- Support environment variables
- Support credential files (with proper permissions)
- Support secret managers (Azure Key Vault, AWS Secrets Manager)

### 10.2 Input Validation

- Validate all user inputs
- Sanitize template inputs
- Check file paths for traversal attacks
- Validate API responses

### 10.3 Dependencies

- Regular security audits
- Pin dependency versions
- Use only trusted packages
- Regular updates

---

## Appendix

### A. Type Mapping

| CDF Type | IR Type | Python Type | TypeScript Type |
|----------|---------|-------------|-----------------|
| text | STRING | str | string |
| boolean | BOOLEAN | bool | boolean |
| float32 | FLOAT | float | number |
| float64 | FLOAT | float | number |
| int32 | INT | int | number |
| int64 | INT | int | number |
| timestamp | DATETIME | datetime | Date |
| date | DATE | date | Date |
| json | JSON | dict[str, Any] | Record<string, any> |

### B. Reserved Words

Python reserved words that need escaping:
```python
PYTHON_RESERVED = {
    "False", "None", "True", "and", "as", "assert", "async", "await",
    "break", "class", "continue", "def", "del", "elif", "else", "except",
    "finally", "for", "from", "global", "if", "import", "in", "is",
    "lambda", "nonlocal", "not", "or", "pass", "raise", "return",
    "try", "while", "with", "yield",
}
```

Strategy: Append underscore (e.g., `class` → `class_`)

