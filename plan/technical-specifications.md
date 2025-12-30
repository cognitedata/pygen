# Pygen Rewrite - Technical Specifications

## Overview

This document provides detailed technical specifications for key components of the Pygen rewrite. It serves as a reference for implementation and ensures consistency across the codebase.

## Architectural Approach

The Pygen rewrite follows a **"concrete-first, abstract-later"** approach:

1. **Phases 2-3**: Build generic InstanceAPI and InstanceClient base classes with hand-written example SDKs
   - Phase 2: Python implementation with examples
   - Phase 3: TypeScript implementation with examples
   - This establishes proven patterns before codifying them

2. **Phase 4**: Create Intermediate Representation (IR) based on proven patterns
   - IR is designed to generate code that matches Phases 2-3 patterns
   - Validation layer ensures clean input to IR

3. **Phase 5**: Build generators that produce code extending generic classes
   - Python generator creates classes extending Python InstanceAPI
   - TypeScript generator creates classes extending TypeScript InstanceAPI
   - Generated code follows the same patterns as hand-written examples

This approach ensures the IR and generators produce practical, maintainable code based on real-world patterns.

---

## 1. Pygen Client API

### 1.1 HTTPClient (Internal Wrapper)

**Purpose**: Internal wrapper around httpx for consistent HTTP operations.

**Interface**:

```python
class HTTPClient:
    """
    Internal HTTP client wrapper around httpx.
    
    Provides consistent interface for HTTP operations with:
    - Connection pooling
    - Rate limiting
    - Retry logic
    - Request/response logging
    
    Args:
        base_url: Base URL for API
        timeout: Request timeout in seconds
        max_retries: Maximum number of retries
        rate_limit: Maximum requests per second
        pool_size: Connection pool size
    """
    
    def __init__(
        self,
        base_url: str,
        timeout: int = 30,
        max_retries: int = 3,
        rate_limit: int | None = None,
        pool_size: int = 10,
    ) -> None: ...
    
    def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response: ...
    
    def post(
        self,
        path: str,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response: ...
    
    def put(
        self,
        path: str,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response: ...
    
    def delete(
        self,
        path: str,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response: ...
    
    def close(self) -> None:
        """Close HTTP connections"""
    
    def __enter__(self) -> Self:
        """Context manager support"""
    
    def __exit__(self, *args) -> None:
        """Context manager support"""
```

### 1.2 PygenClient Class

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
    ) -> None:
        # Initializes internal HTTPClient
        self._http_client = HTTPClient(
            base_url=base_url,
            timeout=timeout,
            max_retries=max_retries,
            rate_limit=rate_limit,
            pool_size=connection_pool_size,
        )
        ...
    
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
    
    def query_builder(self, space: str, view: str | ViewReference) -> QueryBuilder:
        """Create a query builder for simplified querying"""
        
    def close(self) -> None:
        """Close HTTP connections"""
        
    def __enter__(self) -> Self:
        """Context manager support"""
        
    def __exit__(self, *args) -> None:
        """Context manager support"""
```

### 1.3 Query Builder

**Purpose**: Simplify complex query construction and optimize API calls.

**Interface**:

```python
class QueryBuilder:
    """
    Query builder for constructing and executing CDF queries.
    
    Simplifies complex query patterns common in Pygen use cases.
    """
    
    def __init__(
        self,
        client: HTTPClient,
        space: str,
        view: ViewReference,
    ) -> None: ...
    
    def filter(self, **kwargs) -> Self:
        """Add filters to query"""
        
    def select(self, *properties: str) -> Self:
        """Select specific properties"""
        
    def limit(self, n: int) -> Self:
        """Limit number of results"""
        
    def sort(self, *fields: str) -> Self:
        """Sort results (prefix with - for descending)"""
        
    def execute(self) -> Iterator[dict[str, Any]]:
        """Execute query and return lazy iterator"""
        
    def count(self) -> int:
        """Get count of matching results"""
```

### 1.4 Authentication

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

### 1.5 Resource APIs

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

### 1.6 Pydantic Models

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

## 2. Generic Instance API (Python)

### 2.1 InstanceModel Base Classes

**Purpose**: Generic base classes for all CDF instances (nodes and edges).

**Interface**:

```python
from pydantic import BaseModel, Field
from typing import Literal, ClassVar

class ViewRef(BaseModel):
    """Reference to a view"""
    space: str
    external_id: str = Field(alias="externalId")
    version: str

class DataRecord(BaseModel):
    """Metadata for an instance"""
    version: int
    last_updated_time: datetime = Field(alias="lastUpdatedTime")
    created_time: datetime = Field(alias="createdTime")
    deleted_time: datetime | None = Field(None, alias="deletedTime")

class InstanceModel(BaseModel):
    """Base for all instance models"""
    _view_id: ClassVar[ViewRef]
    instance_type: Literal["node", "edge"] = Field(alias="instanceType")
    space: str
    external_id: str = Field(alias="externalId")

class Instance(InstanceModel):
    """Base for read instances"""
    data_record: DataRecord

class InstanceWrite(InstanceModel):
    """Base for write instances"""
    data_record: DataRecordWrite = Field(default_factory=DataRecordWrite)

class InstanceList(UserList[T_Instance]):
    """Generic list of instances with pagination support"""
    def dump(self, camel_case: bool = True) -> list[dict]: ...
    def to_pandas(self, dropna_columns: bool = False) -> pd.DataFrame: ...
```

### 2.2 InstanceClient

**Purpose**: Generic client for instance CRUD operations.

**Interface**:

```python
class InstanceClient:
    """Generic client for CDF instance operations"""
    
    def __init__(self, config: PygenClientConfig) -> None:
        self._config = config
        self._http_client = HTTPClient(config)
    
    def upsert(
        self,
        items: Sequence[InstanceWrite],
        mode: Literal["update", "create"],
    ) -> InstanceResult:
        """
        Create or update instances.
        
        Args:
            items: Instances to upsert
            mode: "create" to fail if exists, "update" to merge
            
        Returns:
            Result with created/updated/unchanged items
        """
    
    def delete(
        self,
        items: Sequence[InstanceWrite | InstanceId]
    ) -> InstanceResult:
        """Delete instances"""
```

### 2.3 InstanceAPI

**Purpose**: Generic base class for view-specific API operations.

**Interface**:

```python
class InstanceAPI(Generic[T_InstanceWrite, T_Instance, T_InstanceList]):
    """Generic API for view-specific operations"""
    
    def __init__(
        self,
        http_client: HTTPClient,
        view_ref: ViewRef,
        instance_type: Literal["node", "edge"]
    ) -> None:
        self._http_client = http_client
        self._view_ref = view_ref
        self._instance_type = instance_type
    
    def retrieve(
        self,
        id: str | InstanceId | list[str | InstanceId],
        space: str | None = None,
    ) -> T_Instance | T_InstanceList | None:
        """Retrieve instances by ID"""
    
    def list(self) -> T_InstanceList:
        """List all instances with lazy iteration"""
    
    def iterate(self) -> Page[T_InstanceList]:
        """Fetch a single page of instances"""
    
    def search(
        self,
        query: str,
        properties: list[str] | None = None,
    ) -> T_InstanceList:
        """Full-text search"""
    
    def aggregate(self, ...) -> AggregateResult:
        """Aggregate instances"""
```

### 2.4 Example Usage (Hand-Written)

```python
# Example data class extending Instance
class PrimitiveNullable(Instance):
    _view_id = ViewRef(
        space="example_space",
        external_id="PrimitiveNullable",
        version="v1"
    )
    
    text: str | None = None
    boolean: bool | None = None
    float32: float | None = None
    int64: int | None = None

class PrimitiveNullableWrite(InstanceWrite):
    # Same fields as read class
    ...

class PrimitiveNullableList(InstanceList[PrimitiveNullable]):
    _INSTANCE = PrimitiveNullable

# Example API class extending InstanceAPI
class PrimitiveNullableAPI(
    InstanceAPI[PrimitiveNullableWrite, PrimitiveNullable, PrimitiveNullableList]
):
    """API for PrimitiveNullable instances"""
    # Inherits all methods with proper types

# Example client extending InstanceClient
class ExampleClient(InstanceClient):
    def __init__(self, config: PygenClientConfig):
        super().__init__(config)
        self.primitive_nullable = PrimitiveNullableAPI(
            self._http_client,
            view_ref=PrimitiveNullable._view_id,
            instance_type="node"
        )
```

---

## 3. Generic Instance API (TypeScript)

### 3.1 Instance Interfaces

```typescript
interface ViewRef {
  space: string;
  externalId: string;
  version: string;
}

interface DataRecord {
  version: number;
  lastUpdatedTime: number;
  createdTime: number;
  deletedTime?: number;
}

interface Instance {
  instanceType: "node" | "edge";
  space: string;
  externalId: string;
  dataRecord: DataRecord;
}

interface InstanceWrite {
  instanceType: "node" | "edge";
  space: string;
  externalId: string;
  dataRecord?: DataRecordWrite;
}
```

### 3.2 InstanceClient (TypeScript)

```typescript
class InstanceClient {
  constructor(config: PygenClientConfig) {
    this._config = config;
    this._httpClient = new HTTPClient(config);
  }
  
  async upsert(
    items: InstanceWrite[],
    mode: "update" | "create"
  ): Promise<InstanceResult> {
    // ...
  }
  
  async delete(items: (InstanceWrite | InstanceId)[]): Promise<InstanceResult> {
    // ...
  }
}
```

### 3.3 InstanceAPI (TypeScript)

```typescript
class InstanceAPI<TWrite extends InstanceWrite, TRead extends Instance, TList> {
  constructor(
    httpClient: HTTPClient,
    viewRef: ViewRef,
    instanceType: "node" | "edge"
  ) {
    this._httpClient = httpClient;
    this._viewRef = viewRef;
    this._instanceType = instanceType;
  }
  
  async retrieve(id: string | string[]): Promise<TRead | TList | null> {
    // ...
  }
  
  async *list(): AsyncIterableIterator<TRead> {
    // Lazy iteration with async generator
  }
  
  async iterate(): Promise<Page<TList>> {
    // ...
  }
}
```

---

## 4. Validation Layer (Phase 4)

### 4.1 Validator

**Purpose**: Validate CDF data models before IR creation, handle incomplete models gracefully.

**Interface**:

```python
class DataModelValidator:
    """
    Validates CDF data models before IR creation.
    
    Detects issues like:
    - Missing reverse relations
    - Invalid type references
    - Circular dependencies
    - Naming conflicts
    """
    
    def validate(
        self,
        data_model: DataModel,
        strict: bool = False,
    ) -> ValidationResult:
        """
        Validate a data model.
        
        Args:
            data_model: DataModel to validate
            strict: If True, treat warnings as errors
            
        Returns:
            ValidationResult with errors, warnings, and filtered model
        """
    
    def check_relationships(self, data_model: DataModel) -> list[ValidationIssue]:
        """Check relationship consistency"""
    
    def check_type_references(self, data_model: DataModel) -> list[ValidationIssue]:
        """Check type references are valid"""
    
    def check_circular_dependencies(self, data_model: DataModel) -> list[ValidationIssue]:
        """Check for circular dependencies"""


@dataclass
class ValidationIssue:
    """Represents a validation issue"""
    
    severity: Literal["error", "warning", "info"]
    code: str  # e.g., "MISSING_REVERSE_RELATION"
    message: str
    location: str  # e.g., "my_space/MyView/my_property"
    suggestion: str | None = None
    can_auto_fix: bool = False


@dataclass
class ValidationResult:
    """Result of validation"""
    
    errors: list[ValidationIssue]
    warnings: list[ValidationIssue]
    filtered_model: DataModel  # Model with problematic elements removed
    is_valid: bool
    
    def print_summary(self) -> None:
        """Print human-readable summary"""
```

### 4.2 Validation Rules

Common validation rules include:

1. **Relationship Validation**
   - Direct relation must have reverse
   - Reverse relation must point to valid direct relation
   - Relationship targets must exist

2. **Type Validation**
   - All type references must point to valid types
   - No undefined custom types
   - Primitive types must be supported

3. **Naming Validation**
   - No reserved word conflicts
   - No naming collisions
   - Valid identifier names

4. **Structural Validation**
   - No circular inheritance
   - No circular relationships (unless explicit)
   - Valid view structure

---

## 5. Intermediate Representation (IR) (Phase 4)

**Note**: IR is designed after Phases 2-3 to generate code matching proven patterns.

### 5.1 Type System

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

### 5.2 IR Models

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

### 5.3 Parser (CDF → IR)

```python
class IRParser:
    """Parse validated CDF models to language-agnostic IR"""
    
    def parse_data_model(self, data_model: DataModel, validation_result: ValidationResult) -> IRModel:
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

### 5.4 Transformer (IR → Language-Specific IR)

```python
class IRTransformer:
    """Transform IR to language-specific IR (Python or TypeScript)"""
    
    def flatten_inheritance(self, model: IRModel) -> IRModel:
        """Flatten inheritance hierarchies"""
    
    def resolve_relationships(self, model: IRModel) -> IRModel:
        """Resolve all relationship references"""
    
    def apply_naming_conventions(self, model: IRModel, conventions: dict) -> IRModel:
        """Apply language-specific naming conventions"""
```

---

## 6. Code Generation (Phase 5)

**Purpose**: Generate code from IR that extends the generic InstanceAPI/InstanceClient classes.

### 6.1 Generator Interface

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

### 6.2 Python Generator (from IR)

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

### 6.3 Template Structure

**Note**: Templates generate code that extends generic InstanceAPI/InstanceClient classes.

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

## 7. API Service (Phase 8)

### 7.1 Service Architecture

**Purpose**: Provide HTTP API for generating SDKs on demand.

**Framework**: FastAPI (recommended)

**Endpoints**:

```python
from fastapi import FastAPI, File, UploadFile
from pydantic import BaseModel

app = FastAPI(title="Pygen Service", version="2.0.0")

class GenerateRequest(BaseModel):
    """Request to generate SDK"""
    space: str
    external_id: str
    version: str
    language: Literal["python", "_typescript", "csharp", "pyspark"] = "python"
    output_format: Literal["zip", "tarball", "code"] = "zip"
    cdf_url: str
    cdf_credentials: dict[str, str]  # Encrypted/secured

class GenerateResponse(BaseModel):
    """Response from generate endpoint"""
    job_id: str
    status: Literal["queued", "processing", "completed", "failed"]
    download_url: str | None = None
    warnings: list[str] = []
    errors: list[str] = []

@app.post("/generate", response_model=GenerateResponse)
async def generate_sdk(request: GenerateRequest):
    """Generate SDK from data model"""
    ...

@app.get("/generate/{job_id}", response_model=GenerateResponse)
async def get_generation_status(job_id: str):
    """Get status of generation job"""
    ...

@app.post("/validate")
async def validate_data_model(request: GenerateRequest):
    """Validate data model without generating"""
    ...

@app.get("/health")
async def health_check():
    """Service health check"""
    return {"status": "healthy"}

@app.get("/version")
async def version_info():
    """Pygen version information"""
    return {"version": "2.0.0", "supported_languages": [...]}
```

### 7.2 Job Queue

For long-running generation jobs:

```python
from celery import Celery

celery_app = Celery('pygen_service', broker='redis://localhost:6379/0')

@celery_app.task
def generate_sdk_task(request_data: dict) -> dict:
    """Async task for SDK generation"""
    # Run generation
    # Store result
    # Return download URL
    ...
```

---

## 8. Configuration

### 8.1 Configuration File

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

### 8.2 Configuration Loading

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

## 9. CLI Interface

**Note**: Using typer, not click.

```python
# pygen/cli.py

import typer
from typing import Optional
from pathlib import Path
from enum import Enum

app = typer.Typer(
    name="pygen",
    help="Generate SDKs from CDF Data Models",
    add_completion=False
)

class Language(str, Enum):
    python = "python"
    typescript = "_typescript"
    csharp = "csharp"
    pyspark = "pyspark"

@app.command()
def generate(
    space: str = typer.Option(..., "--space", "-s", help="Space name"),
    model: str = typer.Option(..., "--model", "-m", help="Data model external ID"),
    version: str = typer.Option("1", "--version", "-v", help="Data model version"),
    output: Path = typer.Option("./generated", "--output", "-o", help="Output directory"),
    language: Language = typer.Option(Language.python, "--language", "-l", help="Target language"),
    config: Optional[Path] = typer.Option(None, "--config", "-c", help="Configuration file"),
    strict: bool = typer.Option(False, "--strict", help="Treat warnings as errors"),
):
    """Generate SDK from data model"""
    typer.echo(f"Generating {language.value} SDK for {space}/{model}:{version}")
    
    # Validation
    # Generation
    # Output
    
    typer.secho("✓ Generation complete!", fg=typer.colors.GREEN)


@app.command()
def validate(
    space: str = typer.Argument(..., help="Space name"),
    model: str = typer.Argument(..., help="Data model external ID"),
    version: str = typer.Option("1", "--version", "-v", help="Data model version"),
    strict: bool = typer.Option(False, "--strict", help="Treat warnings as errors"),
):
    """Validate a data model"""
    typer.echo(f"Validating {space}/{model}:{version}")
    
    # Run validation
    # Print results
    
    typer.secho("✓ Validation complete!", fg=typer.colors.GREEN)


@app.command()
def inspect(
    space: str = typer.Argument(..., help="Space name"),
    model: str = typer.Argument(..., help="Data model external ID"),
    version: str = typer.Option("1", "--version", "-v", help="Data model version"),
):
    """Inspect a data model structure"""
    typer.echo(f"Inspecting {space}/{model}:{version}")
    
    # Fetch and display model structure
    ...


@app.command()
def serve(
    host: str = typer.Option("0.0.0.0", "--host", help="Host to bind to"),
    port: int = typer.Option(8000, "--port", help="Port to bind to"),
    reload: bool = typer.Option(False, "--reload", help="Enable auto-reload"),
):
    """Start Pygen API service (Goal 5)"""
    typer.echo(f"Starting Pygen service on {host}:{port}")
    
    import uvicorn
    uvicorn.run("pygen.service:app", host=host, port=port, reload=reload)


if __name__ == "__main__":
    app()
```

---

## 10. Error Handling

### 10.1 Exception Hierarchy

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

## 11. Performance Targets

### 11.1 Client Performance

- **Requests per second**: >100 (with connection pooling)
- **Latency**: <100ms (p95) for single item retrieval
- **Memory**: <50MB for client instance
- **Concurrent requests**: Support 10+ concurrent requests

### 11.2 Generation Performance

- **Small model** (<10 views): <5 seconds
- **Medium model** (10-50 views): <30 seconds
- **Large model** (50-200 views): <2 minutes
- **Memory**: <500MB during generation

### 11.3 Runtime Performance

- **Lazy iteration overhead**: <5% vs direct API calls
- **Query building**: <1ms
- **Serialization**: >10,000 objects/second
- **Memory**: O(chunk_size) not O(total_size)

---

## 12. Versioning & Compatibility

### 12.1 Semantic Versioning

Follow semantic versioning (MAJOR.MINOR.PATCH):
- **MAJOR**: Breaking changes
- **MINOR**: New features (backward compatible)
- **PATCH**: Bug fixes

### 12.2 API Compatibility

- Maintain backward compatibility within major version
- Deprecation warnings for 1 major version
- Clear migration guides for breaking changes

### 12.3 Generated Code Versioning

Include version metadata in generated code:

```python
# Generated by Pygen v2.0.0
__pygen_version__ = "2.0.0"
__model_version__ = "1"
__generated_at__ = "2025-01-15T10:30:00Z"
```

---

## 13. Security

### 13.1 Credential Handling

- Never log credentials
- Support environment variables
- Support credential files (with proper permissions)
- Support secret managers (Azure Key Vault, AWS Secrets Manager)

### 13.2 Input Validation

- Validate all user inputs
- Sanitize template inputs
- Check file paths for traversal attacks
- Validate API responses

### 13.3 Dependencies

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

