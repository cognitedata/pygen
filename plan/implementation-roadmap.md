# Pygen Rewrite - Implementation Roadmap

## Overview

This document outlines the phased implementation approach for the Pygen rewrite. Each phase builds on the previous one and delivers incremental value while maintaining the ability to test and validate progress.

## Guiding Principles

1. **Iterative Development**: Build in phases, test continuously
2. **Minimum Viable Product First**: Get basic functionality working before adding complexity
3. **Test-Driven**: Write tests before or alongside implementation
4. **Documentation Alongside Code**: Document as we build
5. **Regular Integration**: Merge frequently to avoid large integration efforts
6. **Validation Gates**: Each phase must pass criteria before moving to next

## Phase Overview

```
Phase 0: Foundation & Setup (1 week)
    ↓
Phase 1: Pygen Client Core (3-4 weeks)
    ↓
Phase 2: Validation & IR (3-4 weeks)
    ↓
Phase 3: Python Generator MVP (3-4 weeks)
    ↓
Phase 4: Runtime & Lazy Evaluation (3-4 weeks)
    ↓
Phase 5: Feature Parity (4-6 weeks)
    ↓
Phase 6: Query Builder & Optimizer (2-3 weeks)
    ↓
Phase 7: Multi-Language Foundation (3-4 weeks)
    ↓
Phase 8: API Service (2-3 weeks)
    ↓
Phase 9: Production Hardening (2-3 weeks)
    ↓
Phase 10: Migration & Documentation (2-3 weeks)

Total Estimated Time: 26-39 weeks
```

## Phase 0: Foundation & Setup

**Goal**: Organize existing foundation and prepare for v2 development alongside v1.

**Duration**: 1 week

**Note**: Foundation already exists in the repository. This phase focuses on organization for parallel development.

### Tasks

1. **Project Reorganization**
   - Move existing Pygen v1 code to `cognite/pygen/legacy/` folder. However, keep the top-level `cognite/pygen/__init__.py` for v1 installation.
   - Move existing tests to `tests/legacy/` folder.
   - Keep v1 functional during v2 development
   - Set up new v2 structure alongside legacy

2. **Development Environment Validation**
   - Verify existing development tools (ruff, mypy, pytest)
   - Ensure CI/CD pipeline works for dual structure
   - Update pre-commit hooks for new structure

3. **Testing Infrastructure Enhancement**
   - Validate existing pytest configuration
   - Ensure coverage reporting works with new structure
   - Enhance test fixtures and utilities

4. **Documentation Structure**
   - Validate existing documentation setup
   - Plan migration guide structure

### Deliverables
- ✅ V1 code moved to cognite/pygen/legacy/
- ✅ CI/CD works
- ✅ Test infrastructure supports parallel development

### Success Criteria
- ✅ Can format code with `ruff format`
- ✅ Can check types with `mypy`
- ✅ Coverage reporting works
- ✅ V1 remains functional in legacy folder
- ✅ New v2 structure is ready for development

### Migration Plan
- ✅ Keep v1 in cognite/pygen/legacy/ until v2 is complete
- Delete cognite/pygen/legacy/  folder only after v2.0.0 release
- Maintain v1 bug fixes if critical during development

### Status
**✅ PHASE 0 COMPLETE** (December 20, 2025)

All tasks, deliverables, and success criteria have been met. The project is ready to proceed to Phase 1.

---

## Phase 1: Pygen Client Core

**Goal**: Build a lightweight, httpx-based client for CDF Data Modeling API.

**Duration**: 3-4 weeks

### Tasks

1. **HTTP Client Foundation** ✅
   - ✅ Implement internal HTTPClient wrapper around httpx
   - ✅ Implement rate limiting
   - ✅ Add retry logic with exponential backoff
   - ✅ Connection pooling configuration
   - ✅ Request/response logging

2. **Authentication Support** ✅
   - ✅ Token-based authentication
   - ✅ OAuth2 flow support
   - ✅ Token refresh logic
   - ✅ Support for different authentication providers
   - ✅ Integration with CDF authentication
   - ✅ The authentication code should be placed under `cognite/pygen/_client/auth/`

3. **Pydantic Models for API Objects** ✅
   - ✅ DataModel model
   - ✅ View model
   - ✅ Container model
   - ✅ Space model
   - ✅ Error response models

4. **Resource Clients** ✅
   - ✅ SpacesAPI (iterate, list, create, retrieve, delete)
   - ✅ DataModelsAPI (iterate, list, create, retrieve, delete)
   - ✅ ViewsAPI (iterate, list, create, retrieve, delete)
   - ✅ ContainersAPI (iterate, list, create, retrieve, delete)

5. **Error Handling**
   - [ ] Custom exception hierarchy
   - [ ] API error mapping
   - [ ] Detailed error messages
   - [ ] Retry logic for transient errors

6. **Testing**
   - [ ] Unit tests for each component
   - [ ] Integration tests with mock API
   - [ ] Test coverage >90%
   - [ ] Performance benchmarks

### Deliverables
- ✅ Working PygenClient class
- ✅ All CRUD operations implemented
- ✅ Comprehensive test suite
- ✅ API documentation

### Success Criteria
- ✅ Can authenticate to CDF
- [ ] Can perform CRUD on all resource types
- [ ] All tests pass with >90% coverage
- [ ] Performance benchmarks meet targets
- [ ] Type checking passes with mypy

### Status
**🔄 PHASE 1 IN PROGRESS** (Started December 21, 2025)

- ✅ Task 1: HTTP Client Foundation - Complete
- ✅ Task 2: Authentication Support - Complete  
- ✅ Task 3: Pydantic Models for API Objects - Complete
- ✅ Task 4: Resource Clients - Complete (December 22, 2025)
- ⏳ Task 5: Error Handling - Not Started
- ⏳ Task 6: Testing - Not Started

**Progress**: 4/6 tasks complete (67%)

### Dependencies
- Phase 0 complete

---

## Phase 2: Validation & Intermediate Representation

**Goal**: Validate data models upfront, then create language-agnostic IR from validated models.

**Duration**: 3-4 weeks

### Tasks

1. **Validation Layer (Goal 6 - Critical!)**
   - Implement validation rules for data models
   - Check data model for the following:
     - Existance of reverse direct relation target.
     - `source` is defined for direct relations.
     - No name conflicts with Python or Pydantic reserved words for
       properties, classes, methods, and method parameters.
   - Generate warnings for any issues found.
   - Graceful degradation decisions. 
     - For missing reverse direct relation target,exclude the reverse direct relation property.
     - For missing `source` in direct relations, only use a node reference. Note this requires
       no action in the validation layer, but must be handled in the parser/transformer.
     - For name conflicts, append a `_` suffix to the conflicting name. Note this requires
       no action in the validation layer, but must be handled in the parser/transformer.
   - Clear, user-friendly error messages

2. **Type System**
   - Define IRType hierarchy based on ViewResponse property types:
     - **Primitive types from DataType variants:**
       - TextProperty → IRTextType
         - Maps to Python `str`
         - Supports max_text_size and collation constraints
       - Float32Property, Float64Property → IRFloatType
         - Maps to Python `float`
         - Supports unit metadata (Unit with external_id and source_unit)
       - BooleanProperty → IRBoolType
         - Maps to Python `bool`
       - Int32Property, Int64Property → IRIntType
         - Maps to Python `int`
       - TimestampProperty → IRTimestampType
         - Maps to Python `datetime.datetime` (read/write)
         - Maps to `datetime` in typed hints
         - ISO format serialization with milliseconds
       - DateProperty → IRDateType
         - Maps to Python `datetime.date` (read/write)
         - Maps to `date` in typed hints
         - ISO format serialization
       - JSONProperty → IRJSONType
         - Maps to Python `dict`
     - **CDF Reference types (external IDs):**
       - TimeseriesCDFExternalIdReference → IRTimeseriesReferenceType
         - Read: `Union[TimeSeries, str, None]`
         - Write: `Union[TimeSeriesWrite, str, None]`
         - GraphQL: `Optional[TimeSeriesGraphQL]`
         - Always nullable
       - FileCDFExternalIdReference → IRFileReferenceType
         - Read: `Union[FileMetadata, str, None]`
         - Write: `Union[FileMetadataWrite, str, None]`
         - GraphQL: `Optional[FileMetadataGraphQL]`
         - Always nullable
       - SequenceCDFExternalIdReference → IRSequenceReferenceType
         - Read: `Union[SequenceRead, str, None]`
         - Write: `Union[SequenceWrite, str, None]`
         - GraphQL: `Optional[SequenceGraphQL]`
         - Always nullable
     - **Container types:**
       - Support for list types via `list` property on ListablePropertyTypeDefinition
       - Support for `max_list_size` constraint
       - Lists wrap the inner type: `list[T]` or `Optional[list[T]]`
     - **Connection types (derived from ViewResponseProperty variants):**
       - DirectNodeRelation → IRDirectRelationType
         - One-to-one: Write: `DirectRelationReference | tuple[str, str] | None`, Read: `DirectRelationReference | None`
         - One-to-many: Write: `list[DirectRelationReference | tuple[str, str]] | None`, Read: `list[DirectRelationReference] | None`
         - With source: includes target class in Union
         - Without source: only dm.NodeId or str (if has_default_instance_space)
       - SingleEdgeProperty/MultiEdgeProperty → IREdgeType
         - Single: `Union[TargetClass, str, dm.NodeId, None]`
         - Multi: `Optional[list[Union[TargetClass, str, dm.NodeId]]]`
         - With edge class: includes edge class in Union
       - SingleReverseDirectRelationPropertyResponse/MultiReverseDirectRelationPropertyResponse → IRReverseDirectRelationType
         - Single: `Union[TargetClass, str, dm.NodeId, None]`
         - Multi: `Optional[list[Union[TargetClass, str, dm.NodeId]]]`
         - Always read-only (not in write classes)
     - **Enum types:**
       - EnumProperty → IREnumType
         - Maps to `Literal[value1, value2, ...]` with all enum keys
         - Read type includes `| str` to handle unknown values
         - Stores unknown_value handling
     - **Type Modifiers:**
       - Optional/nullable types based on ViewCorePropertyResponse.nullable
       - Cardinality tracking (single vs multi) from connection_type
       - Each IRType has methods:
         - as_python_type() -> str (base Python type)
         - as_read_type_hint() -> str (for read data classes)
         - as_write_type_hint() -> str (for write data classes)
         - as_graphql_type_hint() -> str (for GraphQL data classes)
         - as_typed_hint(operation: Literal["read", "write"]) -> str (for TypedNode/TypedEdge)

3. **IR Models**
   - **IRProperty** - Represents a property in a class
     - Source: ViewCorePropertyResponse
     - Fields:
       - name: str (from ViewCoreProperty.name or property key, transformed per naming config)
       - prop_name: str (original property identifier in data model)
       - doc_name: str (human-readable name for documentation)
       - description: str | None (from ViewCoreProperty.description)
       - type: IRType (parsed from ViewCorePropertyResponse.type)
       - nullable: bool (from ViewCorePropertyResponse.nullable)
       - immutable: bool | None (from ViewCorePropertyResponse.immutable)
       - auto_increment: bool | None (from ViewCorePropertyResponse.auto_increment)
       - default_value: Any | None (from ViewCorePropertyResponse.default_value)
       - container_reference: ContainerReference (from ViewCoreProperty.container)
       - container_property_identifier: str (from ViewCoreProperty.container_property_identifier)
       - is_read_only: bool (computed from container/property in readonly list)
       - need_alias: bool (whether name != prop_name, requiring pydantic alias)
     - Methods (for code generation):
       - as_read_type_hint() -> str (type hint for read data class)
       - as_write_type_hint() -> str (type hint for write data class)
       - as_graphql_type_hint() -> str (type hint for GraphQL data class)
       - as_typed_hint(operation: Literal["read", "write"]) -> str (type hint for typed init)
       - support_filtering() -> bool (whether property can be filtered)
       - get_filtering_class() -> str (DMS filter class name like "IntFilter", "StringFilter")
   - **IRConnection** - Represents a connection/relationship in a class
     - Source: SingleEdgeProperty, MultiEdgeProperty, ReverseDirectRelationProperty, DirectNodeRelation
     - Fields:
       - name: str (from property key or ConnectionPropertyDefinition.name)
       - prop_name: str (original property identifier)
       - doc_name: str (human-readable name)
       - variable: str (variable name for iteration)
       - description: str | None (from ConnectionPropertyDefinition.description)
       - connection_type: IRConnectionType (parsed from connection_type discriminator)
       - target_class: IRClassReference | None (resolved from source, can be None for direct relation without source)
       - edge_class: IRClassReference | None (for edge connections with properties)
       - is_list: bool (from multi vs single connection type)
       - direction: Literal["outwards", "inwards"] (from EdgeProperty.direction)
       - edge_type: DirectRelationReference | None (from EdgeProperty.type)
       - edge_source: ViewReference | None (from EdgeProperty.edge_source)
       - through: PropertyId | None (for reverse direct relations)
       - container_reference: ContainerReference | None (for direct relations)
       - container_property_identifier: str | None
       - type_hint_node_reference: list[str] (like ["str", "dm.NodeId"] for direct relations)
       - need_alias: bool
     - Properties (computed):
       - is_direct_relation: bool (edge_type is None and through is None)
       - is_direct_relation_no_source: bool (is direct relation without target_class)
       - is_reverse_direct_relation: bool (through is not None)
       - is_edge: bool (edge_type is not None)
       - is_edge_without_properties: bool (is edge and edge_class is None)
       - is_edge_with_properties: bool (is edge and edge_class is not None)
       - is_write_field: bool (not reverse direct relation and not read-only)
     - Methods (for code generation):
       - as_read_type_hint() -> str
       - as_write_type_hint() -> str
       - as_graphql_type_hint() -> str
       - as_typed_hint(operation: Literal["read", "write"]) -> str
       - support_filtering() -> bool (only direct relations support filtering)
       - get_filtering_class() -> str (returns "DirectRelationFilter")
   - **IRClass** - Represents a generated class (from View)
     - Source: ViewResponse
     - Fields:
       - read_name: str (class name for read operations, PascalCase)
       - write_name: str (class name for write operations, e.g., "{read_name}Write")
       - graphql_name: str (class name for GraphQL operations, e.g., "{read_name}GraphQL")
       - read_list_name: str (list class name, e.g., "{read_name}List")
       - write_list_name: str (write list class name, e.g., "{read_name}WriteList")
       - doc_name: str (human-readable singular name)
       - doc_list_name: str (human-readable plural name)
       - variable: str (variable name for instances)
       - variable_list: str (variable name for lists)
       - file_name: str (file name for the class module)
       - space: str (from ViewResponse.space)
       - external_id: str (from ViewResponse.external_id)
       - version: str (from ViewResponse.version)
       - description: str | None (from ViewResponse.description)
       - properties: list[IRProperty] (parsed from ViewResponse.properties dict)
       - connections: list[IRConnection] (parsed from ViewResponse.properties dict)
       - implements: list[IRClassReference] (from ViewResponse.implements, parent classes)
       - direct_children: list[IRClassReference] (child classes that implement this class)
       - is_writable: bool (from ViewResponse.writable)
       - is_interface: bool (computed: true if has children and not writable)
       - used_for: Literal["node", "edge", "all"] (from ViewResponse.used_for)
       - node_type: DirectRelationReference | None (for node classes with filter)
       - has_edge_class: bool (if used_for == "all" and this is node class)
       - has_node_class: bool (if used_for == "all" and this is edge class)
     - Properties (computed):
       - pydantic_field: Literal["Field", "pydantic.Field"] (use "pydantic.Field" if class named "Field")
       - dependencies: list[IRClass] (all classes referenced by connections)
       - has_container_fields: bool (any field has container reference)
     - Methods (for iteration):
       - __iter__() -> Iterator[IRProperty | IRConnection] (iterate all fields)
       - get_field(name: str) -> IRProperty | IRConnection | None
   - **IRAPIClass** - Represents a generated API class
     - Fields:
       - parent_attribute: str (attribute name on parent client)
       - name: str (API class name, e.g., "{class_name}API")
       - file_name: str (file name for the API class)
       - data_class: IRClass (the data class this API serves)
       - filter_method: IRFilterMethod (main filter method)
       - is_edge_class: bool (whether this API serves an edge class)
   - **IREdgeAPIClass** - Represents a generated edge API class
     - Extends IRAPIClass
     - Fields:
       - start_class: IRClass (source node class)
       - end_class: IRClass (target node class)
       - edge_class: IRClass | None (edge properties class if exists)
       - field_name: str (field name on start class)
       - edge_type: DirectRelationReference (edge type identifier)
       - direction: Literal["outwards", "inwards"]
       - end_filter_method: IRFilterMethod (filter for end nodes)
       - has_default_instance_space: bool
   - **IRFilterMethod** - Represents a generated filter/list method
     - Fields:
       - name: str (method name, usually "list")
       - parameters: list[IRFilterParameter]
       - implementations: list[IRFilterImplementation]
       - return_type: str (return type hint)
   - **IRFilterParameter** - Represents a filter method parameter
     - Fields:
       - name: str (parameter name)
       - type_: str (type hint)
       - description: str (parameter description)
       - default: str | None (default value)
       - is_nullable: bool
       - is_time: bool (computed from type)
       - is_timestamp: bool (computed from type)
   - **IRFilterImplementation** - Represents a DMS filter implementation
     - Fields:
       - filter_class: str (DMS filter class like "dm.filters.Equals", "dm.filters.Range")
       - prop_name: str (property to filter on)
       - keyword_arguments: dict[str, IRFilterParameter]
       - is_edge_class: bool
     - Methods:
       - get_condition() -> str (condition to check if filter should be applied)
       - get_arguments() -> str (arguments to pass to filter constructor)
   - **IRMethod** - Represents a generated method on API classes
     - Fields:
       - name: str (method name like "retrieve", "apply", "delete", "search")
       - description: str | None (docstring content)
       - arguments: list[IRMethodArgument]
       - return_type: IRType
       - is_async: bool
   - **IRMethodArgument** - Represents a method parameter
     - Fields:
       - name: str (parameter name)
       - type: IRType (parameter type)
       - default: Any | None (default value)
       - is_required: bool
       - description: str | None
   - **IREnum** - Represents an enum type
     - Source: EnumProperty
     - Fields:
       - name: str (generated from context)
       - values: dict[str, IREnumValue] (from EnumProperty.values)
       - unknown_value: str | None (from EnumProperty.unknown_value)
   - **IREnumValue** - Represents an enum value
     - Source: EnumValue
     - Fields:
       - name: str (from EnumValue.name or key)
       - description: str | None (from EnumValue.description)
   - **IRModule** - Represents a Python module/file
     - Fields:
       - name: str (module name)
       - classes: list[IRClass]
       - api_classes: list[IRAPIClass]
       - enums: list[IREnum]
       - imports: list[IRImport]
   - **IRImport** - Represents an import statement
     - Fields:
       - module: str (module to import from)
       - names: list[str] (names to import)
       - alias: str | None (import alias)
   - **IRModel** - Top-level representation of entire data model
     - Source: DataModelResponse (containing multiple ViewResponse objects)
     - Fields:
       - name: str (data model name)
       - space: str (data model space)
       - external_id: str (data model external_id)
       - version: str (data model version)
       - description: str | None
       - modules: list[IRModule] (organized views into modules)
       - classes: list[IRClass] (all classes across modules)
       - api_classes: list[IRAPIClass] (all API classes)
       - edge_api_classes: list[IREdgeAPIClass] (edge-specific API classes)
       - view_mapping: dict[ViewReference, IRClass] (for resolving references)
       - has_default_instance_space: bool (whether default instance space is set)

4. **Parser**
   - **ViewResponse → IRClass Parser**
     - First pass: Create IRClass structure from ViewResponse
       - Extract view metadata (space, external_id, version, description)
       - Apply naming conventions based on config (PygenConfig)
       - Determine class variants (read_name, write_name, graphql_name, list names)
       - Generate variable names (variable, variable_list)
       - Determine file_name for module placement
       - Extract implements relationships (parent views)
       - Determine is_writable, used_for, node_type
       - Handle reserved word conflicts (append "_" suffix)
     - Second pass: Populate fields after all classes exist
       - Parse ViewCorePropertyResponse → IRProperty
         - Extract property metadata (name, description, nullable, etc.)
         - Parse DataType to IRType
         - Determine container references
         - Check if property is read-only
         - Handle enum types (extract EnumProperty → IREnum)
       - Parse connection properties → IRConnection
         - DirectNodeRelation → one-to-one or one-to-many direct relation
         - SingleEdgeProperty/MultiEdgeProperty → edge connections
         - ReverseDirectRelationProperty → reverse direct relations
         - Resolve target_class from view_mapping
         - Resolve edge_class from edge_class_mapping
         - Determine cardinality (single vs multi)
         - Handle missing sources (target_class = None)
       - Build EdgeClass instances (for edge type tracking)
       - Handle EndNodeField for edge classes (special multi-type connection)
       - Detect pydantic.Field vs Field naming conflicts
     - Third pass: Resolve inheritance and dependencies
       - Link parent classes via implements
       - Link child classes via direct_children
       - Determine is_interface (has children and not writable)
       - Build dependency graph
   - **DataModelResponse → IRModel Parser**
     - Extract data model metadata
     - Parse all views to IRClass instances
     - Build view_mapping: dict[ViewReference, IRClass]
     - Build edge_class_by_view_id mapping
     - Build node_class_by_view_id mapping
     - Determine has_default_instance_space
     - Organize classes into modules
   - **Helper Parsers**
     - DataType → IRType parser
       - Handle all primitive types with proper Python type mapping
       - Handle list types with cardinality
       - Handle enum types with literal generation
       - Handle CDF reference types
       - Handle DirectNodeRelation
     - Build direct_relations_by_view_id mapping (for validation)
     - Build view_property_by_container_direct_relation mapping (for reverse relations)

5. **Transformer**
   - **API Class Generation**
     - Generate IRAPIClass for each IRClass
       - Determine parent_attribute on client
       - Generate API class name and file name
       - Create IRFilterMethod for list/filter operations
         - Build IRFilterParameter for each filterable property
         - Build IRFilterImplementation for each filter type
         - Support Equals, In, Range, Prefix filters
         - Handle time/timestamp conversions
     - Generate IREdgeAPIClass for each edge connection
       - Link start_class and end_class
       - Include edge_class if edge has properties
       - Create filter methods for both endpoints
       - Support from/to node filtering with optional space parameters
   - **Filter Method Generation**
     - For each primitive field that supports filtering:
       - Generate appropriate filter parameters (min/max for ranges, equals, prefix for text)
       - Generate filter implementations with proper DMS filter mapping
     - For direct relation fields:
       - Generate DirectRelationFilter support
       - Handle space parameters if has_default_instance_space
   - **Method Generation**
     - Generate standard CRUD methods: retrieve, list, apply, delete, search
     - Generate aggregate methods if applicable
     - Generate query builder methods
   - **Naming Convention Application**
     - Apply field naming (name vs variable vs doc_name)
     - Apply class naming (PascalCase for classes, snake_case for variables)
     - Apply file naming conventions
     - Handle reserved word collisions
   - **Inheritance Flattening**
     - Collect all properties from parent classes
     - Handle property overrides
     - Maintain proper MRO (Method Resolution Order)
   - **Optimization**
     - Eliminate duplicate imports
     - Optimize class organization into modules
     - Determine minimal set of dependencies

6. **Testing**
   - Unit tests for validation rules
   - Unit tests for each IR component
   - Integration tests with real data models
   - Test with intentionally incomplete models
   - Edge case handling
   - Test coverage >90%

### Deliverables
- ✅ Complete validation layer with warning system
- ✅ Complete IR model definitions
- ✅ Parser from validated API models to IR
- ✅ Transformer utilities
- ✅ Comprehensive test suite

### Success Criteria
- Can validate complex data models
- Can detect and handle incomplete models gracefully
- Generates helpful warnings
- Can parse complex validated models
- All tests pass with >90% coverage
- IR is truly language-agnostic
- Well-documented validation and IR structure

### Dependencies
- Phase 1 complete (need API models and client)

---

## Phase 3: Python Generator MVP (Client-Based)

**Goal**: Generate basic Python SDK from IR using client-based design (not ORM).

**Duration**: 3-4 weeks

### Tasks

1. **Generator Infrastructure**
   - BaseGenerator abstract class
   - PythonGenerator implementation
   - Jinja2 template environment
   - File writing utilities
   - Formatter integration (ruff)

2. **Templates - Data Classes**
   - Simple Pydantic model template
   - Property generation with type hints
   - Docstrings
   - No ORM behavior (just data)

3. **Templates - API Classes (Client-Based)**
   - API class template that wraps PygenClient
   - Constructor takes PygenClient instance
   - CRUD methods delegate to client
   - List/filter methods with lazy iteration
   - Query building using client's query builder
   - Documentation

4. **Package Structure**
   - `__init__.py` generation
   - Module organization
   - Import management
   - Version information

5. **Post-Processing**
   - Run ruff format
   - Run ruff check
   - Validate generated code syntax
   - Run mypy on generated code

6. **Testing**
   - Test generation from IR
   - Test generated code quality
   - Test generated code functionality
   - Integration tests with real models
   - Test coverage >90%

### Deliverables
- ✅ Working Python generator
- ✅ Generated code passes linting
- ✅ Generated code is type-safe
- ✅ Basic CRUD functionality works
- ✅ Comprehensive test suite

### Success Criteria
- Can generate Python SDK from IR
- Generated code is PEP8 compliant
- Generated code passes mypy
- Can instantiate and use generated classes
- All tests pass with >90% coverage

### Dependencies
- Phase 2 complete (need IR)

---

## Phase 4: Runtime Support & Lazy Evaluation

**Goal**: Implement runtime support for generated SDKs with lazy evaluation (client-based).

**Duration**: 3-4 weeks

### Tasks

1. **Runtime Base Classes (Client-Based)**
   - Base API class for generated API classes
   - Base data class (simple Pydantic BaseModel extension)
   - Lazy iteration helpers
   - Query helper utilities

2. **Lazy Evaluation Mechanisms**
   - Iterator protocol implementation in API classes
   - Chunk-based fetching through PygenClient
   - Transparent pagination via client
   - Optional result caching

3. **Query Helpers**
   - Helper functions for filter composition
   - Helpers for working with query builder
   - Type-safe filter methods
   - Pagination helpers

4. **Integration with Generator**
   - Update templates to use runtime base classes
   - Generate API classes with client injection
   - Generate lazy list/filter methods
   - Generate relationship traversal methods

5. **Performance Optimization**
   - Efficient batching through client
   - Connection reuse (already in HTTPClient)
   - Memory management in iterators
   - Benchmark vs v1

6. **Testing**
   - Unit tests for runtime base classes
   - Integration tests with generated code
   - Performance tests vs v1
   - Memory usage tests
   - Test coverage >90%

### Deliverables
- ✅ Complete runtime support library (client-based)
- ✅ Lazy evaluation working through API classes
- ✅ Query helpers functional
- ✅ Generated code uses client-based lazy patterns
- ✅ Performance benchmarks met

### Success Criteria
- Can iterate over large datasets without loading all into memory
- Client-based design is clear and intuitive
- Performance is better than v1
- All tests pass with >90% coverage
- Memory usage is O(chunk_size)

### Dependencies
- Phase 3 complete (need generator)

---

## Phase 5: Feature Parity

**Goal**: Match all features of original Pygen.

**Duration**: 4-6 weeks

### Tasks

1. **Advanced Query Features**
   - Complex filters
   - Aggregations
   - Joins across views
   - Full-text search
   - Geospatial queries (if supported)

2. **Advanced Generation Features**
   - Custom naming conventions
   - Include/exclude specific views
   - Custom template support
   - Configuration file support
   - CLI improvements

3. **Edge Cases**
   - Self-referential relationships
   - Circular dependencies
   - Deep inheritance hierarchies
   - Large schemas
   - Reserved word handling

4. **API Coverage**
   - All View types supported
   - All property types supported
   - All relationship types supported
   - All query types supported

5. **Developer Experience**
   - Better error messages
   - Validation feedback
   - Progress indicators
   - Debug mode
   - Dry-run mode

6. **Testing**
   - Test all edge cases
   - Test with real-world schemas
   - Regression tests vs old Pygen
   - Performance comparison tests
   - Test coverage >90%

### Deliverables
- ✅ All original Pygen features implemented
- ✅ Edge cases handled
- ✅ Better UX than original
- ✅ Comprehensive test suite

### Success Criteria
- Can generate SDK for any data model that original Pygen supported
- Performance is equal or better
- Developer experience is improved
- All tests pass with >90% coverage
- No known bugs

### Dependencies
- Phase 4 complete

---

## Phase 6: Query Builder & Optimizer

**Goal**: Build a comprehensive query builder and optimizer for complex CDF queries.

**Duration**: 2-3 weeks

### Tasks

1. **Query Builder Foundation**
   - Implement fluent query builder API
   - Support for filter composition
   - Support for nested filters and logical operators
   - Type-safe filter building

2. **Query Optimization**
   - Query optimization logic for efficient CDF queries
   - Automatic query simplification
   - Cost-based optimization hints
   - Query plan analysis

3. **Common Query Patterns**
   - Support for common query patterns from v1
   - Relationship traversal queries
   - Aggregation query helpers
   - Pagination and sorting helpers

4. **Integration**
   - Integrate query builder with PygenClient
   - Update generated API classes to use query builder
   - Add query builder helpers to runtime base classes

5. **Testing**
   - Unit tests for query builder
   - Unit tests for query optimizer
   - Integration tests with PygenClient
   - Performance benchmarks vs manual queries
   - Test coverage >90%

### Deliverables
- ✅ Complete query builder implementation
- ✅ Query optimization working
- ✅ Integration with client and generated code
- ✅ Comprehensive test suite
- ✅ Documentation and examples

### Success Criteria
- Can build complex queries programmatically
- Query optimization improves performance
- Type-safe query building works
- All tests pass with >90% coverage
- Better DX than v1 query building

### Dependencies
- Phase 1 complete (need PygenClient)
- Phase 4 complete (need runtime base classes)

---

## Phase 7: Multi-Language Foundation

**Goal**: Enable generation of TypeScript SDKs (proof of concept).

**Duration**: 3-4 weeks

### Tasks

1. **Generator Abstraction**
   - Refactor generator interface
   - Extract common functionality
   - Language-specific configuration
   - Template organization

2. **TypeScript Generator**
   - TypeScriptGenerator class
   - TypeScript templates (basic)
   - Data class generation
   - API class generation
   - Type definitions

3. **TypeScript Runtime**
   - Base classes in TypeScript
   - HTTP client wrapper
   - Query builder
   - Lazy evaluation support

4. **Testing**
   - Test TypeScript generation
   - Test generated TypeScript code
   - Cross-language consistency tests
   - Test coverage >90%

5. **Documentation**
   - Multi-language architecture docs
   - TypeScript SDK guide
   - Language comparison guide

### Deliverables
- ✅ TypeScript generator working
- ✅ TypeScript SDK generation functional
- ✅ Framework for adding more languages
- ✅ Documentation updated

### Success Criteria
- Can generate TypeScript SDK from IR
- TypeScript SDK has basic functionality
- Architecture supports adding C# and PySpark
- All tests pass with >90% coverage

### Dependencies
- Phase 5 complete
- Phase 6 complete (query builder may be used in generated code)

---

## Phase 8: API Service (Goal 5)

**Goal**: Build Pygen backend service for generating SDKs on demand via API.

**Duration**: 2-3 weeks

### Tasks

1. **API Service Framework**
   - Choose framework (FastAPI recommended)
   - Set up service structure
   - API endpoint design
   - Request/response models

2. **Core Endpoints**
   - POST `/generate` - Generate SDK from specification
   - POST `/validate` - Validate data model
   - GET `/health` - Service health check
   - GET `/version` - Pygen version info

3. **Generation Service**
   - Async generation support
   - Job queue for long-running generations
   - Result caching (optional)
   - Rate limiting

4. **Output Formats**
   - ZIP file of source files
   - Tarball/package format
   - Direct code response (for small SDKs)
   - Support multiple languages

5. **Security & Auth**
   - API authentication
   - Input validation
   - Rate limiting
   - Resource limits

6. **Testing**
   - API endpoint tests
   - Integration tests
   - Load tests
   - Test coverage >90%

### Deliverables
- ✅ Working API service
- ✅ All endpoints functional
- ✅ Documentation (OpenAPI/Swagger)
- ✅ Deployment guide

### Success Criteria
- Can generate SDK via API
- Service is stable under load
- All tests pass with >90% coverage
- API documentation complete
- Deployment is straightforward

### Dependencies
- Phase 7 complete (multi-language support)

---

## Phase 9: Production Hardening

**Goal**: Prepare for production use.

**Duration**: 2-3 weeks

### Tasks

1. **Performance Optimization**
   - Profile and optimize hot paths
   - Reduce memory footprint
   - Improve generation speed
   - Optimize runtime queries

2. **Error Handling & Resilience**
   - Comprehensive error handling
   - Retry strategies
   - Timeout handling
   - Graceful degradation

3. **Logging & Monitoring**
   - Structured logging
   - Performance metrics
   - Error tracking
   - Usage analytics (opt-in)

4. **Security Review**
   - Credential handling review
   - Input validation review
   - Dependency audit
   - Security best practices

5. **Stability Testing**
   - Load testing
   - Stress testing
   - Long-running tests
   - Edge case fuzzing

6. **Production Readiness**
   - Deployment guide
   - Configuration guide
   - Troubleshooting guide
   - Runbook for common issues

### Deliverables
- ✅ Production-ready code
- ✅ Security audit complete
- ✅ Performance optimized
- ✅ Monitoring in place

### Success Criteria
- Passes all load tests
- No critical security issues
- Performance meets SLAs
- All documentation complete
- Ready for beta release

### Dependencies
- Phase 7 complete

---

## Phase 10: Migration & Documentation

**Goal**: Enable users to migrate from old to new Pygen.

**Duration**: 2-3 weeks

### Tasks

1. **Migration Guide**
   - Breaking changes document
   - Step-by-step migration guide
   - Code examples for common patterns
   - Troubleshooting section

2. **Migration Tools**
   - Automated migration script (if feasible)
   - Compatibility shims (if needed)
   - Validation tool for generated code

3. **Comprehensive Documentation**
   - User guide (quickstart to advanced)
   - API reference (complete)
   - Architecture documentation
   - Contributing guide
   - Examples and tutorials
   - FAQ

4. **Examples & Tutorials**
   - Update existing examples
   - Create new examples for new features
   - Video tutorials (optional)
   - Interactive notebooks

5. **Release Preparation**
   - Changelog
   - Release notes
   - Version strategy
   - Deprecation plan for old version

### Deliverables
- ✅ Complete migration guide
- ✅ Full documentation
- ✅ Examples updated
- ✅ Ready for release

### Success Criteria
- Users can migrate with clear guidance
- All features documented
- Examples cover common use cases
- Release artifacts ready

### Dependencies
- Phase 9 complete

### Post-Release
- Delete legacy/ folder after v2.0.0 is stable
- Archive v1 documentation
- Sunset v1 support per deprecation plan

---

## Risk Management

### High-Risk Areas

1. **Lazy Evaluation Complexity**
   - **Risk**: Complex to implement correctly
   - **Mitigation**: Start simple, iterate, extensive testing

2. **Performance Targets**
   - **Risk**: May not achieve desired performance
   - **Mitigation**: Early benchmarking, continuous profiling, set realistic targets

3. **Breaking Changes**
   - **Risk**: Users may struggle to migrate
   - **Mitigation**: Good documentation, migration tools, deprecation period

4. **Multi-Language Support**
   - **Risk**: May be more complex than anticipated
   - **Mitigation**: Start with one additional language, learn, iterate

5. **Timeline Overruns**
   - **Risk**: Project may take longer than estimated
   - **Mitigation**: Buffer time, prioritize ruthlessly, MVP approach

### Mitigation Strategies

- **Regular Reviews**: Weekly progress reviews
- **Adjust Scope**: Be willing to cut features if timeline at risk
- **Early Validation**: Test with real users early
- **Technical Spikes**: Investigate risky areas early
- **Parallel Work**: Multiple contributors on independent phases

---

## Success Metrics

### Technical Metrics
- Test coverage >90%
- Type checking passes (mypy strict mode)
- All linters pass (ruff)
- Performance benchmarks met
- Memory usage within bounds

### User Metrics
- Migration success rate
- API consistency score
- Documentation completeness
- Issue resolution time
- User satisfaction surveys

### Business Metrics
- Adoption rate
- Community contributions
- Support burden reduction
- Performance improvements vs old version

---

## Resource Requirements

### Team Composition (Ideal)
- 1-2 Senior Python developers
- 1 TypeScript developer (Phase 7+)
- 1 QA/Testing specialist
- 1 Technical writer (part-time)

### Infrastructure
- CI/CD pipeline
- Test CDF instance
- Documentation hosting
- Package repository (PyPI)

### Tools & Services
- GitHub/GitLab
- Coverage reporting service
- Performance monitoring
- Error tracking
- Documentation platform

---

## Go-Live Strategy

### Beta Release
- Limited audience
- Feature-complete but may have rough edges
- Gather feedback
- Fix critical issues

### Release Candidate
- Broader audience
- Production-ready features
- Final bug fixes
- Performance validation

### General Availability
- Full release
- Marketing/announcement
- Support plan in place
- Migration support available

### Post-Release
- Monitor usage and issues
- Regular patch releases
- Feature additions
- Community engagement

---

## Continuous Improvement

After initial release:
1. Gather user feedback continuously
2. Prioritize feature requests
3. Performance optimization ongoing
4. Add more languages (C#, PySpark)
5. Keep dependencies updated
6. Community building
7. Conference talks / blog posts
8. Integration with other tools

