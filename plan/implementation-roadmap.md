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
Phase 0: Foundation & Setup (1 week) ✅ COMPLETE
    ↓
Phase 1: Pygen Client Core (3-4 weeks) ✅ COMPLETE
    ↓
Phase 2: Generic Instance API & Example SDK - Python (3-4 weeks) ✅ COMPLETE
    ↓
Phase 3: Generic Instance API & Example SDK - TypeScript (4-5 weeks) ✅ COMPLETE
    ↓
Phase 4: PygenModel & Code Generation (4-6 weeks) ⏳ PENDING
    ↓
Phase 5: CLI, Feature Parity & Advanced Features (3-4 weeks) ⏳ PENDING
    ↓
Phase 6: Query Builder & Advanced Queries (2-3 weeks) ⏳ PENDING [Can parallel with Phase 5]
    ↓
Phase 7: API Service (2-3 weeks) ⏳ OPTIONAL
    ↓
Phase 8: Production Hardening (2-3 weeks) ⏳ PENDING
    ↓
Phase 9: Migration & Documentation (2-3 weeks) ⏳ PENDING

Completed: Phases 0-3 (~2 weeks actual)
Remaining: Phases 4-9 (~12-19 weeks estimated)
Total Estimated Time: ~14-21 weeks
```

## Current Module Structure

```
cognite/pygen/
├── _client/              # PygenClient (Phase 1) ✅
├── _example_datamodel/   # Example data model for SDK patterns
├── _generator/           # Code generation (Phase 4) ⏳
│   ├── config.py         # PygenSDKConfig
│   ├── gen_functions.py  # generate_sdk(), generate_sdk_notebook()
│   ├── generator.py      # Generator base class
│   ├── transformer.py    # CDF → PygenModel
│   ├── python.py         # PythonGenerator
│   └── typescript.py     # TypeScriptGenerator
├── _legacy/              # v1 code (delete after v2.0.0)
├── _pygen_model/         # Internal model for generation (Phase 4) ⏳
│   ├── _model.py         # CodeModel base
│   ├── _data_class.py    # DataClass, ReadDataClass
│   └── _field.py         # Field representation
├── _python/              # Python SDK (Phase 2) ✅
│   ├── instance_api/     # Generic InstanceAPI, InstanceClient
│   └── example/          # Example SDK extending generic classes
├── _typescript/          # TypeScript SDK (Phase 3) ✅
│   ├── instance_api/     # Generic InstanceAPI, InstanceClient
│   └── example/          # Example SDK extending generic classes
└── _utils/               # Utility functions
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

5. **Error Handling** ✅
   - ✅ Custom exception hierarchy (PygenAPIError, OAuth2Error)
   - ✅ API error mapping (FailedResponse with error parsing)
   - ✅ Detailed error messages
   - ✅ Retry logic for transient errors (part of HTTPClient)

6. **Testing** ✅
   - ✅ Unit tests for each component
   - ✅ Integration tests with mock API
   - ✅ Test coverage configured in pyproject.toml

### Deliverables
- ✅ Working PygenClient class
- ✅ All CRUD operations implemented
- ✅ Comprehensive test suite
- ✅ API documentation

### Success Criteria
- ✅ Can authenticate to CDF
- ✅ Can perform CRUD on all resource types
- ✅ All tests pass with coverage configured
- ✅ Type checking passes with mypy

### Status
**✅ PHASE 1 COMPLETE** (December 22, 2025)

- ✅ Task 1: HTTP Client Foundation - Complete
- ✅ Task 2: Authentication Support - Complete  
- ✅ Task 3: Pydantic Models for API Objects - Complete
- ✅ Task 4: Resource Clients - Complete
- ✅ Task 5: Error Handling - Complete
- ✅ Task 6: Testing - Complete

**Progress**: 6/6 tasks complete (100%)

### Dependencies
- Phase 0 complete

---

## Phase 2: Generic Instance API & Example SDK (Python)

**Goal**: Build a generic instance API with InstanceClient and InstanceAPI, then create an example SDK that extends these generic classes to demonstrate the pattern.

**Duration**: 3-4 weeks

### Tasks

1. **Generic Instance Models (Foundation)** ✅
   - Complete generic `InstanceModel`, `Instance`, `InstanceWrite` base classes
   - Implement `InstanceList` with pagination support
   - Implement `ViewRef` for view references
   - Implement `DataRecord` and `DataRecordWrite` for metadata
   - Generic serialization/deserialization (to/from CDF API format)
   - Support for both `node` and `edge` instance types
   - Pandas integration for data analysis

2. **Generic InstanceClient** ✅
   - ✅ Build `InstanceClient` class for instance CRUD operations
   - ✅ Implement `upsert()` method (create and update modes)
   - ✅ Implement `delete()` method
   - ✅ Use a three different thread pool executors, one for writing, one for deleting, and one for retrieving. They
     will have different concurrency limits.
   - ✅ Integration with HTTPClient from Phase 1
   - ✅ Proper error handling and validation
   - ✅ Return `InstanceResult` with created/updated/unchanged/deleted items


3.a **Generic InstanceAPI Part 1** ✅
   - ✅ Build `InstanceAPI` base class for view-specific operations
   - ✅ Implement `iterate()` for pagination built on https://api-docs.cognite.com/20230101/tag/Instances/operation/advancedListInstance
   - ✅ Implement `list()` a simple wrapper around `iterate()`
   - ✅ Implement `search()` for full-text search built on https://api-docs.cognite.com/20230101/tag/Instances/operation/searchInstances
   - ✅ The methods `.iterate()`, `.list()`, and `.search()` should match the API signature, except
     they should not include the `view`/`sources` parameters or the `instanceType` parameter,
     these are passed in via the `InstanceAPI` `__init__`.
   - ✅ Introduce Filtering data structures.
   - ✅ Introduce sort data structure (`PropertySort`).
   - ✅ Introduce unit data structure (`UnitConversion`, `PropertyWithUnits`).
   - ✅ Introduce debug data structure for the `.list()` and `.iterate()` methods (`DebugInfo`, `ListResponse`).

3.b  **Generic InstanceAPI Part 2** ✅
   - ✅ Implement `retrieve()` with single/batch, the thread pool executor built it on https://api-docs.cognite.com/20230101/tag/Instances/operation/byExternalIdsInstances
     should be passed into the `InstanceAPI` constructor.
   - ✅ Implement `aggregate()` for aggregations support built on https://api-docs.cognite.com/20230101/tag/Instances/operation/aggregateInstances
   - ✅ The `.aggregate` method should reuse the sort, filtering, unit data structures from part 1.

4. **Example API Classes** ✅
   - ✅ Build an example client and API classes based on the example data model
     located in `cognite/pygen/_generation/example_model/`
   - ✅ Remove the existing `PrimitiveNullable` example.
   - ✅ This should have a `ExampleClient` extending `InstanceClient` with three view-specific API classes
     extending `InstanceAPI`, one for each view in the example model. Note this subclassing
     should be trivial to implement, a simple subclassing of `InstanceClient` and add three
     attributes for the view-specific API classes.
   - ✅ Demonstrate how to initialize API classes with HTTPClient and ViewReference
   - ✅ Implement type-safe retrieve/list/iterate methods with unpacked parameters. The goal is to use
     as much primary Python typing features as possible, such as:
     int, float, str, bool, datetime, None or list version of these in the method signatures. If absolute necessary, use
     a more complex data structures that you create from pydantic or data classes. 

### Deliverables
- ✅ Complete generic InstanceModel/Instance/InstanceWrite base classes
- ✅ Generic InstanceClient with CRUD operations
- ✅ Generic InstanceAPI with retrieve/list/iterate/aggregate/search
- ✅ Example data classes demonstrating various property types
- ✅ Example API classes showing proper usage patterns
- ✅ Example client demonstrating how to compose API classes
- ✅ Comprehensive test suite with >90% coverage
- ✅ Documentation and usage examples

### Success Criteria
- ✅ Can perform CRUD operations on instances using generic InstanceClient
- ✅ Can retrieve, list, and filter instances using view-specific API classes
- ✅ Example SDK demonstrates clear patterns for extending generic classes
- ✅ Type safety maintained throughout with proper generics
- ✅ All tests pass with >90% coverage
- ✅ Generated code is clean and well-documented
- ✅ Pandas integration works for data analysis

### Status
**✅ PHASE 2 COMPLETE** (December 27, 2025)

- ✅ Task 1: Generic Instance Models - Complete
- ✅ Task 2: Generic InstanceClient - Complete
- ✅ Task 3.a: Generic InstanceAPI Part 1 - Complete
- ✅ Task 3.b: Generic InstanceAPI Part 2 - Complete
- ✅ Task 4: Example API Classes - Complete

**Progress**: 4/4 tasks complete (100%)

### Dependencies
- Phase 1 complete (need HTTPClient and PygenClient)


---

## Phase 3: Generic Instance API & Example SDK (TypeScript)

**Goal**: Build TypeScript equivalent of Phase 2 - generic instance API with InstanceClient and InstanceAPI, plus an example SDK.

**Duration**: 4-5 weeks

### Tasks

0. **Development Environment & Tooling Setup**
   - **Testing Framework Selection**: Choose between:
     - **Vitest** (recommended) - Fast, native ESM support, Jest-compatible API, excellent TypeScript support
     - Jest - Mature ecosystem, widely used, but slower with TypeScript
     - Mocha + Chai - Flexible but requires more setup
   - **Data Validation/Schema Library Selection**: Evaluate options:
     - **Plain TypeScript** (recommended for SDK) - Minimal dependencies, compile-time type safety
     - Zod - Runtime validation with TypeScript inference, useful for API responses
     - io-ts - Functional approach to validation, pairs well with fp-ts
     - Note: Unlike Python's Pydantic, TypeScript types are compile-time only. For runtime
       validation of API responses, consider Zod for critical paths.
   - **HTTP Client Selection**: Evaluate options:
     - **fetch** (recommended) - Native browser/Node.js API, no dependencies
     - axios - More features, interceptors, but adds dependency
   - **Project Structure Setup**:
     - Create `cognite/pygen/_generation/typescript/` directory structure for source code
     - Place all TypeScript configuration at the repository root level:
       - `package.json` - TypeScript dev dependencies (shared across project)
       - `tsconfig.json` - TypeScript configuration (strict mode, ESM output)
       - `vitest.config.ts` - Test runner configuration
     - `node_modules/` at root level (shared, gitignored)
     - TypeScript tests in `tests/tests_typescript/` directory
     - Set up ESLint and Prettier for code quality
     - Use the same `.gitignore` for both Python and TypeScript artifacts
   - **CI/CD Pipeline**:
     - Add TypeScript testing job to existing CI/CD workflow
     - Run `npm install` from repository root (uses root package.json)
     - Configure test coverage reporting (c8 or istanbul)
     - Add TypeScript compilation check
     - Add linting and formatting checks
     - Ensure CI runs on PRs affecting TypeScript files
   - **Documentation**:
     - Document chosen packages and rationale
     - Create CONTRIBUTING guide for TypeScript code

1. **HTTP Client Foundation (TypeScript)**
   - Implement `HTTPClient` class wrapping fetch API
   - Implement retry logic with exponential backoff
   - Add rate limiting support (handle 429 responses with Retry-After)
   - Connection timeout handling
   - Request/response logging capability
   - Create `RequestMessage`, `SuccessResponse`, `FailedResponse`, `FailedRequest` types
   - Error response parsing (match Python's ErrorDetails)

2. **Authentication Support (TypeScript)**
   - Create `Credentials` interface (abstract base)
   - Implement `TokenCredentials` for static token auth
   - Implement `OAuthCredentials` with token refresh logic
   - Support for different authentication providers
   - Token caching and refresh handling
   - Create `PygenClientConfig` interface for client configuration

3. **Generic Instance Models (TypeScript)**
   - Create `InstanceModel`, `Instance`, `InstanceWrite` base interfaces/classes
   - Implement `InstanceList<T>` with array-like behavior and utility methods
   - Implement reference types: `ViewReference`, `NodeReference`, `ContainerReference`
   - Implement `DataRecord` and `DataRecordWrite` interfaces
   - Implement `InstanceId` type for instance identification
   - Generic serialization/deserialization (to/from CDF API format with camelCase conversion)
   - Support for both `node` and `edge` instance types
   - Custom date/datetime handling (milliseconds since epoch like Python's `DateTimeMS`)

4. **Filter System (TypeScript)**
   - Implement filter data types matching Python's `filters.py`:
     - `EqualsFilter`, `InFilter`, `RangeFilter`, `PrefixFilter`, `ExistsFilter`
     - `ContainsAnyFilter`, `ContainsAllFilter`, `MatchAllFilter`, `NestedFilter`
     - `OverlapsFilter`, `HasDataFilter`, `InstanceReferencesFilter`
     - Logical filters: `AndFilter`, `OrFilter`, `NotFilter`
   - Implement data type filter builders matching Python's `dtype_filters.py`:
     - `TextFilter`, `FloatFilter`, `IntegerFilter`, `BooleanFilter`
     - `DateFilter`, `DateTimeFilter`, `DirectRelationFilter`
   - Create `FilterContainer` base class for view-specific filter containers
   - Ensure type-safe filter construction with method chaining

5. **Runtime Migration (Node to Deno)** ✅
   - **Rationale**: Migrate from Node.js to Deno for better TypeScript support, modern standards, and improved security
   - **Benefits**:
     - Native TypeScript support (no transpilation needed for development)
     - Built-in test runner and assertion library
     - Modern ES modules by default
     - Secure by default (explicit permissions)
     - Built-in tooling (formatter, linter, bundler)
     - Better standard library
     - NPM compatibility with `npm:` specifier
   - **Migration Tasks**:
     - ✅ Update source imports to use `.ts` file extensions
     - ✅ Create `deno.json` configuration file for project settings
     - ✅ Update CI/CD pipeline to use Deno instead of Node.js
     - ✅ Keep tests compatible via Vitest running through Deno
     - ✅ Update build scripts and tooling commands
     - ✅ Test all existing functionality works with Deno runtime
     - ✅ Create comprehensive Deno setup documentation
   - **Compatibility**:
     - ✅ Generated TypeScript code works with both Deno and Node.js/browser environments
     - ✅ Uses standard Web APIs where possible (fetch, etc.)
     - ✅ Avoids Deno-specific APIs in generated SDK code
   - **Documentation**:
     - ✅ Created development setup instructions for Deno
     - ✅ Documented how to run tests with Deno
     - ✅ Added Deno installation instructions

6. **Query & Response Models (TypeScript)** ✅
   - ✅ Implement query parameter types:
     - ✅ `PropertySort` for sorting configuration
     - ✅ `UnitConversion`, `UnitReference`, `UnitSystemReference` for unit handling
     - ✅ `DebugParameters` for debug mode
   - ✅ Implement aggregation types:
     - ✅ `Count`, `Sum`, `Avg`, `Min`, `Max`, `Histogram`
     - ✅ `Aggregation` union type with discriminator
     - ✅ `validateAggregationRequest` helper function
   - ✅ Implement response types:
     - ✅ `ListResponse<T>`, `Page<T>` for paginated responses
     - ✅ `UpsertResult` with created/updated/unchanged/deleted tracking
     - ✅ `DeleteResponse`, `AggregateResponse`, `AggregateItem`
     - ✅ Helper functions: `getCreated`, `getUpdated`, `getUnchanged`, `extendUpsertResult`

7. **Exception Hierarchy (TypeScript)**
   - Create `PygenAPIError` base error class
   - Implement `OAuth2Error` for authentication failures
   - Implement `MultiRequestError` for partial batch failures (with successful results)
   - Proper error message formatting and stack traces

8. **Generic InstanceClient (TypeScript)**
   - Build `InstanceClient` class for instance CRUD operations
   - Implement `upsert()` method with modes: "replace", "update", "apply"
   - Implement `delete()` method with flexible input types
   - Support batch operations with chunking (1000 items per request)
   - Use `Promise.all` or similar for parallel request execution
   - Implement `_collectResults()` for aggregating batch results
   - Context manager pattern equivalent (using `Disposable` or explicit close)
   - Configurable worker counts for write/delete/retrieve operations

9. **Generic InstanceAPI (TypeScript)**
   - Build `InstanceAPI<TInstance, TInstanceList>` base class
   - Implement `_iterate()` for single-page retrieval with cursor
   - Implement `_list()` with automatic pagination (collects all pages)
   - Implement `_search()` for full-text search
   - Implement `_retrieve()` with single/batch support and parallel execution
   - Implement `_aggregate()` for aggregation queries
   - Generic filter support via `Filter` type
   - All methods should use protected underscore prefix for subclass overriding

10. **Example Data Classes (TypeScript)**
   - Create `ProductNode`, `ProductNodeWrite`, `ProductNodeList` (node with various types)
   - Create `CategoryNode`, `CategoryNodeWrite`, `CategoryNodeList` (simple node)
   - Create `RelatesTo`, `RelatesToWrite`, `RelatesToList` (edge type)
   - Create view-specific filter containers:
     - `ProductNodeFilter` with type-safe property filters
     - `CategoryNodeFilter`
     - `RelatesToFilter`
   - Demonstrate property type mappings:
     - `string`, `number`, `boolean`, `Date` (for date/datetime)
     - Optional fields with `| undefined` or `?`
     - Arrays: `string[]`, `number[]`
     - Direct relations as `InstanceId | [string, string]`
   - Implement `asWrite()` conversion method on read classes
   - Include comprehensive JSDoc comments

11. **Example API Classes (TypeScript)**
    - Create `ProductNodeAPI` extending `InstanceAPI` with unpacked filter parameters
    - Create `CategoryNodeAPI` extending `InstanceAPI`
    - Create `RelatesToAPI` extending `InstanceAPI` for edge operations
    - Each API class should have:
      - `retrieve()` with overloads for single/batch
      - `iterate()` with unpacked filter parameters
      - `list()` with unpacked filter parameters and sorting
      - `search()` with query and filter parameters
      - `aggregate()` with aggregation and filter parameters
    - Create `ExampleClient` extending `InstanceClient` that composes all APIs

### Deliverables
- Complete development environment with chosen tooling
- CI/CD pipeline for TypeScript testing and linting
- HTTP client with retry and authentication support
- Complete generic TypeScript instance models
- Type-safe filter system with builder pattern
- Generic InstanceClient with CRUD operations
- Generic InstanceAPI with retrieve/list/iterate/aggregate/search
- Example TypeScript data classes, filters, APIs, and client
- Comprehensive test suite with >90% coverage
- Documentation and usage examples

### Success Criteria
- Development environment allows quick iteration with good DX
- CI/CD catches type errors and test failures before merge
- Can authenticate to CDF using various credential types
- Can perform CRUD operations using TypeScript InstanceClient
- Can retrieve, list, and filter instances using TypeScript API classes
- Filter builders provide type-safe, chainable filter construction
- Example SDK demonstrates clear patterns for extending generic classes
- Type safety maintained throughout with TypeScript generics
- All tests pass with >90% coverage
- Generated code is clean and well-documented
- Follows TypeScript best practices (strict mode, ESM, etc.)

### Dependencies
- Phase 1 complete (need to understand client patterns)
- Phase 2 complete (need Python patterns to mirror)

### Status
**✅ PHASE 3 COMPLETE** (December 29, 2025)

- ✅ Task 0: Development Environment & Tooling Setup - Complete (December 28, 2025)
- ✅ Task 1: HTTP Client Foundation - Complete (December 28, 2025)
- ✅ Task 2: Authentication Support - Complete (December 28, 2025)
- ✅ Task 3: Generic Instance Models - Complete (December 28, 2025)
- ✅ Task 4: Filter System - Complete (December 28, 2025)
- ✅ Task 5: Runtime Migration (Node to Deno) - Complete (December 28, 2025)
- ✅ Task 6: Query & Response Models - Complete (December 28, 2025)
- ✅ Task 7: Exception Hierarchy - Complete (December 28, 2025)
- ✅ Task 8: Generic InstanceClient - Complete (December 28, 2025)
- ✅ Task 9: Generic InstanceAPI - Complete (December 29, 2025)
- ✅ Task 10: Example Data Classes - Complete (December 29, 2025)
- ✅ Task 11: Example API Classes - Complete (December 29, 2025)

**Progress**: 12/12 tasks complete (100%)

---

## Phase 4: PygenModel & Code Generation

**Goal**: Build the PygenModel (internal representation of CDF data models) and code generators together, iterating between model design and generation output to ensure they work well together.

**Duration**: 4-6 weeks

**Approach**: This phase combines model definition with code generation in an iterative manner. The PygenModel and generators are tightly coupled - the model exists to serve generation, and generation requirements inform the model design. Building them together allows for:
- Faster feedback loops between model design and generation output
- Discovery of missing model attributes when implementing f-string templates
- Validation that the model structure actually works for code generation
- Reduced rework compared to finishing the model before starting generation

### Current Module Structure

```
cognite/pygen/
├── _pygen_model/          # Internal model for code generation ✅ SCAFFOLDED
│   ├── __init__.py        # Exports all model classes
│   ├── _model.py          # CodeModel base class (Pydantic)
│   ├── _data_class.py     # DataClass, ReadDataClass, ListDataClass, FilterClass
│   ├── _field.py          # Field representation
│   └── _sdk.py            # DataClassFile, APIClassFile, PygenSDKModel
├── _generator/            # Code generation logic ✅ SCAFFOLDED
│   ├── __init__.py
│   ├── _types.py          # OutputFormat, UserCasing, Casing type aliases
│   ├── config.py          # NamingConfig, PygenSDKConfig ✅ COMPLETE
│   ├── dtype_converter.py # DataTypeConverter, Python/TypeScript converters ✅ COMPLETE
│   ├── gen_functions.py   # generate_sdk() scaffolded, generate_sdk_notebook() TODO
│   ├── generator.py       # Generator base class ✅ SCAFFOLDED
│   ├── transformer.py     # to_pygen_model() ✅ SCAFFOLDED (core props only)
│   ├── python.py          # PythonGenerator ⏳ STUB
│   └── typescript.py      # TypeScriptGenerator ⏳ STUB
```

### Scaffolding Summary

The following has been implemented as scaffolding:

**PygenModel (`_pygen_model/`)**:
- ✅ `CodeModel` - Pydantic base class for all code models
- ✅ `Field` - Basic field with cdf_prop_id, name, type_hint, filter_name, description
- ✅ `DataClass` - View representation with view_id, name, fields, instance_type, display_name, description
- ✅ `ReadDataClass` - Extends DataClass with write_class_name
- ✅ `ListDataClass` - List class representation
- ✅ `FilterClass` - Filter container representation
- ✅ `DataClassFile` - Groups read, write, list, filter classes for a view
- ✅ `APIClassFile` - API class representation
- ✅ `PygenSDKModel` - Top-level model with data_classes and api_classes

**Generator (`_generator/`)**:
- ✅ `PygenSDKConfig` - Complete configuration class
- ✅ `DataTypeConverter` - Abstract base with Python/TypeScript implementations
- ✅ `to_pygen_model()` - Transforms DataModelResponse to PygenSDKModel (core properties only)
- ✅ `Generator` base class - Abstract with generate() method and abstract template methods
- ✅ `generate_sdk()` - Main function scaffolded (calls transformer and generator)
- ⏳ `PythonGenerator` - Stub with NotImplementedError on template methods
- ⏳ `TypeScriptGenerator` - Stub with NotImplementedError on all methods

**Tests (`tests/test_python/test_unit/test_generator/`)**:
- ✅ `test_dtype_converter.py` - Good coverage for both converters
- ✅ `test_transformer.py` - Basic test for to_pygen_model
- ✅ `test_gen_functions.py` - Tests with xfail for full generation
- ✅ `conftest.py` - Fixtures for example data model responses

### MVP Scope

The MVP focuses on generating SDKs for the **ExampleDataModel** located in `cognite/pygen/_example_datamodel/`. This includes 3 views:
- **ProductNode**: Node view with various property types (text, float, int, bool, date, datetime)
- **CategoryNode**: Node view with basic properties
- **RelatesTo**: Edge view with properties

**MVP Limitations** (to be addressed in future iterations):
- No connection/relationship support (direct relations, edges, reverse relations)
- No validation layer
- No code formatting integration
- Core properties only (text, int, float, bool, date, timestamp, json)

**File Structure**: Unlike the hand-written examples (all classes in one file), generated SDKs will have **separate files** for each data class and API class.

### Remaining Tasks

#### Task 4.1: Complete Python Data Class Templates ⏳

Implement in `python.py` `PythonDataClassGenerator`:

1. **`create_import_statements()`**:
   - Import from `cognite.pygen._python.instance_api`
   - Import required types (Date, InstanceId, JsonValue, etc.)

2. **`generate_read_class()`**:
   - Extend `Instance` base class
   - Generate all field definitions with type hints
   - Generate `_view_id` class attribute

3. **`generate_write_class()`**:
   - Extend `InstanceWrite` base class
   - Generate writable fields only
   - Handle optional vs required fields

4. **`generate_read_list_class()`**:
   - Extend `InstanceList` base class
   - Set generic type parameter

5. **`generate_filter_class()`**:
   - Extend `FilterContainer` base class
   - Generate filter fields with DataTypeFilter types

**Target Output**: One `.py` file per view containing read, write, list, and filter classes.

Reference: `cognite/pygen/_python/example/_data_class.py`

#### Task 4.2: Complete Python API Class Templates ⏳

Implement `create_api_class_code()` in `PythonGenerator`:

1. **API class template**:
   - Extend `InstanceAPI` base class
   - Generate `retrieve()`, `list()`, `iterate()`, `search()`, `aggregate()` methods
   - Unpack filter parameters for type-safe interface

2. **Type-safe method signatures**:
   - Generate filter parameters from FilterClass fields
   - Include sorting, pagination parameters

3. **Import statements**:
   - Import data classes and filter classes from data_classes module
   - Import base classes from instance_api

**Target Output**: One `_<view_name>_api.py` file per view.

Reference: `cognite/pygen/_python/example/_api.py`

#### Task 4.3: Complete Python Client & Package Templates ⏳

1. **Client class template**:
   - Extend `InstanceClient` base class
   - Compose API classes as attributes
   - Generate `__init__` with view registration

2. **Package structure**:
   - Generate `__init__.py` with all exports
   - Generate `data_classes/__init__.py`
   - Generate `_api/__init__.py`

3. **Implement `add_instance_api()`**:
   - Return empty dict when `pygen_as_dependency=True`
   - Copy instance_api module when `pygen_as_dependency=False`

**Target Output**:
```
<sdk_name>/
├── __init__.py
├── _client.py
├── data_classes/
│   ├── __init__.py
│   ├── product_node.py
│   ├── category_node.py
│   └── relates_to.py
└── _api/
    ├── __init__.py
    ├── _product_node_api.py
    ├── _category_node_api.py
    └── _relates_to_api.py
```

Reference: `cognite/pygen/_python/example/_client.py`

#### Task 4.4: Complete TypeScript Data Class Templates ⏳

Implement in `typescript.py`:

1. **Data class templates**:
   - `Instance` interface/class (read)
   - `InstanceWrite` interface
   - `InstanceList` class
   - Filter container class

2. **Handle TypeScript idioms**:
   - Use `readonly` for immutable arrays
   - Use `| undefined` for nullable types
   - Use interfaces where appropriate

**Target Output**: One `.ts` file per view containing read, write, list, and filter classes.

Reference: `cognite/pygen/_typescript/example/dataClasses.ts`

#### Task 4.5: Complete TypeScript API & Client Templates ⏳

1. **API class template**:
   - Extend `InstanceAPI` base class
   - Type-safe methods with unpacked parameters

2. **Client class template**:
   - Extend `InstanceClient` base class
   - Compose API classes

3. **Package structure**:
   - Generate `index.ts` with exports

4. **Implement `add_instance_api()`**:
   - Copy TypeScript instance_api module

**Target Output**:
```
<sdk_name>/
├── index.ts
├── client.ts
├── dataClasses/
│   ├── productNode.ts
│   ├── categoryNode.ts
│   └── relatesTo.ts
└── api/
    ├── productNodeApi.ts
    ├── categoryNodeApi.ts
    └── relatesToApi.ts
```

Reference: `cognite/pygen/_typescript/example/api.ts`, `cognite/pygen/_typescript/example/client.ts`

### Deliverables (MVP)
- ✅ Basic Field, DataClass models (PygenModel) - SCAFFOLDED
- ✅ PygenSDKModel top-level representation - SCAFFOLDED  
- ✅ Configuration system (PygenSDKConfig) - COMPLETE
- ✅ Data type converters (Python/TypeScript) - COMPLETE
- ✅ Basic transformer (core properties) - SCAFFOLDED
- ⏳ Working Python generator (data classes, API classes, client)
- ⏳ Working TypeScript generator (data classes, API classes, client)
- ⏳ Package structure generation (separate files per view)
- ⏳ Generated Python SDK for ExampleDataModel
- ⏳ Generated TypeScript SDK for ExampleDataModel

### MVP Success Criteria
- Can generate Python SDK from ExampleDataModel
- Can generate TypeScript SDK from ExampleDataModel  
- Naming conventions correctly applied for both languages
- Generated code correctly extends generic Instance API classes
- Generated code structure: separate files per data class and API class
- Generated code handles core property types (text, int, float, bool, date, timestamp, json)

### Deferred to Post-MVP
- Connection/relationship support (direct relations, edges, reverse relations)
- Validation layer with error messages
- Code formatting integration (ruff, deno fmt)
- Comprehensive test suite
- Complex data model support

### Status
**⏳ IN PROGRESS** - Scaffolding complete, MVP scope defined

- ✅ Task 4.0: Scaffolding - Complete
- ⏳ Task 4.1: Complete Python Data Class Templates - Not started
- ⏳ Task 4.2: Complete Python API Class Templates - Not started
- ⏳ Task 4.3: Complete Python Client & Package Templates - Not started
- ⏳ Task 4.4: Complete TypeScript Data Class Templates - Not started
- ⏳ Task 4.5: Complete TypeScript API & Client Templates - Not started

**Progress**: 1/6 tasks complete (~17%)

### Dependencies
- Phase 1 complete (need CDF API models: ViewResponse, DataModelResponse) ✅
- Phase 2 complete (understand Python patterns, example SDK to match) ✅
- Phase 3 complete (understand TypeScript patterns, example SDK to match) ✅

---

## Phase 5: CLI, Feature Parity & Advanced Features

**Goal**: Implement CLI, match all features of original Pygen, and add advanced capabilities.

**Duration**: 3-4 weeks

### Tasks

1. **CLI Implementation**
   - Implement typer-based CLI in `cognite/pygen/cli.py`
   - Commands:
     - `pygen generate` - Generate SDK from data model
     - `pygen validate` - Validate data model without generating
     - `pygen version` - Show version info
   - Options:
     - `--space`, `--external-id`, `--version` for data model reference
     - `--output-format` for Python/TypeScript
     - `--output-directory` for output location
     - `--config` for configuration file
   - Environment variable support for authentication
   - Progress indicators and clear output

2. **Configuration File Support**
   - Support `pygen.yaml` or `pygen.toml` configuration files
   - All `PygenSDKConfig` options available in config file
   - CLI options override config file
   - Document configuration options

3. **Complete `generate_sdk_notebook()` Function**
   - Implement in-memory SDK generation for Jupyter notebooks
   - Execute generated code to create client class
   - Return instantiated client ready for use
   - Support for interactive development workflows

4. **Edge Cases & Complex Models**
   - Self-referential relationships (view references itself)
   - Circular dependencies between views
   - Deep inheritance hierarchies (multiple `implements`)
   - Large schemas (100+ views) - performance optimization
   - Reserved word handling for both Python and TypeScript
   - Views without any properties

5. **Comprehensive Type Coverage**
   - All CDF property types: text, int32, int64, float32, float64, boolean, timestamp, date, json, object
   - All CDF reference types: timeseries, file, sequence
   - All connection types: direct relations, edges, reverse direct relations
   - List/array types with proper cardinality
   - Nullable vs required fields

6. **Developer Experience**
   - Clear error messages with actionable suggestions
   - Validation warnings for problematic but valid models
   - Progress indicators for long-running operations
   - `--verbose` flag for debug output
   - `--dry-run` flag to preview without writing files

### Deliverables
- ⏳ Complete CLI implementation
- ⏳ Configuration file support
- ⏳ `generate_sdk_notebook()` working
- ⏳ All edge cases handled
- ⏳ Full type coverage
- ⏳ Improved developer experience
- ⏳ Comprehensive test suite

### Success Criteria
- CLI works for common use cases
- Can generate SDK for any data model that original Pygen supported
- Edge cases handled gracefully with clear messages
- Performance is equal or better than v1
- All tests pass with >90% coverage

### Status
**⏳ NOT STARTED**

### Dependencies
- Phase 4 complete (need working generation pipeline)

---

## Phase 6: Query Builder & Advanced Queries

**Goal**: Build a comprehensive query builder for complex CDF queries and implement advanced query features.

**Duration**: 2-3 weeks

**Note**: This phase can be started in parallel with Phase 5 as it primarily extends the generic Instance API.

### Tasks

1. **Query Builder Foundation**
   - Implement fluent query builder API in `cognite/pygen/_utils/query_builder.py`
   - Support for filter composition with type hints
   - Support for nested filters and logical operators (and, or, not)
   - Type-safe filter building with proper IDE support
   - Integration with existing filter data structures from `_python/instance_api/filters.py`

2. **Advanced Query Features**
   - Complex filter combinations
   - Relationship traversal queries (following direct relations)
   - Aggregation query helpers (count, sum, avg, min, max by property)
   - Sorting with multiple fields
   - Pagination helpers for large result sets

3. **Query Optimization (Optional)**
   - Query simplification (combine redundant filters)
   - Ordering optimization for common patterns
   - Logging/debugging of generated queries
   - Note: Full optimization may be deferred to post-v2.0

4. **Integration with Generated SDKs**
   - Query builder available on generated API classes
   - Optional fluent interface: `client.my_view.query().filter(...).sort(...).list()`
   - Alternative to unpacked filter parameters for complex queries
   - Works with both Python and TypeScript SDKs

5. **Testing**
   - Unit tests for query builder
   - Integration tests with InstanceAPI
   - Test complex query scenarios
   - Performance benchmarks for large queries
   - Test coverage >90%

### Deliverables
- ⏳ Query builder implementation
- ⏳ Advanced query features working
- ⏳ Integration with generated SDKs
- ⏳ Comprehensive test suite
- ⏳ Documentation and examples

### Success Criteria
- Can build complex queries programmatically
- Type-safe query building works
- Query builder integrates seamlessly with generated SDKs
- All tests pass with >90% coverage
- Better DX than v1 query building

### Status
**⏳ NOT STARTED**

### Dependencies
- Phase 2 complete (need InstanceAPI patterns)
- Phase 4 complete (need generation working for integration)

---

## Phase 7: API Service (Optional)

**Goal**: Build Pygen backend service for generating SDKs on demand via HTTP API.

**Duration**: 2-3 weeks

**Note**: This phase is optional for v2.0 release. It can be implemented post-release if there's demand for a hosted generation service.

### Tasks

1. **API Service Framework**
   - Use FastAPI for the service
   - Service structure in `cognite/pygen/_api/` (not shipped with package)
   - Pydantic models for request/response
   - OpenAPI documentation auto-generated

2. **Core Endpoints**
   - `POST /generate` - Generate SDK from data model specification
     - Input: data model reference + configuration
     - Output: ZIP file or JSON with file contents
   - `POST /validate` - Validate data model without generating
     - Input: data model reference
     - Output: validation results and warnings
   - `GET /health` - Service health check
   - `GET /version` - Pygen version info

3. **Generation Service Logic**
   - Reuse `generate_sdk()` from `gen_functions.py`
   - Support for both Python and TypeScript output
   - Configurable options via request body
   - Streaming response for large SDKs

4. **Output Formats**
   - ZIP file of source files (default)
   - JSON with file contents (for programmatic use)
   - Both Python and TypeScript support

5. **Security & Operational Concerns**
   - API authentication (API keys or OAuth2)
   - Input validation and sanitization
   - Rate limiting per client
   - Resource limits (max views, timeout)
   - Request logging and monitoring

6. **Testing**
   - API endpoint tests with pytest + httpx
   - Integration tests with actual generation
   - Load tests for concurrent requests
   - Test coverage >90%

### Deliverables
- ⏳ Working API service
- ⏳ All endpoints functional
- ⏳ OpenAPI documentation
- ⏳ Docker deployment configuration
- ⏳ Deployment guide

### Success Criteria
- Can generate SDK via HTTP API
- Service handles concurrent requests
- All tests pass with >90% coverage
- API documentation complete
- Deployment is straightforward

### Status
**⏳ NOT STARTED** (Optional for v2.0)

### Dependencies
- Phase 4 complete (need working generators)

---

## Phase 8: Production Hardening

**Goal**: Prepare Pygen v2 for production use with optimizations and comprehensive testing.

**Duration**: 2-3 weeks

### Tasks

1. **Performance Optimization**
   - Profile code generation hot paths
   - Optimize large data model handling (100+ views)
   - Reduce memory footprint during generation
   - Benchmark against v1 for comparison
   - Ensure generated SDK runtime performance is optimal

2. **Error Handling & Resilience**
   - Review all error paths for clarity
   - Ensure all CDF API errors are properly mapped
   - Add retry logic for transient failures
   - Timeout handling for long operations
   - Graceful handling of partial failures

3. **Logging & Debugging**
   - Structured logging throughout
   - `--verbose` mode for debugging
   - Clear progress indicators for CLI
   - Error context in exception messages

4. **Security Review**
   - Credential handling review (no secrets in generated code)
   - Input validation for all user inputs
   - Dependency audit with safety/pip-audit
   - No arbitrary code execution from user input

5. **End-to-End Testing**
   - E2E tests with real CDF data models
   - Test generated SDKs actually work
   - Test with various data model configurations
   - Cross-language consistency tests (Python and TypeScript)

6. **Beta Testing**
   - Release beta version for early adopters
   - Collect feedback and issues
   - Fix critical bugs
   - Iterate on UX based on feedback

### Deliverables
- ⏳ Production-ready code
- ⏳ Performance benchmarks documented
- ⏳ Security audit complete
- ⏳ E2E test suite
- ⏳ Beta release

### Success Criteria
- Performance equal or better than v1
- No critical security issues
- E2E tests pass with real data models
- Beta users can successfully use v2
- All documentation complete

### Status
**⏳ NOT STARTED**

### Dependencies
- Phase 5 complete (need full feature set)

---

## Phase 9: Migration & Documentation

**Goal**: Enable users to migrate from v1 to v2 Pygen and complete all documentation.

**Duration**: 2-3 weeks

### Tasks

1. **Migration Guide**
   - Breaking changes document with clear explanations
   - Step-by-step migration guide from v1 to v2
   - Code examples for common patterns:
     - Basic SDK generation
     - Configuration changes
     - Runtime usage differences
   - Troubleshooting section for common issues
   - FAQ for migration questions

2. **Migration Tools (If Feasible)**
   - Script to update v1 configuration to v2 format
   - Comparison tool showing differences in generated code
   - Validation that v2 SDK covers same functionality as v1

3. **Comprehensive Documentation**
   - **User Guide**:
     - Quickstart (5 minutes to first SDK)
     - Configuration options
     - CLI usage
     - Programmatic usage
     - TypeScript SDK guide
   - **API Reference** (auto-generated from docstrings)
   - **Architecture documentation** (for contributors)
   - **Contributing guide**
   - **Examples and tutorials**

4. **Examples & Tutorials**
   - Basic SDK generation example
   - Python SDK usage example
   - TypeScript SDK usage example
   - Jupyter notebook tutorial
   - Complex data model example
   - Custom configuration example

5. **Release Preparation**
   - Complete CHANGELOG with all changes from v1
   - Release notes highlighting key improvements
   - PyPI package preparation
   - npm package preparation (for TypeScript runtime)
   - Deprecation plan for v1 (6 months security fixes, 12 months EOL)

### Deliverables
- ⏳ Complete migration guide
- ⏳ Full user documentation
- ⏳ API reference documentation
- ⏳ Examples and tutorials
- ⏳ Release artifacts ready

### Success Criteria
- Users can migrate from v1 with clear guidance
- All features documented with examples
- Documentation is clear and complete
- Release artifacts ready for PyPI/npm
- v2.0.0 released

### Status
**⏳ NOT STARTED**

### Dependencies
- Phase 8 complete

### Post-Release
- Delete `cognite/pygen/_legacy/` folder after v2.0.0 is stable
- Archive v1 documentation
- Sunset v1 support per deprecation plan
- Monitor issues and release patch versions as needed

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
- 1 TypeScript developer (Phase 4+)
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

