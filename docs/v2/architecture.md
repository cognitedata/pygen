# Pygen v2 Architecture

## Overview

Pygen v2 is a complete rewrite focused on:
1. **Modularity**: Clear separation of concerns
2. **Extensibility**: Easy to add new target languages
3. **Validation**: Upfront validation with helpful warnings
4. **Performance**: Client-based design with lazy evaluation
5. **Maintainability**: Type-safe, well-tested, documented

## High-Level Architecture

```
┌─────────────────────────────────────────────────────┐
│                 Pygen v2 Pipeline                   │
└─────────────────────────────────────────────────────┘
                          │
                          ▼
        ┌─────────────────────────────────┐
        │   1. Fetch from CDF API         │
        │      (Pygen Client)             │
        └─────────────────────────────────┘
                          │
                          ▼
        ┌─────────────────────────────────┐
        │   2. Validate Data Model        │
        │      (Validation Layer)         │
        └─────────────────────────────────┘
                          │
                          ▼
        ┌─────────────────────────────────┐
        │   3. Parse to IR                │
        │      (IR Parser)                │
        └─────────────────────────────────┘
                          │
                          ▼
        ┌─────────────────────────────────┐
        │   4. Transform IR               │
        │      (IR Transformer)           │
        └─────────────────────────────────┘
                          │
                          ▼
        ┌─────────────────────────────────┐
        │   5. Generate Code              │
        │      (Language Generators)      │
        └─────────────────────────────────┘
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
      Python        TypeScript          C#
```

## Module Descriptions

### 1. Pygen Client (`cognite.pygen.client`)
- Lightweight HTTP client wrapping httpx
- Handles authentication, rate limiting, retries
- Provides API for CDF Data Modeling resources:
  - Spaces, DataModels, Views, Containers, Instances
- Query builder for complex queries

### 2. Validation Layer (`cognite.pygen.validation`)
- Validates data models before generation
- Detects:
  - Missing reverse relationships
  - Incomplete models
  - Naming conflicts
  - Unsupported features
- Generates actionable warnings
- Allows graceful degradation

### 3. Intermediate Representation (`cognite.pygen.ir`)
- Language-agnostic model representation
- Core types:
  - `IRModel`: Top-level model
  - `IRClass`: Class/View representation
  - `IRProperty`: Property representation
  - `IRRelationship`: Relationship representation
  - `IRType`: Type system
- Parser: API models → IR
- Transformer: IR optimization and normalization

### 4. Generators (`cognite.pygen.generators`)
- Base generator interface
- Language-specific implementations:
  - `PythonGenerator`
  - `TypeScriptGenerator`
  - `CSharpGenerator` (future)
  - `PySparkGenerator` (future)
- Template-based code generation (Jinja2)
- Post-processing (formatting, validation)

### 5. Runtime Support (`cognite.pygen.runtime`)
- Base classes for generated SDKs
- Query helpers
- Lazy evaluation mechanisms
- Iterator protocols
- Language-specific runtimes

### 6. Utilities (`cognite.pygen.utils`)
- Common helper functions
- Text processing
- Naming conventions
- Reserved word handling

## Design Decisions

### Client-Based vs ORM
**Decision**: Use a client-based design, not an ORM.

**Rationale**:
- Clearer separation of concerns
- More explicit and predictable
- Easier to optimize queries
- Better for code generation
- Simpler to understand and debug

### Lazy Evaluation
**Decision**: Implement lazy evaluation for lists and queries.

**Rationale**:
- Memory efficient for large datasets
- Transparent pagination
- Better performance for filtered queries
- Natural Python/TypeScript idioms

### Validation First
**Decision**: Validate data models before generation.

**Rationale**:
- Catch issues early
- Provide actionable feedback
- Allow graceful degradation
- Improve user experience

### Language-Agnostic IR
**Decision**: Use an intermediate representation.

**Rationale**:
- Enables multi-language support
- Separates parsing from generation
- Allows IR-level optimizations
- Single source of truth

## Comparison with V1

| Aspect | V1 | V2 |
|--------|----|----|
| HTTP Client | cognite-sdk | httpx (lightweight) |
| Design | ORM-like | Client-based |
| Validation | Post-generation | Pre-generation |
| Multi-language | Python only | Python, TS, C#, PySpark |
| IR | No | Yes |
| Lazy Evaluation | Partial | Full |
| Type Safety | Good | Excellent |
| Test Coverage | ~70% | >90% target |

## Future Enhancements

- GraphQL-style query builder
- Caching layer
- Performance monitoring
- Code generation API service
- IDE plugins
- Custom template support

## References

- [Implementation Roadmap](../../plan/implementation-roadmap.md)
- [Development Workflow](development-workflow.md)

