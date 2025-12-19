# Plan Updates Summary

**Date**: December 19, 2025  
**Status**: Plan Updated Based on Feedback

---

## Changes Made

### 1. Goals Extended (readme.md)

**Added Goals 5 & 6**:
- **Goal 5**: API Service - Support Pygen backend service for generating SDKs on demand via API
- **Goal 6**: Upfront Validation - Validate data models upfront, generate warnings, gracefully handle incomplete models

### 2. Architecture Changes (architecture.md)

**Major Changes**:

#### a. Added Validation Layer (Before IR)
- Validation now happens **before** IR creation, not after
- Can filter out problematic elements before parsing
- Enables graceful degradation for incomplete models
- Generates actionable warnings

#### b. Changed Generated Runtime from ORM to Client-Based Design
- **OLD**: Database ORM-style with `PygenResource` base class, `save()`, `delete()` methods on data objects
- **NEW**: Client-based design following Pygen v1 patterns
  - API classes that wrap PygenClient
  - Simple data classes (just Pydantic models, no behavior)
  - Operations go through API classes, not data objects
  - Clear separation: data vs operations

#### c. Added Query Builder/Optimizer
- Internal query builder in PygenClient
- Simplifies complex query patterns
- Provides Pygen's value-add layer on top of raw CDF API

#### d. Added HTTPClient Wrapper
- Internal wrapper around httpx
- Provides consistent interface for all HTTP operations
- Handles retry, rate limiting, connection pooling

#### e. Reordered Architecture Layers
```
OLD:                          NEW:
1. Client                     1. Client (with HTTPClient + QueryBuilder)
2. IR                         2. Validation Layer ← NEW ORDER
3. Generation                 3. IR
4. Runtime (ORM-style)        4. Generation
                              5. Runtime (Client-based) ← REDESIGNED
```

### 3. Implementation Roadmap Changes (implementation-roadmap.md)

**Phase Changes**:

#### Phase 0: Foundation (Updated)
- **Changed**: Now acknowledges foundation already exists in repo
- **Plan**: Move v1 code to `legacy/` folder
- **Keep**: V1 functional alongside v2 development
- **Delete**: legacy/ only after v2.0.0 is stable
- **Duration**: Reduced from 1-2 weeks to 1 week

#### Phase 1: Pygen Client (Enhanced)
- **Added**: HTTPClient wrapper implementation
- **Added**: Query builder/optimizer implementation

#### Phase 2: Renamed and Expanded
- **OLD**: "Intermediate Representation"
- **NEW**: "Validation & Intermediate Representation"
- **Added**: Complete validation layer implementation
- **Changed**: Validation now first, then IR parsing
- **Duration**: Increased from 2-3 weeks to 3-4 weeks

#### Phase 3: Clarified Design
- **Title**: Now "Python Generator MVP (Client-Based)"
- **Emphasis**: Client-based design, not ORM

#### Phase 4: Renamed and Refocused
- **OLD**: "Lazy Evaluation & Runtime"
- **NEW**: "Runtime Support & Lazy Evaluation"
- **Changed**: Client-based patterns instead of ORM
- **Focus**: API classes with client injection

#### Phase 7: New Phase Added
- **NEW**: API Service implementation (Goal 5)
- FastAPI-based service
- Endpoints for SDK generation on demand
- Job queue for long-running generations
- **Duration**: 2-3 weeks

#### Phases 8-9: Renumbered
- OLD Phase 7 → NEW Phase 8 (Production Hardening)
- OLD Phase 8 → NEW Phase 9 (Migration & Docs)

**Total Duration**: 24-36 weeks (was 23-33 weeks)

### 4. Technical Specifications Changes (technical-specifications.md)

**Major Additions**:

#### a. HTTPClient Specification (Section 1.1 - NEW)
- Internal wrapper around httpx
- Consistent interface for GET, POST, PUT, DELETE
- Connection pooling, rate limiting, retry logic

#### b. QueryBuilder Specification (Section 1.3 - NEW)
- Query builder for simplified querying
- Filter composition
- Execute with lazy iteration

#### c. Validation Layer Specification (Section 2 - NEW)
- `DataModelValidator` class
- `ValidationIssue` and `ValidationResult` types
- Validation rules documentation
- Graceful degradation logic

#### d. IR Parser Change
- Now takes `ValidationResult` as input
- Works with validated/filtered models

#### e. Runtime Base Classes Redesigned
- **OLD**: `PygenResource` with ORM-like methods
- **NEW**: `BaseAPI` for generated API classes
- **NEW**: `BaseDataClass` for simple data classes (no ORM)
- Example generated code showing client-based pattern

#### f. API Service Specification (Section 6 - NEW)
- FastAPI-based service
- Endpoint specifications
- Job queue with Celery
- Request/response models

#### g. CLI Changed from Click to Typer
- Complete rewrite of CLI section
- Using typer instead of click
- Modern, type-safe CLI interface
- Added `serve` command for API service

### 5. Other Document Updates

#### project-overview.md
- Updated goals list
- Updated architecture decisions
- Updated technology stack (added FastAPI, typer)
- Updated timeline to 9 phases

#### decisions-and-tradeoffs.md
- Would need updates for new decisions (validation-first, client-based design)

#### quick-reference.md
- Would need updates to reflect new structure

---

## Key Architectural Principles (Updated)

### 1. Validation First
Data models are validated **before** IR creation, enabling:
- Early error detection
- Graceful degradation
- Better user feedback
- Filtering of incomplete elements

### 2. Client-Based Design (Not ORM)
Following Pygen v1 patterns:
- **Data classes**: Simple Pydantic models (just data)
- **API classes**: Wrap PygenClient, provide operations
- **Clear separation**: Data vs operations
- **No magic**: Explicit client usage

### 3. Query Builder Layer
Pygen provides value by simplifying CDF API:
- Query builder abstracts complex patterns
- Optimizes common operations
- Makes API more user-friendly

### 4. HTTPClient Wrapper
Internal consistency layer:
- Single point for all HTTP operations
- Consistent retry/rate-limiting
- Easy to test and mock

### 5. Coexistence Strategy
V1 and V2 development in parallel:
- V1 moves to `legacy/` folder
- Both functional during development
- Delete legacy/ only after v2.0.0 stable
- Maintains v1 bug fixes if critical

---

## What Stayed the Same

✅ Python 3.10+ minimum version  
✅ Pydantic v2 for data models  
✅ Jinja2 for templates  
✅ >90% test coverage goal  
✅ Multi-language support via IR  
✅ httpx as HTTP client (now wrapped)  
✅ Lazy evaluation by default  
✅ Template-based generation  

---

## Next Steps

1. ✅ Plan updated and approved
2. ⏳ Begin Phase 0: Move v1 to legacy/
3. ⏳ Set up v2 project structure
4. ⏳ Start Phase 1: HTTPClient + QueryBuilder implementation

---

## Questions Addressed

**Q: Why validation before IR?**  
A: Can filter out problematic elements before parsing, enabling partial generation of incomplete models.

**Q: Why client-based instead of ORM?**  
A: Maintains compatibility with Pygen v1 patterns, simpler mental model, clear separation of concerns.

**Q: Why query builder?**  
A: Pygen's value proposition includes simplifying the CDF API for common use cases.

**Q: Why HTTPClient wrapper?**  
A: Provides internal consistency, single point for HTTP configuration, easier testing.

**Q: Why keep v1 in legacy/?**  
A: Allows v1 bug fixes during v2 development, maintains functional v1 for users, clean separation.

---

**Plan Status**: ✅ Complete and Ready for Implementation

