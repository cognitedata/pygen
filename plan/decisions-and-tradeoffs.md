# Pygen Rewrite - Architectural Decisions & Tradeoffs

## Overview

This document records key architectural decisions made during the Pygen rewrite, including the rationale, alternatives considered, and tradeoffs involved. It serves as a reference for understanding "why" certain choices were made.

---

## Decision Log

### ADR-001: Use httpx instead of requests

**Status**: Accepted

**Context**:
The original Pygen uses `cognite-sdk` which builds on the `requests` library. We need to choose an HTTP client for the new implementation.

**Decision**:
Use `httpx` as the HTTP client library.

**Rationale**:
1. **Async/Sync Support**: httpx provides both synchronous and asynchronous APIs with the same interface
2. **HTTP/2**: Native HTTP/2 support for better performance
3. **Modern Design**: Built with modern Python practices (type hints, async/await)
4. **Connection Pooling**: Better connection pooling and resource management
5. **Active Development**: Actively maintained with regular updates
6. **Performance**: Generally faster than requests, especially with HTTP/2

**Alternatives Considered**:
- **requests**: Most popular, but lacks async support and HTTP/2
- **aiohttp**: Good async support, but only async (no sync API)
- **urllib3**: Too low-level, would require significant wrapper code

**Tradeoffs**:
- **Pro**: Better performance and modern features
- **Pro**: Future-proof with async support
- **Con**: Slightly less mature than requests
- **Con**: Additional dependency to learn

**Implementation Impact**:
- Medium complexity
- Affects: Client module
- Migration: Users won't notice difference

---

### ADR-002: Use Pydantic v2 for data models

**Status**: Accepted

**Context**:
We need a validation and serialization framework for API objects and generated models.

**Decision**:
Use Pydantic v2 for all data models.

**Rationale**:
1. **Performance**: Rust core provides 5-17x faster validation
2. **Type Safety**: Excellent type hint support and IDE integration
3. **Validation**: Rich validation capabilities out of the box
4. **Serialization**: Fast JSON serialization/deserialization
5. **JSON Schema**: Can generate JSON schemas automatically
6. **Documentation**: Auto-generate documentation from models
7. **Industry Standard**: Widely adopted in Python ecosystem

**Alternatives Considered**:
- **dataclasses + marshmallow**: More manual work, less performant
- **attrs + cattrs**: Good, but less ecosystem support
- **Plain dataclasses**: No validation or serialization
- **Custom solution**: Too much work, reinventing wheel

**Tradeoffs**:
- **Pro**: Excellent DX and performance
- **Pro**: Reduces boilerplate significantly
- **Con**: Dependency on external library
- **Con**: Breaking changes between Pydantic versions

**Implementation Impact**:
- Low complexity (well-documented library)
- Affects: All data models
- Migration: Natural fit for SDK generation

---

### ADR-003: Implement Intermediate Representation (IR)

**Status**: Accepted

**Context**:
We need to support multiple target languages (Python, TypeScript, C#, PySpark). We can either:
1. Parse CDF models directly to each language
2. Create an intermediate representation

**Decision**:
Implement a language-agnostic Intermediate Representation (IR) layer.

**Rationale**:
1. **Separation of Concerns**: Decouples parsing from generation
2. **Multi-Language**: Single parse, multiple generators
3. **Testability**: Can test parsing and generation independently
4. **Transformations**: Can apply optimizations at IR level
5. **Extensibility**: Easy to add new languages
6. **Versioning**: Can maintain compatibility across API versions

**Alternatives Considered**:
- **Direct generation**: Simpler but not scalable to multiple languages
- **AST-based**: Too complex, language-specific
- **GraphQL-style schema**: Considered but too domain-specific

**Tradeoffs**:
- **Pro**: Scalable to many languages
- **Pro**: Better maintainability
- **Con**: Additional abstraction layer
- **Con**: More initial development time

**Implementation Impact**:
- High complexity initially
- Affects: Core architecture
- Migration: Internal only, users unaffected

---

### ADR-004: Lazy evaluation by default

**Status**: Accepted

**Context**:
The original Pygen loads all data eagerly, causing scaling issues with large datasets. We need to decide on the default behavior.

**Decision**:
Implement lazy evaluation by default with explicit eager loading.

**Rationale**:
1. **Scalability**: Can handle arbitrarily large datasets
2. **Memory Efficiency**: Only load what's needed
3. **Performance**: Faster initial response
4. **Streaming**: Natural fit for streaming APIs
5. **User Control**: Users can opt into eager loading

**Alternatives Considered**:
- **Eager by default**: Simple but doesn't scale
- **Always eager**: Original problem persists
- **Optional lazy**: Inconsistent API, confusing

**Tradeoffs**:
- **Pro**: Solves scalability problem
- **Pro**: Better resource usage
- **Con**: More complex implementation
- **Con**: Potential confusion for users expecting lists
- **Con**: Debugging can be harder

**Implementation Impact**:
- High complexity
- Affects: Generated code and runtime
- Migration: Breaking change, requires user adaptation

---

### ADR-005: Template-based code generation

**Status**: Accepted

**Context**:
We need to generate code in multiple languages. Options include:
1. String concatenation
2. AST building
3. Template-based
4. DSL

**Decision**:
Use Jinja2 template-based code generation.

**Rationale**:
1. **Readability**: Templates look like target code
2. **Maintainability**: Easy to modify generated code structure
3. **Customization**: Users can provide custom templates
4. **Proven**: Industry-standard approach
5. **Flexibility**: Can generate any text format
6. **Community**: Many developers familiar with Jinja2

**Alternatives Considered**:
- **String concatenation**: Unmaintainable, error-prone
- **AST building**: Too complex, language-specific
- **Code generation frameworks**: Overkill, limited flexibility
- **DSL**: Too much upfront investment

**Tradeoffs**:
- **Pro**: Easy to understand and modify
- **Pro**: Good balance of power and simplicity
- **Con**: Templates can become complex
- **Con**: Limited static analysis

**Implementation Impact**:
- Medium complexity
- Affects: Generation engine
- Migration: Internal only

---

### ADR-006: Replace cognite-sdk entirely

**Status**: Accepted

**Context**:
We could either:
1. Build on top of cognite-sdk
2. Replace it entirely with custom client

**Decision**:
Replace cognite-sdk with a custom, lightweight client.

**Rationale**:
1. **Performance**: Direct control over HTTP layer
2. **Dependencies**: Smaller dependency footprint
3. **Pydantic Integration**: Native Pydantic models
4. **Flexibility**: Can optimize for our use case
5. **Simplicity**: Only implement what we need

**Alternatives Considered**:
- **Wrap cognite-sdk**: Maintains compatibility but doesn't solve performance issues
- **Contribute to cognite-sdk**: Too slow, different goals
- **Hybrid approach**: Complex, worst of both worlds

**Tradeoffs**:
- **Pro**: Full control and optimization potential
- **Pro**: Smaller, faster
- **Con**: More maintenance burden
- **Con**: Need to implement auth, retry, etc.
- **Con**: Potential compatibility issues

**Implementation Impact**:
- High complexity
- Affects: Core client
- Migration: Major breaking change

---

### ADR-007: Test-driven development with >90% coverage

**Status**: Accepted

**Context**:
Original Pygen has ~70% test coverage. We need to decide on testing approach.

**Decision**:
Mandate >90% test coverage with test-driven development.

**Rationale**:
1. **Quality**: Higher coverage = fewer bugs
2. **Confidence**: Can refactor with confidence
3. **Documentation**: Tests serve as examples
4. **Regression**: Catch regressions early
5. **Professional Grade**: Industry expectation

**Alternatives Considered**:
- **Lower threshold (80%)**: Not ambitious enough
- **100% coverage**: Diminishing returns, impractical
- **No mandate**: Would likely regress

**Tradeoffs**:
- **Pro**: High quality, maintainable code
- **Pro**: Catches bugs early
- **Con**: More development time
- **Con**: Can slow iteration initially

**Implementation Impact**:
- Medium complexity
- Affects: All development
- Migration: N/A (internal process)

---

### ADR-008: Use Python 3.10+ as minimum version

**Status**: Accepted

**Context**:
Need to choose minimum Python version for the rewrite.

**Decision**:
Support Python 3.10 and above.

**Rationale**:
1. **Pattern Matching**: Can use structural pattern matching
2. **Type Hints**: Better type hint support (union operator `|`)
3. **Performance**: Improved performance over 3.9
4. **Ecosystem**: Most libraries support 3.10+
5. **Reasonable Cutoff**: 3.10 released Oct 2021, mature enough

**Alternatives Considered**:
- **Python 3.8+**: Too old, missing useful features
- **Python 3.9+**: Close, but 3.10 has significant improvements
- **Python 3.11+**: Too new, limits user base
- **Python 3.12+**: Too cutting edge

**Tradeoffs**:
- **Pro**: Modern features and performance
- **Pro**: Better type system
- **Con**: Excludes some users on older versions
- **Con**: Can't use Python 3.9 features in development

**Implementation Impact**:
- Low complexity
- Affects: All code
- Migration: Users must upgrade Python

---

### ADR-009: Monorepo structure for multi-language support

**Status**: Accepted

**Context**:
How to organize code for multiple language generators?

**Decision**:
Keep all generators in a single repository (monorepo).

**Rationale**:
1. **Shared Code**: IR and core logic shared across languages
2. **Consistency**: Easier to keep consistent
3. **Atomic Changes**: Changes affect all generators atomically
4. **Versioning**: Single version number
5. **Testing**: Can test cross-language consistency

**Alternatives Considered**:
- **Separate repos**: More modular but harder to coordinate
- **Plugin architecture**: Complex, overkill for initial versions
- **Polyglot repo**: Considered but Python-focused for now

**Tradeoffs**:
- **Pro**: Easier to maintain consistency
- **Pro**: Shared infrastructure
- **Con**: Larger repository
- **Con**: Tighter coupling between generators

**Implementation Impact**:
- Low complexity
- Affects: Repository structure
- Migration: N/A (new project)

---

### ADR-010: CLI-first with programmatic API

**Status**: Accepted

**Context**:
How should users interact with Pygen?

**Decision**:
Primary interface is CLI, with full programmatic API available.

**Rationale**:
1. **User Preference**: Most users prefer CLI for generation
2. **CI/CD**: Easier to integrate in pipelines
3. **Scripts**: Still available for advanced users
4. **Documentation**: CLI is self-documenting
5. **Best of Both**: Don't need to choose one

**Alternatives Considered**:
- **API only**: Less accessible
- **CLI only**: Less flexible
- **GUI**: Overkill, hard to maintain

**Tradeoffs**:
- **Pro**: Covers most use cases
- **Pro**: Flexible for different workflows
- **Con**: Two interfaces to maintain
- **Con**: Need to keep in sync

**Implementation Impact**:
- Medium complexity
- Affects: User interface
- Migration: Similar to current

---

## Key Tradeoffs

### Tradeoff 1: Performance vs. Compatibility

**Choice**: Prioritize performance over backward compatibility

**Reasoning**:
- This is a major rewrite (v2.0)
- Users expect breaking changes
- Performance improvements justify migration effort
- Can provide migration tools

**Impact**:
- Faster, more efficient code
- Requires user migration
- Need excellent documentation

---

### Tradeoff 2: Simplicity vs. Flexibility

**Choice**: Balance with sensible defaults + customization

**Reasoning**:
- 80% of users should get good results with defaults
- 20% of users with special needs can customize
- Template system provides escape hatch

**Impact**:
- Better initial experience
- Still powerful for advanced users
- More complex implementation

---

### Tradeoff 3: Features vs. Timeline

**Choice**: MVP first, then iterate

**Reasoning**:
- Get basic functionality working first
- Validate approach with users
- Add features based on feedback
- Avoid over-engineering

**Impact**:
- Faster initial release
- May miss some features users want
- Can adjust based on feedback

---

### Tradeoff 4: Type Safety vs. Dynamic Flexibility

**Choice**: Prioritize type safety

**Reasoning**:
- Better IDE support
- Catch errors at development time
- Generated code should be strongly typed
- Users can opt out if needed

**Impact**:
- Better developer experience
- More rigid structure
- Requires more type annotations

---

### Tradeoff 5: Documentation vs. Development

**Choice**: Document alongside development

**Reasoning**:
- Documentation is part of quality
- Easier to document while fresh
- Users need docs for adoption
- Tests serve as documentation too

**Impact**:
- Better end product
- Slightly slower development
- Easier maintenance

---

## Future Decisions

Decisions that need to be made during implementation:

### FD-001: Async vs. Sync API
**Status**: Pending
**Question**: Should we provide async API in addition to sync?
**Considerations**:
- Would require async version of everything
- Significant complexity
- Most users probably don't need it
- Could add later if needed

**Recommendation**: Start with sync only, add async if demand exists

---

### FD-002: Caching Strategy
**Status**: Pending
**Question**: Should we implement client-side caching?
**Considerations**:
- Could improve performance
- Adds complexity
- Cache invalidation is hard
- May not be necessary with lazy loading

**Recommendation**: Measure performance first, add caching if needed

---

### FD-003: Plugin System
**Status**: Pending
**Question**: Should we support plugins for custom generators?
**Considerations**:
- Increases extensibility
- Adds complexity
- Not needed for initial release
- Templates provide some customization

**Recommendation**: Defer until v2.1+, templates sufficient for now

---

### FD-004: GraphQL Support
**Status**: Pending
**Question**: Should we support GraphQL in addition to REST?
**Considerations**:
- CDF may add GraphQL API
- Would be more efficient for complex queries
- Significant additional work
- Not currently supported by CDF

**Recommendation**: Wait for CDF GraphQL support

---

### FD-005: Web UI
**Status**: Pending
**Question**: Should we provide a web UI for configuration?
**Considerations**:
- Better for non-technical users
- Significant development effort
- Maintenance burden
- CLI + docs may be sufficient

**Recommendation**: Defer indefinitely, focus on CLI

---

## Anti-Decisions

Things we explicitly decided NOT to do:

### AD-001: Support Python 2
**Reason**: Python 2 is end-of-life, modern features required

### AD-002: Support synchronous-only patterns
**Reason**: Want to keep async door open for future

### AD-003: Create DSL for schema definition
**Reason**: CDF already has schema format, no need to invent new one

### AD-004: Support every CDF API
**Reason**: Focus on Data Modeling, other APIs out of scope

### AD-005: Build GUI
**Reason**: CLI is sufficient, GUI is expensive to maintain

### AD-006: Support code generation to Java
**Reason**: Low demand, significant effort, focus on requested languages

### AD-007: Build custom template engine
**Reason**: Jinja2 is excellent, no need to reinvent

### AD-008: Support inline code generation
**Reason**: File-based is cleaner, easier to version control

---

## Lessons from Original Pygen

What we learned from the original implementation:

### Lesson 1: Eager Loading Doesn't Scale
**Original Issue**: Loading all data into memory
**Solution**: Lazy evaluation by default

### Lesson 2: Over-Reliance on cognite-sdk
**Original Issue**: Coupled to cognite-sdk's abstractions
**Solution**: Custom lightweight client

### Lesson 3: Organic Growth Led to Complexity
**Original Issue**: Architecture evolved without clear plan
**Solution**: Upfront architectural design with IR layer

### Lesson 4: Test Coverage Matters
**Original Issue**: Hard to refactor with 70% coverage
**Solution**: Mandate >90% coverage from start

### Lesson 5: Single Language Was Limiting
**Original Issue**: Hard to add new languages
**Solution**: IR enables multi-language from start

---

## Risk Assessment

### Technical Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Lazy evaluation too complex | Medium | High | Start simple, iterate |
| Performance targets not met | Low | High | Early benchmarking |
| Multi-language support harder than expected | Medium | Medium | Start with 2 languages |
| IR abstraction leaks | Medium | Medium | Extensive testing |
| User adoption issues | Medium | High | Good docs, migration guide |

### Business Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Timeline overrun | Medium | Medium | Phased approach, buffer time |
| Breaking changes upset users | High | Medium | Clear communication, migration tools |
| Maintenance burden too high | Low | Medium | Good architecture, tests |
| Competing tools emerge | Low | Low | First-mover advantage, quality |

---

## Success Criteria

How we'll know if our decisions were correct:

### Technical Success
- [ ] >90% test coverage achieved
- [ ] Performance targets met or exceeded
- [ ] Can generate SDK for TypeScript
- [ ] Lazy loading works seamlessly
- [ ] Zero critical bugs in first 3 months

### User Success
- [ ] >80% of original Pygen users migrate
- [ ] Positive community feedback
- [ ] Active community contributions
- [ ] Documentation rated helpful
- [ ] Support ticket volume manageable

### Business Success
- [ ] Increased adoption vs. original
- [ ] Positive impact on product metrics
- [ ] Reduced maintenance burden
- [ ] Clear path for future enhancements

---

## Conclusion

These decisions form the foundation of the Pygen rewrite. They represent careful consideration of:
- Technical requirements
- User needs
- Maintainability
- Future extensibility
- Team capacity

As implementation progresses, we'll validate these decisions and adjust as needed. The key is to remain pragmatic while maintaining the vision of a high-quality, professional-grade SDK generator.

**Next Steps**:
1. Review this document with stakeholders
2. Begin Phase 0 (Foundation)
3. Validate decisions during implementation
4. Update this document as new decisions are made
5. Conduct retrospectives to learn from decisions

