# Pygen Rewrite - Project Overview

## Executive Summary

This document provides a high-level overview of the Pygen rewrite project, serving as an entry point for all stakeholders. Pygen is a Python package that generates Software Development Kits (SDKs) from Cognite Data Fusion (CDF) Data Models.

---

## What is Pygen?

**Pygen** (Python Generator) automatically generates type-safe, fully-featured SDKs from CDF Data Models. Instead of manually writing API client code, developers describe their data model in CDF, and Pygen generates:

- Data classes with validation
- API client methods (CRUD operations)
- Query builders
- Type hints for IDE support
- Documentation

### Example Workflow

```
1. Define data model in CDF
   (Views, Properties, Relationships)
          ↓
2. Run Pygen
   $ pygen generate --space my_space --model my_model
          ↓
3. Use generated SDK
   from generated_sdk import MyModel
   
   model = MyModel.get(id="123")
   model.property = "new value"
   model.save()
```

---

## Why Rewrite?

The original Pygen has been successful but faces several limitations:

### Current Problems

1. **Performance Bottlenecks**
   - Wraps `cognite-sdk` which limits optimization
   - Based on `requests` library (single-threaded, no HTTP/2)
   - Eager loading doesn't scale with large datasets

2. **Scalability Issues**
   - All data loaded into memory
   - Doesn't work well with millions of records
   - No streaming support

3. **Limited Language Support**
   - Only generates Python SDKs
   - Growing demand for TypeScript, C#, PySpark

4. **Maintainability Concerns**
   - ~70% test coverage
   - Organically grown architecture
   - Hard to add new features

### Project Goals

1. **Performance**: 5-10x faster through httpx, Pydantic v2, lazy loading
2. **Scalability**: Handle arbitrary dataset sizes through lazy evaluation
3. **Multi-Language**: Support Python, TypeScript, C#, PySpark
4. **Quality**: >90% test coverage, professional-grade code
5. **API Service**: Support Pygen backend service for on-demand SDK generation
6. **Validation**: Upfront validation with graceful degradation for incomplete models
7. **Maintainability**: Clean architecture, well-documented

---

## Project Scope

### In Scope

✅ Complete rewrite of Pygen from scratch
✅ Custom lightweight CDF client with HTTPClient wrapper (replace cognite-sdk)
✅ Query builder/optimizer for simplified API usage
✅ Upfront validation with graceful degradation
✅ Lazy evaluation for scalability (client-based design)
✅ Python SDK generation (maintain parity)
✅ TypeScript SDK generation (new)
✅ Intermediate Representation (IR) for multi-language support
✅ API service for on-demand SDK generation
✅ Comprehensive testing (>90% coverage)
✅ Documentation and migration guide
✅ CLI (typer-based) and programmatic API

### Out of Scope

❌ GUI for configuration
❌ Support for CDF APIs beyond Data Modeling
❌ Python 2 support
❌ Java/Go SDK generation (at least initially)
❌ Real-time code generation (file-based only)

---

## Project Structure

This rewrite is organized into 10 phases over approximately 7-10 months:

### Phase 0: Foundation (1 week)
Reorganize v1 to legacy/, set up v2 structure alongside

### Phase 1: Pygen Client Core (3-4 weeks)
Build httpx-based client with HTTPClient wrapper

### Phase 2: Generic Instance API & Example SDK - Python (3-4 weeks)
Build generic InstanceClient and InstanceAPI base classes, create hand-written example SDK

### Phase 3: Generic Instance API & Example SDK - TypeScript (3-4 weeks)
Build TypeScript equivalent of Python generic API with example SDK

### Phase 4: Intermediate Representation (IR) (3-4 weeks)
Create language-agnostic IR that can generate code for both Python and TypeScript

### Phase 5: Code Generation from IR (4-6 weeks)
Generate SDKs that match Phases 2-3 patterns from IR for both Python and TypeScript

### Phase 6: Feature Parity & Advanced Features (4-6 weeks)
Match all features of original Pygen, handle edge cases

### Phase 7: Query Builder & Optimizer (2-3 weeks)
Build comprehensive query builder for complex CDF queries

### Phase 8: API Service (2-3 weeks)
Build Pygen backend service for on-demand generation

### Phase 9: Production Hardening (2-3 weeks)
Performance optimization, security, stability

### Phase 10: Migration & Docs (2-3 weeks)
Migration guide, complete documentation, release, delete legacy/

---

## Key Architectural Decisions

### 1. Replace cognite-sdk with HTTPClient wrapper around httpx
**Why**: Better performance, HTTP/2, full control
**Impact**: Internal wrapper provides consistent interface

### 2. Build generic API before IR
**Why**: Establishes patterns with concrete examples first
**Impact**: IR design informed by real implementation needs

### 3. Generic base classes with extension pattern
**Why**: Reduce duplication, centralize CRUD logic
**Impact**: Easy to maintain and evolve generated SDKs

### 4. Use Pydantic v2 for all models
**Why**: 5-17x faster, excellent validation, type safety
**Impact**: Better performance and developer experience

### 5. Implement Intermediate Representation after patterns proven
**Why**: Enables multi-language support once patterns validated
**Impact**: More confident IR design, easier to implement

### 6. Validation before IR
**Why**: Catches issues early, enables graceful degradation
**Impact**: Better error messages, partial generation possible

### 7. Template-based generation
**Why**: Readable, maintainable, customizable
**Impact**: Easy to add new languages and modify output

### 8. API Service for on-demand generation
**Why**: Enables SaaS model, easier user adoption
**Impact**: Additional deployment complexity

### 9. Typer for CLI
**Why**: Modern, type-safe, better DX than click
**Impact**: Better CLI experience

### 10. Test-driven development
**Why**: Quality and confidence
**Impact**: >90% coverage from start

See [decisions-and-tradeoffs.md](./decisions-and-tradeoffs.md) for detailed rationale.

---

## Success Metrics

### Technical Metrics
- Test coverage >90%
- 5-10x performance improvement over original
- Memory usage O(chunk_size) not O(dataset_size)
- Can generate TypeScript SDKs
- Zero critical bugs in first quarter

### User Metrics
- >80% user migration rate
- Positive community feedback
- Reduced support burden
- Active community contributions

### Timeline Metrics
- Complete within 8 months
- Each phase meets quality gates
- Beta release after Phase 7

---

## Risk Management

### High Priority Risks

| Risk | Mitigation |
|------|------------|
| Timeline overrun | Phased approach, buffer time, ruthless prioritization |
| Performance targets not met | Early benchmarking, continuous profiling |
| Breaking changes upset users | Clear communication, migration tools, deprecation period |
| Lazy evaluation complexity | Start simple, iterate, extensive testing |
| Multi-language support harder than expected | Start with 2 languages, validate approach |

### Medium Priority Risks

| Risk | Mitigation |
|------|------------|
| User adoption issues | Excellent docs, migration guide, examples |
| Maintenance burden | Good architecture, comprehensive tests |
| Team capacity | Clear scope, avoid feature creep |

See [implementation-roadmap.md](./implementation-roadmap.md) for detailed risk management.

---

## Technology Stack

### Core Technologies
- **Python**: 3.10+ (modern type hints, pattern matching)
- **httpx**: HTTP client (async/sync, HTTP/2)
- **Pydantic v2**: Data validation and serialization
- **Jinja2**: Template engine for code generation
- **FastAPI**: API service framework (for Goal 5)
- **typer**: CLI framework

### Development Tools
- **uv**: Dependency management
- **pytest**: Testing framework
- **ruff**: Linting and formatting
- **mypy**: Type checking
- **coverage.py**: Coverage reporting

### CI/CD
- **GitHub Actions**: Continuous integration
- **codecov**: Coverage tracking
- **PyPI**: Package distribution

---

## Team & Resources

### Recommended Team
- 1-2 Senior Python developers
- 1 TypeScript developer (Phase 6+)
- 1 QA/Testing specialist
- 1 Technical writer (part-time)

### Time Commitment
- **Total**: 23-33 weeks (6-8 months)
- **Per developer**: Full-time recommended for core contributors
- **Part-time contributors**: Welcome for specific phases

### Infrastructure Needs
- CI/CD pipeline
- Test CDF instance
- Documentation hosting
- Package repository access

---

## Documentation

This project includes comprehensive planning documentation:

### Planning Documents (this folder)
1. **[readme.md](./readme.md)** - Problem statement and goals
2. **[project-overview.md](./project-overview.md)** - This document
3. **[architecture.md](./architecture.md)** - Detailed system architecture
4. **[implementation-roadmap.md](./implementation-roadmap.md)** - Phase-by-phase plan
5. **[testing-strategy.md](./testing-strategy.md)** - Comprehensive testing approach
6. **[technical-specifications.md](./technical-specifications.md)** - API specs and interfaces
7. **[decisions-and-tradeoffs.md](./decisions-and-tradeoffs.md)** - Architectural decisions log

### Future Documentation (to be created)
- User Guide (quickstart, tutorials, examples)
- API Reference (auto-generated)
- Developer Guide (contributing, architecture deep-dives)
- Migration Guide (from v1 to v2)
- FAQ and troubleshooting

---

## Project Timeline

```
Month 1-2: Foundation & Client
├─ Week 1:     Phase 0 (Reorganize to legacy/)
├─ Week 2-5:   Phase 1 (Pygen Client Core)
└─ Week 6-9:   Phase 2 (Generic Instance API - Python)

Month 3-4: TypeScript & IR
├─ Week 10-13: Phase 3 (Generic Instance API - TypeScript)
└─ Week 14-17: Phase 4 (Intermediate Representation)

Month 5-6: Generation & Feature Parity
├─ Week 18-23: Phase 5 (Code Generation from IR)
└─ Week 24-29: Phase 6 (Feature Parity & Advanced Features)

Month 7-10: Polish & Release
├─ Week 30-32: Phase 7 (Query Builder & Optimizer)
├─ Week 33-35: Phase 8 (API Service)
├─ Week 36-38: Phase 9 (Production Hardening)
├─ Week 39-41: Phase 10 (Migration & Docs)
└─ Week 42:    Release v2.0.0, delete legacy/
```

---

## Quality Gates

Each phase must meet quality criteria before proceeding:

### Phase Completion Criteria
- ✅ All tests passing
- ✅ Test coverage >90%
- ✅ Type checking passes (mypy)
- ✅ Linting passes (ruff)
- ✅ Documentation updated
- ✅ Code review complete
- ✅ Performance benchmarks met (if applicable)

### Release Criteria (v2.0.0)
- ✅ All phases complete
- ✅ E2E tests passing
- ✅ Security audit complete
- ✅ Performance targets met
- ✅ Migration guide complete
- ✅ Documentation complete
- ✅ Beta testing successful
- ✅ No critical bugs

---

## Communication Plan

### Regular Updates
- **Weekly**: Progress updates to stakeholders
- **Bi-weekly**: Team retrospectives
- **Monthly**: Roadmap review and adjustment
- **Per phase**: Phase completion report

### Channels
- **Planning docs**: Single source of truth
- **GitHub Issues**: Bug tracking, feature requests
- **Pull Requests**: Code review, technical discussion
- **Slack/Teams**: Daily communication
- **Email**: Formal updates to stakeholders

### Milestones
1. **M1**: Phase 1 complete (Working client with HTTPClient)
2. **M2**: Phase 2 complete (Generic Python API working)
3. **M3**: Phase 3 complete (Generic TypeScript API working)
4. **M4**: Phase 5 complete (Can generate SDKs from IR)
5. **M5**: Phase 6 complete (Feature parity achieved)
6. **M6**: Phase 9 complete (Production ready, beta release)
7. **M7**: Phase 10 complete (v2.0.0 release)

---

## Getting Started

### For Contributors

1. **Read Planning Docs**
   - Start with this overview
   - Review architecture.md for technical design
   - Check implementation-roadmap.md for current phase

2. **Set Up Environment**
   - Follow Phase 0 setup instructions
   - Install dependencies with `uv`
   - Run tests to verify setup

3. **Pick a Task**
   - Check current phase in roadmap
   - Find open issues on GitHub
   - Coordinate with team

### For Users (When Released)

1. **Read Migration Guide**
   - Understand breaking changes
   - Follow migration steps
   - Check examples

2. **Install New Version**
   ```bash
   pip install cognite-pygen>=2.0.0
   ```

3. **Generate SDK**
   ```bash
   pygen generate --space my_space --model my_model
   ```

4. **Report Issues**
   - Use GitHub issues
   - Provide minimal reproduction
   - Include version information

---

## Dependencies

### External Dependencies
- CDF Data Modeling API (required)
- CDF access credentials (required)
- Python 3.10+ (required)
- Network access to CDF (required)

### Internal Dependencies
```
Phase 0 → Phase 1 → Phase 2 (Python Generic API)
                          ↓
                    Phase 3 (TypeScript Generic API)
                          ↓
                    Phase 4 (IR based on Phases 2-3)
                          ↓
                    Phase 5 (Generation from IR)
                          ↓
                    Phase 6 (Feature Parity)
                          ↓
                    Phase 7 (Query Builder) → Phase 8 (API Service) → Phase 9 (Hardening) → Phase 10 (Docs)
```

---

## Budget & Resources

### Development Time
- **Core development**: 7-10 months (1-2 FTE)
- **Testing**: Included in each phase
- **Documentation**: 2-3 weeks dedicated + ongoing
- **Buffer**: 20% added to each phase estimate

### Infrastructure Costs
- CI/CD: Free tier likely sufficient
- Test CDF instance: Existing or new project
- Documentation hosting: Free (GitHub Pages)
- Package hosting: Free (PyPI)

### Opportunity Cost
- Could enhance existing Pygen instead
- But architectural issues require rewrite
- Long-term benefits justify investment

---

## Versioning & Releases

### Version Strategy
- **v2.0.0**: Initial rewrite release (breaking changes)
- **v2.x.0**: Feature releases (backward compatible)
- **v2.x.y**: Bug fixes and patches
- **v3.0.0**: Future major version (if needed)

### Release Schedule
- **Alpha**: Internal testing (after Phase 4)
- **Beta**: Limited user testing (after Phase 7)
- **RC**: Release candidate (after Phase 8, pre-release)
- **v2.0.0**: General availability (after validation)
- **v2.1.0+**: Regular feature releases (quarterly)

### Support Policy
- **v2.x**: Active development and support
- **v1.x**: Security fixes only for 6 months post-v2.0.0
- **v1.x**: End of life 12 months post-v2.0.0

---

## Conclusion

The Pygen rewrite is an ambitious but necessary project to address fundamental limitations of the original implementation. With careful planning, phased execution, and commitment to quality, we will deliver a professional-grade SDK generator that:

- Performs 5-10x better
- Scales to unlimited dataset sizes
- Supports multiple languages
- Maintains high code quality (>90% coverage)
- Provides excellent user experience

This project sets the foundation for Pygen's future and enables exciting possibilities for CDF developers.

---

## Next Steps

### Immediate (Week 1-2)
1. ✅ Review all planning documents
2. ✅ Get stakeholder approval
3. ✅ Complete Phase 0 (Foundation)
4. ✅ Set up project infrastructure
5. ⏳ Assemble team

### Short-term (Month 1-2)
1. Complete Phase 0 and Phase 1
2. Validate httpx client approach
3. Begin Phase 2 (IR)
4. Establish testing patterns

### Medium-term (Month 3-6)
1. Complete code generation
2. Achieve feature parity
3. Add TypeScript support
4. Beta testing

### Long-term (Month 7-8)
1. Production hardening
2. Complete documentation
3. Release v2.0.0
4. Plan v2.1+ features

---

## Questions & Contact

### Have Questions?
- Review detailed planning documents first
- Check FAQ (to be created)
- Ask in team Slack/Teams
- Create GitHub issue for specific technical questions

### Want to Contribute?
- Read this overview
- Review architecture.md
- Check implementation-roadmap.md for current phase
- Reach out to team lead

### Feedback on Plan?
- Plans are living documents
- Suggest improvements via PR
- Discuss in team meetings
- Document decisions in decisions-and-tradeoffs.md

---

**Document Version**: 1.4
**Last Updated**: December 29, 2025
**Status**: Phase 3 Complete ✅ - Ready for Phase 4

