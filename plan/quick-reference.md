# Pygen Rewrite - Quick Reference

**Quick lookup for key information. For details, see the full planning documents.**

---

## 📋 Project Summary

**Goal**: Rewrite Pygen from scratch for better performance, scalability, and multi-language support

**Timeline**: 28-42 weeks (7-10 months)

**Status**: Phase 3 In Progress ⏳ - Tasks 0-3 Complete

---

## 🎯 Key Objectives

1. **Performance**: 5-10x faster via httpx + Pydantic v2
2. **Scalability**: Lazy evaluation for unlimited dataset sizes
3. **Multi-Language**: Python, TypeScript, C#, PySpark
4. **Quality**: >90% test coverage
5. **Maintainability**: Clean architecture, well-documented

---

## 📅 10-Phase Roadmap

| Phase | Name | Duration | Status | Key Deliverable |
|-------|------|----------|--------|-----------------|
| 0 | Foundation | 1 week | ✅ Complete | Project reorganized, v1 in legacy/ |
| 1 | Pygen Client | 3-4 weeks | ✅ Complete | httpx-based CDF client with HTTPClient |
| 2 | Generic Instance API (Python) | 3-4 weeks | ✅ Complete | InstanceClient, InstanceAPI, Example SDK |
| 3 | Generic Instance API (TypeScript) | 4-5 weeks | ⏳ In Progress | TypeScript equivalent of Phase 2 |
| 4 | Intermediate Representation (IR) | 3-4 weeks | ⏳ Pending | Validation + Language-agnostic IR |
| 5 | Code Generation from IR | 4-6 weeks | ⏳ Pending | Python & TypeScript SDK generation |
| 6 | Feature Parity & Advanced | 4-6 weeks | ⏳ Pending | Match original Pygen |
| 7 | Query Builder & Optimizer | 2-3 weeks | ⏳ Pending | Query builder/optimizer |
| 8 | API Service | 2-3 weeks | ⏳ Pending | On-demand SDK generation |
| 9 | Production | 2-3 weeks | ⏳ Pending | Hardening, optimization |
| 10 | Release | 2-3 weeks | ⏳ Pending | Migration guide, docs |

---

## 🏗️ Architecture Layers

```
┌─────────────────────────────────────┐
│  1. Pygen Client                    │  ← HTTPClient wrapper + QueryBuilder
│     (httpx + Pydantic)              │     Replace cognite-sdk
├─────────────────────────────────────┤
│  2. Validation Layer                │  ← Validate before IR creation
├─────────────────────────────────────┤
│  3. Intermediate Representation     │  ← Language-agnostic
├─────────────────────────────────────┤
│  4. Code Generation (Jinja2)        │  ← Multi-language
├─────────────────────────────────────┤
│  5. Generated Runtime Support       │  ← Client-based lazy evaluation
└─────────────────────────────────────┘
```

---

## 🔑 Key Technical Decisions

| Decision | Rationale |
|----------|-----------|
| HTTPClient wrapper | Internal consistency, easy mocking/testing |
| Validation first | Catch issues early, enable graceful degradation |
| Client-based design | Follows v1 patterns, clear separation |
| httpx over requests | Async/sync, HTTP/2, better performance |
| Pydantic v2 | 5-17x faster, excellent validation |
| IR layer | Enables multi-language support |
| Lazy by default | Solves scalability issues |
| Template-based | Readable, maintainable, customizable |
| Python 3.10+ | Modern features, type hints |
| Typer for CLI | Modern, type-safe CLI |
| >90% coverage | Professional-grade quality |

---

## 📦 Technology Stack

**Core**:
- Python 3.10+
- httpx (HTTP client)
- Pydantic v2 (data models)
- Jinja2 (templates)
- FastAPI (API service)
- typer (CLI)

**Development**:
- uv (dependencies)
- pytest (testing)
- ruff (linting/formatting)
- mypy (type checking)
- coverage.py (coverage)

**CI/CD**:
- GitHub Actions
- codecov
- PyPI

---

## 🧪 Testing Strategy

### Test Pyramid
- **70%** Unit tests (fast, isolated)
- **25%** Integration tests (component interaction)
- **5%** E2E tests (full workflows)

### Coverage Target
- Overall: >90%
- Critical paths: 100%
- New code: 100%

### Test Types
- Unit tests
- Integration tests
- E2E tests
- Property-based tests
- Performance tests
- Security tests
- Regression tests

---

## 📁 Project Structure

```
cognite/pygen/
├── legacy/             # V1 code (delete after v2.0.0)
│   └── ...
├── client/             # Pygen Client (Phase 1)
│   ├── http.py         # HTTPClient wrapper
│   ├── query.py        # Query builder
│   ├── core.py
│   ├── auth.py
│   ├── models/
│   └── resources/
├── validation/         # Validation Layer (Phase 2)
│   ├── validator.py
│   ├── rules.py
│   └── issues.py
├── ir/                 # Intermediate Representation (Phase 2)
│   ├── models.py
│   ├── types.py
│   ├── parser.py
│   └── transformer.py
├── generation/         # Code Generation (Phase 3+)
│   ├── base.py
│   ├── python/
│   ├── typescript/
│   └── ...
├── runtime/            # Generated Runtime (Phase 4)
│   ├── base.py
│   ├── lazy.py
│   └── query.py
├── api/                # API Service (Phase 7)
│   ├── app.py
│   ├── endpoints.py
│   └── models.py
└── cli.py              # CLI Interface (typer)
```

---

## 🚀 Quick Commands

### Development
```bash
# Setup
uv venv
source .venv/bin/activate  # or `.venv/Scripts/activate` on Windows
uv pip install -e ".[dev]"

# Run tests
pytest

# Check coverage
pytest --cov --cov-report=html

# Format code
ruff format .

# Lint code
ruff check --fix .

# Type check
mypy .
```

### Usage (after release)
```bash
# Install
pip install cognite-pygen>=2.0.0

# Generate SDK
pygen generate --space my_space --model my_model

# With config file
pygen generate --config pygen.yaml
```

---

## 📊 Success Metrics

### Technical
- [x] Phase 0 complete (Foundation)
- [x] Phase 1 complete (Full client with HTTPClient, Auth, Resource Clients)
- [x] Phase 2 complete (InstanceClient, InstanceAPI, Example SDK)
- [ ] Test coverage >90%
- [ ] 5-10x performance improvement
- [ ] Memory usage O(chunk_size)
- [ ] TypeScript generation works
- [ ] Zero critical bugs (first 3 months)

### User
- [ ] >80% migration rate
- [ ] Positive feedback
- [ ] Active contributions
- [ ] Good docs ratings

### Timeline
- [ ] Complete within 8 months
- [ ] All quality gates met
- [ ] Beta release after Phase 7

---

## ⚠️ Top Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Timeline overrun | Medium | Medium | Phased approach, buffer |
| Lazy evaluation complexity | Medium | High | Start simple, iterate |
| Performance targets | Low | High | Early benchmarking |
| Breaking changes | High | Medium | Migration tools, docs |
| Multi-language harder | Medium | Medium | Start with 2 languages |

---

## 🎯 Quality Gates

**Every Phase**:
- ✅ All tests passing
- ✅ Coverage >90%
- ✅ Mypy passes
- ✅ Ruff passes
- ✅ Docs updated
- ✅ Code reviewed

**Release (v2.0.0)**:
- ✅ All phases complete
- ✅ E2E tests passing
- ✅ Security audit done
- ✅ Performance targets met
- ✅ Migration guide done
- ✅ Beta testing successful

---

## 📚 Documentation Map

| Document | Purpose |
|----------|---------|
| **readme.md** | Problems and goals |
| **project-overview.md** | High-level summary (start here!) |
| **architecture.md** | System design and components |
| **implementation-roadmap.md** | Phase-by-phase plan |
| **testing-strategy.md** | Testing approach |
| **technical-specifications.md** | API specs and interfaces |
| **decisions-and-tradeoffs.md** | Architectural decisions log |
| **UPDATES.md** | Change history and updates |
| **PROGRESS.md** | Implementation progress tracking |
| **quick-reference.md** | This document |

---

## 👥 Team Recommendations

**Core Team**:
- 1-2 Senior Python developers
- 1 TypeScript developer (Phase 6+)
- 1 QA specialist
- 1 Technical writer (part-time)

**Time Commitment**:
- Full-time recommended
- Part-time for specific phases OK

---

## 📅 Key Milestones

| Milestone | Description | Target | Status |
|-----------|-------------|--------|--------|
| **M0** | Phase 0 complete (Foundation) | Week 1 | ✅ Complete |
| **M1** | Phase 1 complete (Full client) | Week 2 | ✅ Complete |
| **M1.5** | Phase 2 Tasks 1-3.b complete (InstanceClient, InstanceAPI) | Week 3 | ✅ Complete |
| **M2** | Phase 2 complete (Example SDK) | Month 1 | ✅ Complete |
| **M2.1** | Phase 3 Tasks 0-3 complete (TypeScript dev environment, HTTPClient, Auth, Instance Models) | Month 1 | ✅ Complete |
| **M3** | Phase 5 complete (Can generate Python & TypeScript) | Month 4 | ⏳ Pending |
| **M4** | Phase 6 complete (Feature parity) | Month 6 | ⏳ Pending |
| **M5** | Phase 10 complete (v2.0.0) | Month 9 | ⏳ Pending |

---

## 🔄 Version Strategy

- **v2.0.0**: Initial rewrite (breaking changes)
- **v2.x.0**: Feature releases (compatible)
- **v2.x.y**: Bug fixes
- **v3.0.0**: Future major version

**Support**:
- v2.x: Active
- v1.x: Security fixes (6 months)
- v1.x: EOL (12 months)

---

## 🎓 Key Concepts

### Lazy Evaluation
```python
# Old (eager) - loads everything into memory
items = api.list_all()  # ⚠️ Could be millions
for item in items:
    process(item)

# New (lazy) - loads in chunks
for item in api.list():  # ✅ Yields one at a time
    process(item)
```

### Intermediate Representation
```
CDF Data Model → IR → Python SDK
                 ↓
                 └─→ TypeScript SDK
                 └─→ C# SDK
                 └─→ PySpark SDK
```

### Template-Based Generation
```python
# template.py.jinja
class {{ class_name }}:
    def __init__(self, {% for prop in properties %}{{ prop.name }}: {{ prop.type }}{% endfor %}):
        ...

# Generated code
class MyModel:
    def __init__(self, id: str, name: str):
        ...
```

---

## 💡 Pro Tips

### For Contributors
1. **Read planning docs first** - especially architecture.md
2. **Start with tests** - TDD from the beginning
3. **Follow style guide** - ruff + mypy must pass
4. **Document as you go** - future you will thank you
5. **Ask questions** - better to clarify than assume

### For Users (Future)
1. **Read migration guide** - breaking changes explained
2. **Test in dev first** - don't migrate prod immediately
3. **Report issues early** - help us fix bugs quickly
4. **Provide feedback** - tell us what works/doesn't
5. **Check examples** - common patterns documented

---

## 🔗 Important Links

**Planning**:
- All docs in `plan/` folder
- Start with `project-overview.md`

**Code** (after Phase 0):
- Repository: TBD
- Issues: GitHub Issues
- PRs: GitHub Pull Requests

**Documentation** (after release):
- User Guide: TBD
- API Reference: TBD
- Migration Guide: TBD

---

## ❓ FAQ

**Q: Why rewrite instead of enhance?**
A: Architectural limitations can't be fixed incrementally.

**Q: Will it be backward compatible?**
A: No, v2.0 has breaking changes, but migration guide provided.

**Q: How long until release?**
A: 6-8 months for v2.0.0, beta after ~6 months.

**Q: Can I use it now?**
A: Not yet, in planning phase. Alpha in ~4 months.

**Q: Will v1 still be supported?**
A: Yes, security fixes for 6 months, EOL at 12 months.

**Q: Can I contribute?**
A: Yes! See implementation-roadmap.md for current phase.

**Q: What about my existing code?**
A: Migration guide will help transition. Some changes required.

**Q: Why Python 3.10+?**
A: Modern type hints, pattern matching, performance.

---

## 📞 Getting Help

**Have questions?**
1. Check this quick reference
2. Review full planning docs
3. Check FAQ sections
4. Ask in team channels
5. Create GitHub issue

**Want to contribute?**
1. Read project-overview.md
2. Review architecture.md
3. Check current phase in roadmap
4. Pick a task
5. Submit PR

---

## ✅ Current Status

**Phase 0**: ✅ Complete (December 20, 2025)

**Phase 1**: ✅ Complete (December 22, 2025)

**Phase 2**: ✅ Complete (December 27, 2025)

**Phase 3**: ⏳ In Progress (Started December 28, 2025)

**Current Progress**:
- ✅ Phase 0 complete - Project reorganized
- ✅ V1 code moved to `cognite/pygen/legacy/`
- ✅ Phase 1 complete - Pygen Client Core
  - HTTPClient wrapper with rate limiting and retry logic
  - Authentication with OAuth2 support
  - Pydantic models for all API objects
  - Resource clients (Spaces, DataModels, Views, Containers)
  - Error handling with custom exception hierarchy
  - Comprehensive test suite
- ✅ Phase 2 complete - Generic Instance API & Example SDK (Python)
  - InstanceModel, Instance, InstanceWrite base classes
  - InstanceList with pagination and pandas integration
  - ViewRef, DataRecord, DataRecordWrite
  - InstanceClient with CRUD operations (upsert, delete)
  - Thread pool executors for concurrency
  - InstanceResult tracking
  - InstanceAPI with iterate(), list(), search(), retrieve(), aggregate()
  - Filtering, sorting, unit data structures
  - Example client and API classes based on example data model
  - Type-safe retrieve/list/iterate methods with unpacked parameters
- ⏳ Phase 3 in progress - Generic Instance API & Example SDK (TypeScript)
  - ✅ Task 0: Development Environment & Tooling Setup complete
    - Vitest selected as testing framework
    - Plain TypeScript with optional Zod for runtime validation
    - Native fetch API as HTTP client
    - TypeScript config at repository root level
    - CI/CD pipeline updated for TypeScript testing
  - ✅ Task 1: HTTP Client Foundation complete
    - HTTPClient class wrapping fetch API
    - Retry logic with exponential backoff
    - Rate limiting support (429 responses with Retry-After)
    - Request/response type system (RequestMessage, SuccessResponse, FailedResponse)
    - Error response parsing matching Python patterns
  - ✅ Task 2: Authentication Support complete
    - Credentials interface (abstract base)
    - TokenCredentials for static token auth
    - OAuthCredentials with token refresh logic
    - PygenClientConfig interface for client configuration
  - ✅ Task 3: Generic Instance Models complete
    - InstanceModel, Instance, InstanceWrite base interfaces/classes
    - InstanceList<T> with array-like behavior
    - Reference types: ViewReference, NodeReference, ContainerReference, InstanceId
    - DataRecord and DataRecordWrite interfaces
    - Serialization/deserialization with camelCase conversion
    - Support for both node and edge instance types
    - Custom date/datetime handling (milliseconds since epoch)

**Current Phase**: Phase 3 - Generic Instance API & Example SDK (TypeScript) (4/11 tasks complete)

**Next Steps**:
1. Continue Phase 3: Filter System (TypeScript)
2. Implement query & response models for TypeScript
3. Build exception hierarchy for TypeScript
4. Implement generic InstanceClient for TypeScript

---

**Document Version**: 1.5
**Last Updated**: December 28, 2025
**For Details**: See full planning documents in `plan/` folder

