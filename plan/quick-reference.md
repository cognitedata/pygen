# Pygen Rewrite - Quick Reference

**Quick lookup for key information. For details, see the full planning documents.**

---

## 📋 Project Summary

**Goal**: Rewrite Pygen from scratch for better performance, scalability, and multi-language support

**Timeline**: 28-42 weeks (7-10 months)

**Status**: Phase 3 Complete ✅ - Ready for Phase 4

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
| 0 | Foundation | 1 week | ✅ Complete | Project reorganized, v1 in _legacy/ |
| 1 | Pygen Client | ~2 days | ✅ Complete | httpx-based CDF client with HTTPClient |
| 2 | Generic Instance API (Python) | ~5 days | ✅ Complete | InstanceClient, InstanceAPI, Example SDK |
| 3 | Generic Instance API (TypeScript) | ~2 days | ✅ Complete | TypeScript equivalent of Phase 2 |
| 4 | PygenModel | 2-3 weeks | ⏳ Pending | Internal model for code generation |
| 5 | Code Generation from PygenModel | 3-4 weeks | ⏳ Pending | Python & TypeScript SDK generation |
| 6 | CLI, Feature Parity & Advanced | 3-4 weeks | ⏳ Pending | CLI + match original Pygen |
| 7 | Query Builder & Advanced Queries | 2-3 weeks | ⏳ Pending | Query builder (parallel with P6) |
| 8 | API Service | 2-3 weeks | ⏳ Optional | On-demand SDK generation |
| 9 | Production Hardening | 2-3 weeks | ⏳ Pending | Hardening, optimization |
| 10 | Migration & Documentation | 2-3 weeks | ⏳ Pending | Migration guide, docs, release |

---

## 🏗️ Architecture Layers

```
┌─────────────────────────────────────┐
│  1. Pygen Client (_client/)         │  ← HTTPClient, Auth, Resource APIs
├─────────────────────────────────────┤
│  2. Python SDK (_python/)           │  ← Generic InstanceAPI/InstanceClient
├─────────────────────────────────────┤
│  3. TypeScript SDK (_typescript/)   │  ← Generic InstanceAPI/InstanceClient
├─────────────────────────────────────┤
│  4. PygenModel (_pygen_model/)      │  ← Internal model for generation
├─────────────────────────────────────┤
│  5. Generator (_generator/)         │  ← Transformer + Code generation
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
| PygenModel | Internal representation for multi-language generation |
| Lazy by default | Solves scalability issues |
| F-string templates | Simple, native Python, no extra dependencies |
| Python 3.10+ | Modern features, type hints |
| Typer for CLI | Modern, type-safe CLI |
| >90% coverage | Professional-grade quality |

---

## 📦 Technology Stack

**Core**:
- Python 3.10+
- httpx (HTTP client)
- Pydantic v2 (data models)
- Python f-strings (templates, no Jinja2)
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
├── _client/            # Pygen Client (Phase 1) ✅
│   ├── auth/           # Authentication handlers
│   ├── http_client/    # HTTPClient wrapper
│   ├── models/         # Pydantic models for CDF API
│   └── resources/      # Resource APIs
├── _example_datamodel/ # Example data model for patterns
├── _generator/         # Code generation (Phases 4-5) ⏳
│   ├── config.py       # PygenSDKConfig
│   ├── gen_functions.py # generate_sdk()
│   ├── generator.py    # Generator base class
│   ├── transformer.py  # CDF → PygenModel
│   ├── python.py       # PythonGenerator
│   ├── typescript.py   # TypeScriptGenerator
│   └── templates/      # f-string based templates
├── _legacy/            # V1 code (delete after v2.0.0)
├── _pygen_model/       # Internal model (Phase 4) ⏳
│   ├── _model.py       # CodeModel base
│   ├── _data_class.py  # DataClass, ReadDataClass
│   └── _field.py       # Field representation
├── _python/            # Python SDK (Phase 2) ✅
│   ├── instance_api/   # Generic InstanceAPI, InstanceClient
│   └── example/        # Example SDK
├── _typescript/        # TypeScript SDK (Phase 3) ✅
│   ├── instance_api/   # Generic InstanceAPI, InstanceClient
│   └── example/        # Example SDK
├── _utils/             # Utility functions
└── cli.py              # CLI Interface (Phase 6)
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
- [x] Phase 2 complete (InstanceClient, InstanceAPI, Example SDK - Python)
- [x] Phase 3 complete (TypeScript Generic Instance API & Example SDK)
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
| **M2** | Phase 2 complete (Python Generic API + Example SDK) | Month 1 | ✅ Complete |
| **M2.5** | Phase 3 complete (TypeScript Generic API + Example SDK) | Month 1 | ✅ Complete |
| **M3** | Phase 4 complete (PygenModel) | Month 2 | ⏳ Pending |
| **M4** | Phase 5 complete (Can generate Python & TypeScript) | Month 3 | ⏳ Pending |
| **M5** | Phase 6 complete (CLI + Feature parity) | Month 4 | ⏳ Pending |
| **M6** | Phase 9 complete (Production ready, beta) | Month 5 | ⏳ Pending |
| **M7** | Phase 10 complete (v2.0.0 release) | Month 6 | ⏳ Pending |

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

### PygenModel
```
CDF Data Model → PygenModel → Python SDK
                      ↓
                      └─→ TypeScript SDK
                      └─→ Future: C#, PySpark SDKs
```

### F-String-Based Generation
```python
# template function
def generate_class(class_name: str, properties: list[Property]) -> str:
    props = ", ".join(f"{p.name}: {p.type}" for p in properties)
    return f'''
class {class_name}:
    def __init__(self, {props}):
        ...
'''

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

**Phase 3**: ✅ Complete (December 29, 2025)

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
- ✅ Phase 3 complete - Generic Instance API & Example SDK (TypeScript)
  - ✅ Task 0: Development Environment & Tooling Setup
  - ✅ Task 1: HTTP Client Foundation
  - ✅ Task 2: Authentication Support
  - ✅ Task 3: Generic Instance Models
  - ✅ Task 4: Filter System
  - ✅ Task 5: Runtime Migration (Node to Deno)
  - ✅ Task 6: Query & Response Models
  - ✅ Task 7: Exception Hierarchy
  - ✅ Task 8: Generic InstanceClient
  - ✅ Task 9: Generic InstanceAPI
  - ✅ Task 10: Example Data Classes
  - ✅ Task 11: Example API Classes

**Current Phase**: Ready for Phase 4 - PygenModel

**Next Steps**:
1. Begin Phase 4: Validation layer for data models
2. Complete Field, Connection, DataClass models in `_pygen_model/`
3. Build transformer from CDF ViewResponse to PygenModel
4. Test with example data model to validate patterns

---

**Document Version**: 1.7
**Last Updated**: December 31, 2025
**For Details**: See full planning documents in `plan/` folder

