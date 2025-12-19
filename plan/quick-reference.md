# Pygen Rewrite - Quick Reference

**Quick lookup for key information. For details, see the full planning documents.**

---

## 📋 Project Summary

**Goal**: Rewrite Pygen from scratch for better performance, scalability, and multi-language support

**Timeline**: 23-33 weeks (6-8 months)

**Status**: Planning Complete ✅

---

## 🎯 Key Objectives

1. **Performance**: 5-10x faster via httpx + Pydantic v2
2. **Scalability**: Lazy evaluation for unlimited dataset sizes
3. **Multi-Language**: Python, TypeScript, C#, PySpark
4. **Quality**: >90% test coverage
5. **Maintainability**: Clean architecture, well-documented

---

## 📅 8-Phase Roadmap

| Phase | Name | Duration | Key Deliverable |
|-------|------|----------|-----------------|
| 0 | Foundation | 1-2 weeks | Project setup, tooling, CI/CD |
| 1 | Pygen Client | 3-4 weeks | httpx-based CDF client |
| 2 | IR | 2-3 weeks | Language-agnostic representation |
| 3 | Python Generator | 3-4 weeks | Basic Python SDK generation |
| 4 | Lazy Evaluation | 3-4 weeks | Scalable data access |
| 5 | Feature Parity | 4-6 weeks | Match original Pygen |
| 6 | Multi-Language | 3-4 weeks | TypeScript generator |
| 7 | Production | 2-3 weeks | Hardening, optimization |
| 8 | Release | 2-3 weeks | Migration guide, docs |

---

## 🏗️ Architecture Layers

```
┌─────────────────────────────────────┐
│  1. Pygen Client (httpx + Pydantic) │  ← Replace cognite-sdk
├─────────────────────────────────────┤
│  2. Intermediate Representation     │  ← Language-agnostic
├─────────────────────────────────────┤
│  3. Code Generation (Jinja2)        │  ← Multi-language
├─────────────────────────────────────┤
│  4. Generated Runtime Support       │  ← Lazy evaluation
└─────────────────────────────────────┘
```

---

## 🔑 Key Technical Decisions

| Decision | Rationale |
|----------|-----------|
| httpx over requests | Async/sync, HTTP/2, better performance |
| Pydantic v2 | 5-17x faster, excellent validation |
| IR layer | Enables multi-language support |
| Lazy by default | Solves scalability issues |
| Template-based | Readable, maintainable, customizable |
| Python 3.10+ | Modern features, type hints |
| >90% coverage | Professional-grade quality |

---

## 📦 Technology Stack

**Core**:
- Python 3.10+
- httpx (HTTP client)
- Pydantic v2 (data models)
- Jinja2 (templates)

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
pygen/
├── client/              # Pygen Client (Phase 1)
│   ├── core.py
│   ├── auth.py
│   ├── http.py
│   ├── models/
│   └── resources/
├── ir/                  # Intermediate Representation (Phase 2)
│   ├── models.py
│   ├── types.py
│   ├── parser.py
│   └── validator.py
├── generation/          # Code Generation (Phase 3+)
│   ├── base.py
│   ├── python/
│   ├── typescript/
│   └── ...
├── runtime/             # Generated Runtime (Phase 4)
│   ├── base.py
│   ├── lazy.py
│   └── query.py
└── cli.py               # CLI Interface
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

| Milestone | Description | Target |
|-----------|-------------|--------|
| **M1** | Phase 1 complete (Client works) | Month 1-2 |
| **M2** | Phase 3 complete (Can generate Python) | Month 3 |
| **M3** | Phase 5 complete (Feature parity) | Month 5 |
| **M4** | Phase 7 complete (Beta) | Month 7 |
| **M5** | Phase 8 complete (v2.0.0) | Month 8 |

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

**Planning Phase**: ✅ Complete

**Next Steps**:
1. Review and approve plans
2. Begin Phase 0 (Foundation)
3. Set up development environment
4. Start implementation!

---

**Document Version**: 1.0
**Last Updated**: December 19, 2025
**For Details**: See full planning documents in `plan/` folder

