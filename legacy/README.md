# Pygen v1 (Legacy)

This folder contains the original Pygen v1 codebase, preserved during the v2 rewrite.

## Purpose

The v1 code is maintained here to:
- Allow v1 tests to continue running during v2 development
- Enable comparison between v1 and v2 implementations
- Support bug fixes if critical issues arise during v2 development
- Provide reference for migration efforts

## Structure

- `cognite/pygen/` - Original v1 source code
- `tests/` - Original v1 test suite
- `examples/` - Original v1 generated SDK examples

## Timeline

This legacy code will be removed after the v2.0.0 release is stable and users have successfully migrated.

## Running v1 Tests

To run the v1 tests:
```bash
pytest legacy/tests/
```

## Important

Do not modify this code unless fixing critical bugs. All new development should happen in the v2 codebase (root `cognite/pygen/` directory).

