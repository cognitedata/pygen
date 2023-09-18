## Description
Please describe the change you have made.

## Checklist:
- [ ] Tests added/updated.
- [ ] Documentation updated.
- [ ] Changelog updated in [CHANGELOG.md](https://github.com/cognitedata/cognite-gql-pygen/blob/main/CHANGELOG.md).
- [ ] Version bumped. If triggering a new release is desired, bump the version number in
  [version.py](https://github.com/cognitedata/cognite-gql-pygen/blob/main/cognite/gqlpygen/version.py) and
  [pyproject.toml](https://github.com/cognitedata/cognite-gql-pygen/blob/main/pyproject.toml) per [semantic versioning](https://semver.org/).
- [ ] Regenerate example SDK `export PYTHONPATH=. && python scripts/dev.py generate-sdks`. Need to be run both
  for `pydantic` `v1` and `v2` environments.
