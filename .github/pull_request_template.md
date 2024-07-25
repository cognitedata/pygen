## Description
Please describe the change you have made.

## Checklist:
- [ ] Tests added/updated.
- [ ] Documentation updated.
- [ ] Changelog updated in [CHANGELOG.md](https://github.com/cognitedata/pygen/blob/main/docs/CHANGELOG.md).
- [ ] Version bumped. If triggering a new release is desired (?), bump the version number by running `python dev.py bump --patch` (replace `--patch` with `--minor` or `--major` per [semantic versioning](https://semver.org/)).
- [ ] Regenerate example SDKs `export PYTHONPATH=. && python dev.py generate-sdks`. Need to be run both
  for `pydantic` `v1` and `v2` environments.
