# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Changes are grouped as follows
- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon-to-be removed features.
- `Improved` for transparent changes, e.g. better performance.
- `Removed` for now removed features.
- `Fixed` for any bug fixes.
- `Security` in case of vulnerabilities.

## [0.2.0] - 19-04-23

### Added

* Support for converting from `.graphql` to `pydantic`. This means you can now write your types in a `.graphql` file
  and automatically generate the `pytandic` counterpart.
* CLI for doing the conversion between GraphQL and `pydantic`. The following two commands
  * `dm togql` converts a pydantic schema to `.graphql` and creates the `client.py` which is the SDK to interact
    with the data model through Python.
  * `dm topython` converts a `.graphql` to `pytandic` classes. This command also creates the `client.py`, i.e., the
    SDK to interact with the data model through Pytho
* Documentation of motivation and usage of package.

### Changed

* CLI command going from `gqlpygen` to `pygen`. This is for ease of use.


## [0.1.4]

Changed configuration / settings from a static `config.yaml` to dynaconf and `settings.toml`.

## [0.1.3]

Changed package name from `fdm` to `dm_clients`.

## [0.1.2]

Added support for `Timestamp` and `JSONObject` types.

## [0.1.1]

Added `dm_clients` package.

## [0.1.0]

Initial commit.
