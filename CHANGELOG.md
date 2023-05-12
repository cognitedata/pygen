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

## [0.5.1] - 12-05-23

### Improved

* Minor tweaks to templates for Python codegen, mostly comments and docstring.


## [0.5.0] - 10-05-23

### Added

* Testing Client

### Fix

* Issue when the output folder is not relative to the current working directory.


## [0.4.5] - 10-05-23

### Fixed

* Support for nested types
* Typo for `Int` built-in type, was incorrecly `Integer`

### Improved

* Items that are references are now showing this in __repr__ and __str__.


## [0.4.4] - 09-05-23

### Added

* Added compatibility with cognite-sdk 6.x.x


## [0.4.3] - 05-05-23

### Fixed

* Set the license of the package in poetry build.


## [0.4.2] - 05-05-23

### Added

* Support for basic built-in types for the parser (`bool`, `int`, `float`, with `str` from before)


## [0.4.1] - 03-05-23

### Added

* Support for adding relationships via `.connect`

### Changed

* Optimised use of cache internally.


## [0.4.0] - 26-04-23

### Added

* Support for `pip install -e` for local development.

### Changed

* In the CLI, all commands are not under one `dm` tool.

### Removed

* All bash scripts have been removed (under `dm_clinents/bin`).
* CLI tool `dm_clients` was removed (functions moved to `dm`).

### Fixed

* Added a lock around usages of non-thread-safe cache.


## [0.3.0] - 24-04-23

### Fixed

* In the CLI, `dm topython` the argument `name` is now a option instead of a positional argument. This matched the
  documentation in the README.

### Changed

* Renamed `DomainModelAPI.create` to `DomainModelAPI.apply`, to reflect the usage of the underlying endpoint.


## [0.2.1] - 24-04-23

### Fixed

* Fixed a problem with reading annotations caused by reverse ordering of dataclasses (forward refs).


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
