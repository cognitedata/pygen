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

## [0.11.0] - 02-07-23
### Added

* Support for views with data in multiple containers in different spaces.
* Support for loading cli defaults from `pyproject.toml`.
* Support for generating SDK on the fly in a `jupyter notebook`.

### Improved

* Refactoring of the code generator ensuring consistency in field, class, and attribute names.

### Fixed

* Edge fields with camel cased a `AttributeError` in the generated code. This is now fixed.

## [0.10.6] - 30-06-23

### Fixed

* The paths for the generated SDK and client were not correctly set. This is now fixed. This caused a `ModuleNotFoundError`
  when trying to import from the generated SDK.

## [0.10.5] - 30-06-23

### Fixed

* The `.apply()` method failed to update nested nodes. This is now fixed. This was caused by the last fix in 0.10.4,
  where there was an issue with relative vs absolute imports. As a result `--top-level-package` has been introduced
  See below.

### Changed

* The argument `---sdk-name-snake` is renamed to `--top-level-package` and now is expected to contain the path to
  the top level package. For example, `--top-level-package movie_sdk.client` will place the generated SDK in the
  `movie_sdk` package and the client in the `movie_sdk.client` package. Earlier the `.client` was automatically
  appended to the `---sdk-name-snake` argument.
* The argument `--client-name-pascal` is renamed to `--client-name` and the word `Client` is no longer appended to it.


## [0.10.4] - 29-06-23

### Fixed

* Only use relative imports to avoid requiring to be top level package.
* Writing a direct relation. (Earlier direct relation was treated as an edge, which is a bug).
* Types with snake case names caused `flake8` errors are now fixed.
* import `datetime` when one or more fields are of type `timestamp`.

## [0.10.3] - 28-06-23

### Fixed

* Compatible with `cognite-sdk>=6.5`

## [0.10.2] - 26-06-23

### Fixed

* Bug when trying to write an edge set to `None` in an Apply class. It caused a `TypeError` to be raised. Now, an edge
  set to `None` is ignored.

## [0.10.1] - 26-06-23

### Fixed

* Workaround for missing `inline_views` in `data_modeling.data_models.retrieve` in `cognite-sdk`.

## [0.10.0] - 26-06-23

A complete rewrite of the package. It is now DMS based instead of graphql schema.

### Improved
* `pygen` is now a generator package, meaning you generate code with it, but do not neet to have `pygen`
  as a dependency in your project.
* The CLI now depends on connection to CDF to download data models.
* `pygen` is now built on top of `cognit-sdk` and thus do not have its own custom implementation of `data modeling`
  client.

**Caveat** Not a single line of code from the previous version is used in this version. Thus, no of the own functionality is
  available in this version. This is a complete rewrite.

## [0.9.1] - 09-06-23

### Fixed

* Allow properties to be optional in dms API.


## [0.9.0] - 22-05-23

### Changed

* Package name from `cognite-gql-pygen` to `pygen`.
* Moved `dm_client` into the main `pygen` package.
* CLI command going from `dm` to `pygen` for consistency.


## [0.8.0] - 14-05-23

### Improved

* Split dependencies into required and optional.
* Removed all unused dependencies.
* Updated documentation to reflect non-CLI usage.


## [0.7.0] - 13-05-23

### Fix

* Less strict versioning of other packages to support usage in streamlit pyodide runtime.

## [0.6.0] - 13-05-23

### Fix

* Less strict `packaging` versioning (>=21) to support usage in streamlit pyodide runtime.

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
