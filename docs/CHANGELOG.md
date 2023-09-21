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

## [0.19.0] - 20-09-19
### Added
* Added support for generating filtering options in the generated SDK `.list` methods.
* Added helper methods `as_external_ids` and `as_node_ids` to generated data class lists. Example, if I have a view
  `WorkOrder`, and then do a list call `work_orders = apm_client.work_order.list()`. You can now do
  `work_orders.as_external_ids()` or `work_orders.as_node_ids()` to get a list of the external ids or node ids.
* Option for filtering edges by source id in the generated SDK.

## [0.18.3] - 20-09-19
### Fixed
* Addded missing arguments `overwrite` and `skip-formatting` to `pygen` CLI.

## [0.18.2] - 20-09-18
### Fixed
* When generating an SDK for multiple data models, and two of the views in the different models are of different
   versions. Only one of them would be used to retrieve. This is now fixed.
* Views with a property named `version` will raise an error in the generated SDK. This is now allowed, but
  will overwrite the `node.version` parameter. A warning is issued to the used upon the generation of the SDK.
* Views named `core` will now raise a `ReservedWordConflict`, instead of silently removing the view.

## [0.18.1] - 20-09-18
### Fixed
* Default naming convention for API class name set to unchanged plurality.

## [0.18.0] - 20-09-18
### Added
* Support for configuring naming convention used for the generated SDK.
* Validation of the naming used in the generated SDK. This is to avoid name collisions between data and API classes.

### Changed
* `pygen` no longer pluralize/singularize data and api class names and attributes. You can turn this using the
  configuration.

### Fixed
* Data models containing views with properties without specified `name` caused a `KeyError`. This is now fixed.
* Data models containing views with named `Field` caused namespace collision with `pydantic.Field` in
  the generated data class. This is now fixed.

## [0.17.7] - 20-09-07
### Fixed
* Removed import of `INSTANCES_LIST_LIMIT_DEFAULT` from `cognite-sdk` which no longer exists. This is a private
  constant in the `cognite-sdk` which was removed in `v6.20.0`.

## [0.17.6] - 20-08-24
### Fixed
* `cognite.pygen.generate_sdk_notebook` parameter `overwrite` from `False` to `True`.


## [0.17.5] - 20-08-24
### Fixed
* `utils.cdf.CSVLoader` failed for data type `date`.


## [0.17.4] - 22-08-23
### Fixed
* The `.to_pandas()` of generated list resource failed with `KeyError` if there were no items returned. This is now
  fixed.
* The generated SDK for types with `Date`, `DateTime` or one-to-one relationship fails the `.apply()` method with
  `AttributeError`. This is now fixed.

## [0.17.3] - 20-08-23
### Fixed
* The last fix `SolarFarmAPM.clean()` raising `AttributeError` in `CDF notebook`, did not work as intended.
  Instead, the parameter `auto_confirm` must be set to `True` when calling `SolarFarmAPM.clean()` in a `CDF notebook`.

## [0.17.2] - 20-08-23
### Improved
* Allow `DataModel` and `DataModelList` to be passed directly for `generate_sdk_notebook` and `generate_sdk`,
  which is speeds up the generation time with the demo data and data model.

### Fixed
* `SolarFarmAPM.clean()` raising `AttributeError` in `CDF notebook`. This is now fixed.

## [0.17.1] - 19-08-23
### Fixed
* Bug causing `.to_pandas()` to fail with `pydantic` v1. This is now fixed.

## [0.17.0] - 19-08-23
### Added
* Include demo model `SolarFarmAPM` with data in `cognite.pygen.demo` package.

### Documentation
* Updated documentation with `utils` section.
* Updated documentation with `demo` section.

## [0.16.0] - 18-08-23
### Improved
* Made the parameters `top_level_package` and `client_name` optional in `generate_sdk_notebook`. If not provided,
  default values will be created based on the data model external id.

### Added
* Support for specifying multiple data models in the configuration given in `pyproject.toml`.
* Provide `client-secret` in a `.secret.toml` instead as a CLI argument.

### Fixed
* Raising `DataModelNotFound` if the data model is not found when calling the `generate_sdk` and `generate_sdk_notebook`.
* Avoid adding `tmp/pygen` to path more than once when calling `generate_sdk_notebook`.

## [0.15.3] - 15-08-23
### Fixed

* Bug for pluralization of capitalized snake words, example, `APM_Activity` got pluralized as `activitys` instead
  of `apm_activities`. This is now fixed.

## [0.15.2] - 14-08-23
### Fixed

* When using an Apply type from the generated SDKs, recursive relationships caused an infinite loop. This is now fixed.
* Use correct input types in the generated SDK `InstancesApply` class.

## [0.15.1] - 14-08-23
### Fixed

* Bug causing `cognite.pygen.load_cognite_client_from_toml` to fail for `section = None`. This is now fixed.

## [0.15.0] - 13-08-23
### Added

* Support for formatting the generated SDK with `black`. This is available through the `format_code` parameter
  in `cognite.pygen.generate_sdk` and `cognite.pygen.generate_sdk_notebook` functions.
* Added section in the documentation with `Installation Options`.
* Added section in the documentation with `API` documentation.

### Removed

* `get_cognite_client` this is now obsolete as the factory methods `CogniteClientdefault_oauth_client_credentials`
  does the same.
* `InstancesApply` in the generated SDK `data_classes._core.py`. This is now available in the `cognite-sdk`.

## [0.14.0] - 13-08-23
### Added

* Support for generating an SDK for multiple data models `generate_sdk_notebook`.
* `generate_sdk_notebook` now returns the generated SDK client readily instantiated.
* Allow the generated SDK client to be instantiated with a `CogniteClient` instance.
* When loading a generated SDK from `toml` or using `get_cognite_client_from_toml` you can now specify the section
  to load from.
* `overwrite` parameter to `generate_sdk_notebook` and `generate_sdk` to allow overwriting the existing generated SDK
  if it already exists.

### Removed

* `generate_multimodel_sdk` is removed as it is functionality has been included in `generate_sdk`.

## [0.13.0] - 29-07-23
### Added

* Support for `pydantic` v1 to support using `pygen` with `pyodide`.

### Fixed

* Bug when having a field of type date with name `date` causing `datetime.date` to be excluded by linters. This is now fixed.

## [0.12.3] - 16-07-23
### Fixed

* Types with only edges of one to many caused a `CogniteAPIError` when trying to write. This is now fixed.
* Types with multiple fields of the same type caused duplicated imports in the generated data classes. Thi is now fixed.

## [0.12.2] - 15-07-23
### Fixed

* Marked `model_config` in `DomainModelApply` as class variable.

## [0.12.1] - 14-07-23
### Fixed

* Getting the unique views fails if a property is `SingleHopConnectionDefinition`. This is now fixed.

## [0.12.0] - 14-07-23
### Added

* Support for generating a client from multiple data models. This is currently an experimental feature and only exposed
  through `cognite.pygen.generate_multimodel_sdk` and not available through the CLI.

## [0.11.7] - 14-07-23
### Fixed

* Replace all relative imports with absolute imports as this was causing comparison issues in the generated code.

## [0.11.6] - 13-07-23
### Fixed

* Bug causing camelCase fields to be incorrectly converted to PascalCase. This is now fixed.
* Order of type hints in write classes with one to many edges. Pydantic requires the str option to be last. This is now fixed.
* `pydantic` requires the `Optional` to be imported, even if it is not used. This is now fixed.

## [0.11.5] - 11-07-23
### Fixed

* The variable `output_dir` was ignored if present in `pyproject.toml` this is now fixed.

### Added

* `pygen` version is now printed when running `pygen --version`.

## [0.11.4] - 06-07-23
### Fixed

* Missing `from pydantic import Field` occurring in the generated SDK. This is now fixed.

## [0.11.3] - 06-07-23
### Changed

* Upgraded `pydantic` to `v2.0`.
* Removed the `CircularModel` from the generated SDK. This was a workaround to avoid infinite recursion
  when getting a string representation of a model. This is replaced by `repr=False` on fields that are
  recursive.

## [0.11.2] - 04-07-23
### Fixed

* Exposing the function `generate_sdk` in the `pygen` package. This is now fixed.

## [0.11.1] - 04-07-23
### Fixed

* The optional `CLI` version was used in the presence of a `pyproject.toml` file, and not a `pyproject.toml` with
  a `[tool.pygen]` section. This is now fixed.

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
* Optmised use of cache internally.


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
