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

## [0.99.0] - 30-01-24
Beta release of `pygen`.
### Changed
* Similar to the change in `0.37.0`, the `.delete` method like the `.apply` method of the generated SDK is
  moved from the API class to the client class. For example, instead of `my_client.windmill.delete(...)` you now
  do `my_client.delete(...)`. The motivation is that all `.delete` methods are the same, and thus it is more intuitive
  to have them on the client class. In addition, having the `.delete` method on each API class encourages the user to
  use an anti-pattern of deleting nodes in multiple small request, instead of batching them together in fewer requests,
  which is more efficient. The exising `.delete` methods on the API classes are still available,
  but will be removed in version 1.

## [0.37.2] - 25-01-26
### Fixed
* When calling the query method `my_client.my_view(limit=-1).query()`, setting `limit=-1` only returned the first 100 results. Instead
  of all as expected. This is now fixed.

## [0.37.1] - 25-01-24
### Fixed
* Doing `.apply` on with an edge with properties, would raise `AttributeError: '<DataClassName>' object has no attribute 'data_recrod'`.
  This is now fixed.
* When calling `.to_pandas()` `external_id` would be repeated twice. This is now fixed.

## [0.37.0] - 24-01-24
### Changed
* The `.apply` method for the generated SDK is moved from the API class to the client class. For example,
  instead of `my_client.windmill.apply(...)` you now do `my_client.apply(...)`. The motivation is that all
  `.apply` methods are the same, and thus it is more intuitive to have them on the client class. In addition,
  having the `.apply` method on each API class encourages the user to use an anti-pattern of creating nodes
  and edges in multiple small request, instead of batching them together in fewer requests, which is more efficient.
  The exising `.apply` methods on the API classes are still available, but will be removed in version 1.

## [0.36.1] - 23-01-24
### Fix
* If you use `generate_sdk_notebook` to generate multiple SDKs in one session with the default arguments, the
  `pygen` temporary directory would be cleaned before generating each SDK and thus removing the previous SDKs. This
  is now fixed by ensuring each SDK each generated in a separate temporary directory.

## [0.36.0] - 22-01-24
### Changed (BREAKING)
* The `node` and `edge` properties `version`, `created_time`, `last_updated_time`, `deleted_time`, and
  `existing_version` have been moved from the generated data class to a sub class `data_record`. This is
  to make it clear that these properties are not part of the data model, but are metadata about the data. They
  are available under `my_windmill = my_client.windmill.retrieve('Utsira1')` as `my_windmill.data_record.version`,
  and so on.
* The default behavior of `generate_sdk` and `generate_sdk_notebook`, the parameter `top_level_package`
  is no longer set to `<model_id:snake_case>.client` but instead `<model_id:snake_case>`. This means
  if your data model is named `MyDataModel`, the generated SDK will be placed in the package `my_data_model`,
  and thus you import the client as `from my_data_model import MyDataModelClient`. This is to make
  it more intuitive to import the generated SDK client.

### Change
* The `.to_pandas()` method no longer hides parent properties by default. This was originally used to hide
  the `data_record` properties, but since these are now in a sub class, this is no longer needed.

## [0.35.4] - 22-01-24
### Fixed
* When generating an SDK from data model with a view implementing another view not in the data model, the `pygen`
  would raise `KeyError: ViewId(space='<space>', external_id='<external ID>', version='<version>')`. This is now fixed

## [0.35.3] - 19-01-24
### Fixed
* For edge views, `retrieve` and `list` raised `AttributeError: 'NodeId' object has no attribute 'as_tuple'`.
  This has been fixed by adding this method to the `cognite-sdk`, which means that `pygen` now requires
  `cognite-sdk>=7.13.6`.
* The field `existing_version` was not set in the `.as_apply()` method for the generated read data classes.
  This has now been fixed.

## [0.35.2] - 18-01-24
### Fixed
* If a view has a dependency on another `view` named `Field` importing the generated client would raise
  a `NameError: name 'JsonValue' is not defined`. This has now been fixed.

## [0.35.1] - 15-01-24
### Fixed
* In the `MockGenerator`, when generating edges, the `end_node` now accounts for interfaces and is set to the
  children nodes of the interface, instead of the interface itself.

### Added
* HTML representation method to generated `client` class. This is useful when using the generated SDK in a Jupyter
  notebook.

## [0.35.0] - 07-01-24
### Added
* Module `cognite.pygen.utils.mock_generator` with `MockGenerator` class for generating mock data for a given data model.

### Fixed
* When calling `.aggregate("count")` on the generated SDK, the first property was chose, which can be nullable and thus
  give an incorrect count. This is now fixed by instead choosing `externalId` as the property to count.

## [0.34.1] - 03-01-24
### Fixed
* Bug in `pygen` when generating SDKs from views which implements another view that is also implemented by that
  parent views as well. For example, if you have a view `A` which implements `B` and `C`, and `B` also implements `C`,
  then `pygen` would raise a `TypeError: Cannot create a consistent method resolution`. This is now fixed.

## [0.34.0] - 01-01-24
### Added
* Option for returning generated SDK as a `dict[Path, str]` instead of writing to disk when calling `generate_sdk`.
* Option for cleaning the `pygen` temporary directly when calling `generate_sdk_notebook`. This is useful to ensure
  that previous generated SDKs are not interfering with the current SDK by accident.
* Option `write_none` to the generated `.apply()` methods. This is useful when you want to set a property to `None`.
  By default, `pygen` will ignore properties set to `None` in the `.apply()` methods.
### Fixed
* `pygen` now handles views with the same `external_id` in different spaces and versions. Note that this can only occur if
  `pygen` is used with multiple data models.
* When using `pydantic` `v1` with a view that has a dependency on a non-writable view, importing the generate client
  raised `NameError`. This is now fixed.
### Changed
* The generated `read` classes now reflects whether properties are required or not. Earlier, all read properties
  were optional. The motivation for this change is to make it easier to use `mypy` with the generated SDK.


## [0.33.0] - 01-01-24
### Added
* The generated data classes now are inherited if the view is implementing another view.
* Non-writable (and non-interfaces) data classes are no longer generating a `Apply` (write) classes
  and `.apply` method in the generated SDK.
### Changed
* The field `type` is renamed to `node_type` or `edge_type` in the generated data classes. This is to avoid
  name collisions with the `type` name in Python.

## [0.32.6] - 30-12-23
### Added
* Setting `type` on nodes.

### Fixed
* Bug in `pygen` with views ending in numbers. The number was removed from the name of the resulting data class.
  This is now fixed.
* Bug in `pygen` when retrieving a list of `TimeSeries` caused `pydantic` to raise a `ValidationError` when retrieving
  data from CDF. This is now fixed.
* Bug in `pygen` when creating nodes for a view with a list of `timestamps` or `dates`. This is now fixed.
* Bug in `pygen` when default values are set, the read class should have `None` and not the default values.
  This is now fixed.
* Bug in `DomainModelApply.from_instance()` raised `AttributeError` when called. This is now fixed.
* Bug when getting in a sequence and getting an empty response on `.retrieve` in the generated SDK you got an
  `None` instead of an empty list. This is now fixed.
* When filtering on a direct relation in the generated SDK with an `external_id`, the `space` of the view was used
  and not the `DEFAULT_INSTANCE_SPACE`. This is now fixed.
* When `quering` and including a direct relation in the generated SDK, the property name and the field name were
  assumed to be the same. This is now fixed.
* When retrieving a direct relations with a different space than the source node, the `space` was not set correctly
  in the generated SDK. This is now fixed.

## [0.32.5] - 21-12-23
### Fixed
* In version `7.6-7.8.1` of the `cognite-sdk` there is a bug that breaks `pygen`. This version
  requires `cognite-sdk>=7.8.3`.

## [0.32.4] - 20-12-23
### Fixed
* Correctly recognite `[timeseries]` fields and serialize the values for use in `.apply` methods.


## [0.32.3] - 18-12-23
### Fixed
* `pygen` now handles connections with directions `inwards` instead of assuming `outwards` for all connections.

## [0.32.2] - 15-12-23
### Fixed
* Filtering on a boolean value in the generated SDK with value `False` returned both `True` and `False` values. This is now fixed.
  (The fix from yesterday only solved the issue for `True` values).

## [0.32.1] - 14-12-23
### Fixed
* Filtering on a boolean value in the generated SDK returned both `True` and `False` values. This is now fixed.

## [0.32.0] - 09-12-23
### Added
* Support for views defined with an equals filter node type. This is a common pattern for views that you
  need to account for when you create the nodes that are connected to the view. For example, if you have a
  view `Pump` with a filter `type = "pump"`, then, all nodes connected to the view must have the type `pump`.
  This is now automatically enforced in the generated SDK.
### Fixed
* Using the `DEFAULT_INSTANCE_SPACE` constants in the generated `.retrieve` and `.delete` methods generated
  for each node view APIs as the default space.
* Overload methods was missing `space` in `.retrieve` for node view APIs.

## [0.31.2] - 06-12-23
### Fixed
* The `end_node` in the generated edge data classes field did not handle multiple edges of the same type.
  This is now fixed.

## [0.31.1] - 06-12-23
### Fixed
* Query supports direct relations. This was not the case for the `query` method in the generated SDK. This is now fixed.
* Direct relations can have start and end nodes in different spaces.
* Edges with properties were assumed to have all edges of the same type. Now, an edge with properties (a view), can
  be used with edges of different types.
* Lowered required version of `typing_extensions` to `>=4.0`.
* `pygen` no longer generates API and Query API class for edge views.
### Changed
* Data classes for edges no longer have an end node of a specific type. Instead, the `end_node` field supports
  all node types that can be connected to the start node. For example, in the `EquipmentUnit` with the data class
  `StartEndTime` used to have a field `equipment_module` which is now replaced with `end_node` which can be of type
  `EquipmentModule` or any other end node that uses a `StartEndTime` edge.
* When calling `.query()` in the generated SDK, including the end node is automatically included in the query,
  and no longer optional.
### Added
* Option for setting a default instance space when generating an SDK.

## [0.31.0] - 28-11-23
### Added
* Support for `query`, [see documentation for more information](https://cognite-pygen.readthedocs-hosted.com/en/latest/examples/movie_domain.html#querying).
* Support for reading and writing edges with properties.
* Support for writing TimeSeries when calling `.apply` on a generated SDK.
### Changed
* Calling `.to_pandas` on generated classes does not include the node and edge properties such as `version`, `created_time`, etc.
  by default. Instead, you can pass `include_instance_properties=True` to include these properties.
### Fixed
* Upgrade `pygen` to use `cognite-sdk` v7.
* `to_pandas` on apply list failed with `KeyError`. This is now fixed.

## [0.30.5] - 06-11-23
### Fixed
* Bug when calling `.list()` and `.retrieve` in generated NodeAPI classes, caused a `CogniteAPIError` with `400` status code due
  to invalid filter. This is now fixed.
* Bug when generating from views with timeseries, the method `.retrieve_dataframe_in_tz` was missing arguments
  `target_unit` and `target_unit_system`. This is now fixed.
* Lowered required version of `typing_extensions` to `>=4.4`.

## [0.30.4] - 06-11-23
### Fixed
* `pygen` now handles name collisions `pydantic` methods and the `DomainModel` and `DomainModelApply` classes.

## [0.30.3] - 06-11-23
### Fixed
* `pygen` now handles name collisions with Python keywords, bulitins, and `pygen` reserved words.

## [0.30.2] - 06-11-23
### Fixed
* Views without primitive fields caused `ImportError` in the `generate_sdk_notebook` method. This is now fixed.

## [0.30.1] - 05-11-23
### Fixed
* When setting `space` for a node in the generated SDK, the automatically created edges would not be created in the
  same space. This is now fixed.
* When calling `.retrieve` or `.list` in the generated EdgeAPI class, the generated SDK now uses the model `space`
  to filter on `["edge", "type"]` instead of the `space` passed in.
* When calling `.apply` all one-to-one edges had `space` set to the model space instead of the `space` set in the
  node object. This is now fixed.
* When calling `.list` and `.retrieve` in the generated NodeAPI classes, the `space` parameter was not propagated
  to the underlying `.list` and `.retrieve` calls to the EdgeAPI classes. This is now fixed.

## [0.30.0] - 04-11-23
### Added
* `pygen` now generates docstrings for all methods in the generated SDK. The docstrings are based on the
  documentation in the data model.
* Support for `target_unit` and `target_unit_system` in the generated SDK for `Timeseries` and `DataPoints`.

## [0.29.0] - 02-11-23
### Added
* Support for filtering on `space` in all filter methods.
### Fixed
* THe fix for filtering on `datetime` in `0.27.3` did not work as intended. This is now fixed.

## [0.28.0] - 02-11-23
### Added
* Support for `Support generic OIDC` in the `pygen` CLI.

## [0.27.3] - 02-11-23
### Fixed
* When calling `.apply` the view generating the data class is used to write instead of the container of the property.
  This caused some apply operations to not be retrievable again with the `list` method.
* When filtering in `.list`, `.search`, or `.timeseries` `pygen` ensures that the timespec (milliseconds) used
  by the API is used.

## [0.27.2] - 01-11-23
### Fixed
* When calling `.apply` in the generated data classe, the default class `space` was always used. This is now fixed.

## [0.27.1] - 28-10-23
### Fixed
* Pygen automatically converts `datetime` to `datetime.datetime` in the generated SDK to the timespec (milliseconds)
  used by the API when writing to CDF

## [0.27.0] - 28-10-23
### Added
* Hook for `external_id` factory method in generated SDK. This allows for custom external id generation. This is useful
  when using `pygen` for ingestion and the source data does no come with an external id.
### Changed
* Write type hint in generated classes now include alias if needed.

## [0.26.1] - 23-10-23
### Changed
* Loosen requirement on typing_extensions to >=4.5

## [0.26.0] - 22-10-23
### Added
* Support for aggregation in generated SDK.
### Fixed
* `pygen` generated invalid code for views without text fields. This is now fixed.

## [0.25.0] - 22-10-23
### Added
* Support for search in generated SDK.

## [0.24.0] - 21-10-23
### Added
* Support for filtering on direct edges in the generated SDK. Currently supported filters, `Equals` and `In`.
* `.to_pandas()` method for all generated resource classes, and automatically displaying resource classes
  in pandas dataframes in Jupyter notebooks.

## [0.23.1] - 19-10-23
### Fixed
* Listing a type with a one-to-many edges which returned more than 5000 nodes, resulted in a
  `CogniteAPIError` with `400` status code. This is now fixed.

## [0.23.0] - 19-10-23
### Added
* Parameter `config` to `cognite.pygen.generate_sdk` and `cognite.pygen.generate_sdk_notebook`
  to allow configuration of the generated SDK, i.e., naming convention, formatting, etc.
* Validation of `model_id` argument in `cognite.pygen.generate_sdk` and `cognite.pygen.generate_sdk_notebook`.
  To ensure all `views` are passed in with the data model.

### Removed
* `cognite.pygen.SDKGenerator` and `cognite.pygen.write_to_disk` are removed. These functions
  contained duplicated functionality which has been covered by `generate_sdk`.
* The parameters `logger`, `overwrite` and `format_code` have been removed from `cognite.pygen.generate_sdk_notebook`.
  This is to simply the signature of this function.

### Changed
* The order of `client` and `model_id` in `cognite.pygen.generate_sdk` and `cognite.pygen.generate_sdk_notebook`
  has been swapped. This is because `client` has become an optional argument (it is not necessary when passing in
  a data model as the `model_id` argument). **Note** This is a **breaking change**, but since `pygen` is still
  on 0-version, it is allowed.

## [0.22.1] - 19-10-23
### Fixed
* Auto creating start and end nodes when calling `.apply()` method of generated SDK. This was an issue when
  creating more than 1 000 instances (nodes and edges), which get split into multiple requests. This could cause
  an edge to be created before the source or target node. `pygen` generated SDKs always create edges based on the
  source and target nodes, so it is always safe to auto create these.

## [0.22.0] - 06-10-23
### Changed
* The attribute `space` is now an attribute in all generated data classes (it used to be a class variable).
* Attributes `sources`, `view_id`, `class_type`, `class_apply_type`, `class_list` in the generated API class
  are made private. This is to avoid cluttering the method space when using the generated SDK for exploration.

### Added
* Option for setting `space` in generated `delete` methods for nodes, and `retrieve` methods for edges.

## [0.21.1] - 27-09-23
### Fixed
* Edges with property name not equal to field name in generated data class failed to create the correct alias. Example
  a property named `linkedAssets` with a field named `linked_assets` would fail to create the correct alias. This is
  now fixed.

## [0.21.0] - 27-09-23
### Changed
* Listable primitive and one-to-many edges no accepts None if they are optional, and has `None` as a default value. Earlier
  the default value was an empty list.
### Removed
* Helper method `one_to_many_fields` for DomainCore. This did not distinguish between one-to-many edges and a list of
  strings.

## [0.20.5] - 25-09-23
### Fixed
* Listing/retrieving a value of type `JSONObject` in the generated SDK failed with `ValueError`. This is now fixed.

## [0.20.4] - 25-09-23
### Fixed
* The views with singular == plural caused `pygen` to generate invalid Python syntax. This is now fixed.
* Views with Timeseries without a Timestamp field caused `pygen` to no `import datetime`. This is now fixed.

## [0.20.3] - 25-09-23
### Fixed
* The views with properties of type primitive listable, caused `pygen` to generate invalid
  Python syntax. This is now fixed.

## [0.20.2] - 25-09-23
### Added
* The `as_apply` to all generated read and read list classes.

### Changed
* Reduced the limit of timeseries to retrieve when querying from 10 000 to 1 000. Recommendation from the FDM team.

## [0.20.1] - 25-09-23
### Fixed
* In the generated SDK, when calling `client.generator.production(... filter for generator ).retrieve_dataframe()`
  with `column_names` set to optional fields with a missing value raises `KeyError`. This is now fixed.

## [0.20.0] - 24-09-23
### Added
* Support for reading `TimesSeries` and `DataPoints` in generated SDKs for fields of type a single `TimeSeries`.
  For example, if you have a `Generator` with a field `production` of type `TimeSeries`, you can now do
  `client.generator.production(... filter for generator ).retrieve_dataframe()` to retrieve the data points for
   the filtered generator. In addition, the following methods are will be available
  * `client.generator.production.list( )` to list all the time series for the filtered generators.
  * `client.generator.production(...).retrieve( )` to retrieve the data points for the filtered generators.
  * `client.generator.production(...).retrieve_array( )` to retrieve the data points as `numpy` array for the filtered generators.
  * `client.generator.production(...).retrieve_dataframe( )` to retrieve the data points as a pandas dataframe for the filtered generators.
  * `client.generator.production(...).retrieve_dataframe_in_tz( )` to retrieve the data points with timezone support for the filtered generators.

* Support for sequences of nodes in generated `APIClass.apply(...)` methods. For example, if you have a `PersonView`
  before you would get `movie_client.person.apply(person: PersonApply, ...)`, while now you will get
  `movie_client.persons.apply(person: PersonApply | Sequence[PersonApply], ...)`.

### Fixed
* Generated SDKs failed to import `datetime` in the generated APIClass for views with `Timestamp` or `Date` fields.
  This is now fixed.
* `to_pascal` and `to_camel` in `cognite.pygen.utils.text` failed for strings with a combination of snake and pascal/camel
  case. This is now fixed.

### Removed
* Unused classes from generated SDK `data_classes/_core.py` `DataPoint`, `NumericDataPoint`, `StringDataPoint`,
  `TimeSeries`.

## [0.19.0] - 19-09-23
### Added
* Added support for generating filtering options in the generated SDK `.list` methods.
* Added helper methods `as_external_ids` and `as_node_ids` to generated data class lists. Example, if I have a view
  `WorkOrder`, and then do a list call `work_orders = apm_client.work_order.list()`. You can now do
  `work_orders.as_external_ids()` or `work_orders.as_node_ids()` to get a list of the external ids or node ids.
* Option for filtering edges by source id in the generated SDK.

## [0.18.3] - 19-09-23
### Fixed
* Addded missing arguments `overwrite` and `skip-formatting` to `pygen` CLI.

## [0.18.2] - 18-09-23
### Fixed
* When generating an SDK for multiple data models, and two of the views in the different models are of different
   versions. Only one of them would be used to retrieve. This is now fixed.
* Views with a property named `version` will raise an error in the generated SDK. This is now allowed, but
  will overwrite the `node.version` parameter. A warning is issued to the used upon the generation of the SDK.
* Views named `core` will now raise a `ReservedWordConflict`, instead of silently removing the view.

## [0.18.1] - 18-09-23
### Fixed
* Default naming convention for API class name set to unchanged plurality.

## [0.18.0] - 18-09-23
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

## [0.17.7] - 07-09-23
### Fixed
* Removed import of `INSTANCES_LIST_LIMIT_DEFAULT` from `cognite-sdk` which no longer exists. This is a private
  constant in the `cognite-sdk` which was removed in `v6.20.0`.

## [0.17.6] - 24-08-23
### Fixed
* `cognite.pygen.generate_sdk_notebook` parameter `overwrite` from `False` to `True`.


## [0.17.5] - 24-08-23
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

**Caveat** Not a single line of code from the previous version is used in this version. Thus, no of the old functionality is
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
