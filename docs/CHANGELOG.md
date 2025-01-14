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

## [0.99.61] - 24-11-14
### Fixed
- Add ability to filter on node external ID and space even when no other filterable properties are present. Version 0.99.49 only partially fixed this.

## [0.99.60] - 25-01-01
### Improved
- Error message on validation error in any retrieve type method (e.g. `.retrieve`, `.list`, `.select`). It now
  includes an overview of the total instances that failed validation.

### Added
- Support for turning off validation for the `.search`/edge `.list` methods using the configuration:
  `my_sdk.config import global_config; global_config.validate_retrieve = False`.

## [0.99.59] - 24-12-30
### Added
- Support for filtering on direct relations in the `.select()` method.
- Parameter `retrieve_connections` in the `.retrieve()` method to retrieve all connections for a node.
- Support for turning off the validation in the `.retrieve()`/`.list(...)`/`.select()` methods using the 
  `my_sdk.config import global_config; global_config.validate_retrieve = False`.
- Support for `getLatestDataPoint` in the `graphql_query` method.

### Improved
- When calling the `.upsert(...)` the `write_none` argument is now deprecated. Instead, `pygen` detects which
  properties that are explicitly set to `None` and only sends those properties to the CDF. This way you can
  now set properties to `None` and they will be updated in the CDF.

### Removed
- The `.query()` method (was in `alpha`) has been removed. Instead, use the `.select()` method.

### Changed
- The `.select()` method is no longer in `alpha` and is now the recommended way to query the CDF.

### Fixed
- Calling `.upsert` with an edge with properties in inwards direction, now creates the edge in the correct direction.

## [0.99.58] - 24-12-16
### Fixed
- Updated the timeseries API to support TimeZone.

## [0.99.57] - 24-12-12
### Fixed
- Added `instance-space` to the `pygen` CLI.

## [0.99.56] - 24-12-01
### Fixed
- Views used for edges that implements other views now correctly include all properties in the generated SDK.
- The `.aggregate("count")` method now correctly returns a single value instead of a list of values.
- If a view as a property `classes` the generated SDK no longer raises a `SyntaxError`.
- Using `.earliest()/.latest()` no longer returns multiple items when used as part nested query.

### Improved
- Better error message for typos in the `.select()` method.

## [0.99.55] - 24-11-29

### Added
- Support for referenced timeseries in `.select()` method.

## [0.99.54] - 24-11-26
### Fixed
- Calling `.retrieve(...)` on a view with one-to-many connections no longer raises a `NameError`.
- Calling `.upsert(...)` on a view with a dependency on a CogniteAsset or any extension of it
  no longer raises an `AttributeError`.

## [0.99.53] - 24-11-25
### Added
- The `.select()` method now supports sorting on properties. For example,
  `pygen.wind_turbine.select().name.sort_ascending().list_wind_turbine().
- In the `.select()` method, Timestamp and date properties supports `.latest()` and `.earliest()` as a shorthand
  for sorting and setting the limit to 1. For example,
  `pygen.wind_turbine.select().datasheets.uploaded_time.latest().list_data_sheet()` will return the latest uploaded
  datasheet for all wind turbines.

## [0.99.52] - 24-11-24
### Fixed
- Calling `.upsert` with `CogniteAsset` or `CogniteFile` or any extension of these no longer raises an
  `AttributeError`.

## [0.99.51] - 24-11-22

### Fixed

- Generating an SDK with a connection property with a source pointing to a non-existing target
  no longer raiser `AttributeError: 'NoneType' object has no attribute 'view_id'`. Instead, a warning is issued
  and the connection is skipped.
- Generating an SDK with a listable direct relation property without source no longer raises 
  `SyntaxError` when importing the generated SDK.

## [0.99.50] - 24-11-17
### Added
- Any views that extends the `CogniteTimeSeries` now has the property `data` you can use to retrieve datapoints.
  For example, `pygen.rotor.select().rotor_speed_controller.data.retrieve_dataframe(...)` will retrieve the datapoints
  for the `rotor_speed_controller` timeseries.
- Any views that extends the `CogniteFile` now has the property `content` you can use to download the file.
  For example, `pygen.wind_turbine.select().datasheets.content.download("my_directory")` will download the files
  for the `data_sheet` files for all wind turbines.

### Fixed
- The `.query()` method has been renamed to `.select()`. The `.query()` method is still available, but will
  be removed shortly.
- Calling `.select()` over an edge with properties without filtering no longer raises a `ValueError`.
- When using the `CogniteCore` model, either directly or an extension of it, the generated SDK now
  respects the read-only properties in `CogniteAsset` and `CogniteFile`.
- When calling `.select()` the `limit` parameter is now respected and the correct number of nodes are returned.
  There were some edge cases when the pagination stopped before the limit was reached. This is now fixed.

## [0.99.49] - 24-11-14
### Fixed
- Add ability to filter on node external ID and space even when no other filterable properties are present.

## [0.99.48] - 24-11-13
### Changed
- [Experimental feature] Generic query split into three methods `.list()`, `.search()`, and `.aggregate(...)`.

## [0.99.47] - 24-11-12
### Fixed
- Raised lower bound of dependency `inflect` to `6.2` when support for `pydantic` `v2` was added.
- Reverting change from `0.99.34`. For example, for `CogniteTimeSeries` the property `type` will
  now be named `type_` and not `time_series_type`. This is to avoid conflicts if the view has a property
  named `time_series_type`. This typically occurs when someone extends `CogniteTimeSeries` and introduces
  custom `types`.

## [0.99.46] - 24-11-11
### Fixed
- Views with external ids on the formate `UPPERCASE_LETTERS_NUMBERS` no longer raise a NameConflict error when
  generating the SDK. This was caused by a bug in `to_pascal` method used to create the data class names.

## [0.99.45] - 24-11-10
### Improved
- When calling `generate_sdk` or `generate_sdk_notebook` you now get an improved formatting of warnings. In addition,
  irrelevant warnings are no longer shown.

### Added
- In the generated list classes, all connection properties are now available as properties. This is to make it easier
  to access the connection properties without having to go through the `data_record` property.
- Support for traversing over reverse direct relation of list in the `.query()` method.

### Changed
- [Experimental - Breaking] Reverted breaking change from `0.99.40`; The `.query` property is now a method `query`.
  Instead of `client.asset.query` you now do `client.asset.query()`. This is to make type hints better when working
  ina Notebook environment.

### Fixed
- The `.list_<type>` of the `.query()` method no longer returns empty lists when the start node is the same as the
  end node.

## [0.99.44] - 24-11-08
### Improved
- In the underlying `query` operation, used for `.list(...)` as well as query methods, `pygen` now
  does chunking queries that leads to use of `In` filters. This is to avoid the `CogniteAPIError - Internal Server 500` 

## [0.99.43] - 24-11-05
### Changed
- The following two changes is to give more granular control over were `pygen` writes the generated SDK. This
  is to make it easier to integrate `pygen` with another code base:
  - The `output_dir` parameter in `generate_sdk`, if set, is now independent of `top_level_package`. This means
    that if you set `output_dir` to a specific directory, the generated SDK will be written to that directory
    and not under `top_level_package`.
  - All imports generated by `pygen` are now absolute imports. This is to make it easier to integrate the generated
    SDK with other code bases. As well as comply with certain tools which require absolute imports.

## [0.99.42] - 24-11-03
### Added
- Experimental feature generic query.

## [0.99.41] - 24-10-30
### Added
- The `cognite.pygen.utils.MockGenerator` now generate properties for edges that have properties.

### Fixed
- When retrieving reverse direct relations in the generated SDK, you no longer get a `ValueError` with
  `Circular reference...`.
- The `cognite.pygen.utils.MockGenerator` now checks readonly properties on container level and not on view level.
  This is to ensure that readonly properties are not set on nodes that are not writable.

## [0.99.40] - 24-10-26
### Added
- Support for using `NodeId` and `tuple[str, str]` as well as sequences of these in the `.retrieve`
  method of the generated SDK.
- In the `.list` method with `retrieve_connections="full"`, reverse direct relations pointing to list direct relations
  are now supported.

### Fixed
- The results of `client.node_type.query.....list_full()` is now filtered no the client side to ensure that only
  nodes that are connected to the starting node are returned.

### Changed
- [Experimental - Breaking] The `.query()` method is now a property `query`. Instead of `client.asset.query()`
  you now do `client.asset.query`. This is to make it more intuitive to use the query method.

## [0.99.39] - 24-10-19
### Fixed
- In `.list` setting `retrieve_connections="full"` no longer raises 
  `CogniteAPIError: Cannot traverse lists of direct relations inwards.`. However, reverse direct relations pointing
  to list direct relations are not supported.
- When using the config `faker` in the `MockGenerator`, setting the `seed` now produces the same data for each view.

## [0.99.38] - 24-10-16
### Added
- When calling `.list(...)` with `retrieve_connections="full"` `pygen` will no longer raise
  the `CogniteAPIError: Graph query timed out.` error, but instead dynamically adjust the
  `limit` to ensure that all nodes are retrieved.
- If the `.list(...)` with `retrieve_connections="full"` `pygen` will now automatically detect large queries
  and print the progress with estimated time left.

## [0.99.37] - 24-10-15
### Added
- Support for deleting by `NodeId` in generated SDK.
- You can now use `DirectRelationReference` and `tuple[str, str]` as node identifiers in the Write classes
  of the generated SDK.
- In the `.list` method, `NodeId` and `DirectRelationReference` can now be used when filtering on direct relations
  (in addition to `tuple[str, str]` and `str`).

## [0.99.36] - 24-10-04
### Changed
- [BREAKING] In the experimental query method, the `.execute(...)` has been renamed to `.list_full(...)`, and 
  `.list(...)` has been renamed to `.list_<name of type>(...)`. This is to make it more intuitive to understand
  what the methods do.

### Removed
- `pydantic` `v1` is no longer supported. This is because `pyodide` now supports `pydantic` `2.7`.

## [0.99.35] - 24-09-30
### Added
- Support for `Enum` in `MockGenerator`.

### Fixed
- Generating an SDK from a Data Model with a view with only primitive properties and reverse direct relations no
  longer raises a `ValueError`.
- Generating an SDK from a Data Model with a view for edges that is not writable but used in an implements, no longer
  raises an `ImportError`.

### Removed
- Support for Python `3.9` is dropped.

## [0.99.34] - 24-09-06
### Changed
- If a view has a property named `type` or `version` this is now prefixed with the view external id instead
  of being suffixed with underscore. For example, `CogniteAsset.type` is now `CogniteAsset.asset_type`. This is to
  avoid confusion with the `node_type` and `edge_type` fields in the generated data classes.

## [0.99.33] - 24-08-21
### Added
- Support for `files` and `sequences` in the generated SDK. This includes the ability to create and retrieve
  `files` and `sequences` in the generated SDK.

### Changed
- In the `MockGenerator`, setting the `seed` makes the generator produce the same data for every view. For example, if
  you use the same `seed` for a data model and add a new view, the generator will create the same data for all
  existing views.

### Fixed
- In the generated SDK, fields of `TimeSeries` as now set to `TimeSeriesWrite` in the write Data Class of the generated
  SDK.
- If an input view had a reverse direct relation, that points to a non-existing target, the generated SDK would raise
  a `ValueError`. `pygen` now gracefully handles this case by raising a warning instead.
- If a view only had a dependency on itself, `pygen` would generate invalid code. This is now fixed.

## [0.99.32] - 24-08-17
### Added
- Support for files and sequences in the generated SDK.
### Changed
- When generating an SDK if the `default_instance_space` parameter, `pygen` will no longer use the space of the
  data model as the default space for nodes and edges. Instead, the generated SDK will be without a default space,
  and all nodes and edges must be specified with space when creating or querying them. The motivation for this change
  is that it is an anti-pattern to have data (node and edges) in the same space as the schema (data model, views,
  containers). This change enforces the user to specify an instance space either when creating the SDK or
  when creating or querying nodes and edges.
- In the generated `GraphQL` class methods `as_write` and `as_read` no longer defaults to DEFAULT_INSTANCE_SPACE.
  This is to ensure that the user must specify the space when creating or querying nodes and edges, and not introdcue
  a bug where the space is not set correctly.

### Fixed
- In the generated SDK, fields of `TimeSeries` as now set to `TimeSeriesWrite` in the write format of the generated
  SDK.

## [0.99.31] - 24-08-16
### Added
- Support for edges with properties of type `single_edge_connection`.
- Support for retrieving `GraphQL` queries with datapoints`
- Support for searching on `external_id` in the generated SDK.
- In write data class, allow node type to be `tuple` and `NodeId` as well as
  `DirectRelationReference`.
- You can now retrieve interfaces as child classes in the generated `.retrieve` method.

### Fixed
- The `external_id_factory` will no longer be called when creating a `DomainModelWrite` object
  if the `external_id` is set. This is to avoid the `external_id_factory` to overwrite the `external_id`
  when creating a new object.
- Edges of type `single_edge_connection` with direction `inwards` no longer creates a list
  field.
- Edges with properties now correctly generates the end_node field for direction `inwards`.
- Edges with properties now have the correct destination node when multiple edge properties
  have the same edge type.
- If a view property starts with a digit or contains a hyphen, the generated data class will
  now have the property name prefixed with `no_` to make it a valid Python variable name instead
  of raising an exception.

## [0.99.30] - 24-08-13
### Added
- New approach to querying. This is accessed through the `.query()` method
  on the generated SDK. This is a simplification to make it easier to create
  complex queries. The new approach is currently experimental and may  change
  based on feedback. What the new approach supports:
  - Traversing any connection, including edges, direct relations, and reverse direct relations.
  - Filtering on properties that are not `JSON` or list.
  - Return either the entire query, `.execute()`, or only the last node, `.list()`.

## [0.99.29] - 24-08-10
### Added
- Support for **reverse direct relations**. This includes
  - All read data classes now include reverse direct relations fields.
  - In the `.list` method a `retrieve_connections` parameter is added that can be set to
    `full` to retrieve all reverse direct relations, direct relations, and edges with
    destination node for each item in the returned list.

### Removed
- In the `.list` method, the `retrieve_edges` parameter is removed. This has been replaced
  by the `retrieve_connections` parameter. Setting the `retrieve_connections="identifier'`
  will have the same behavior as `retrieve_edges=True` and `retrieve_connections="skip"`
  the same as `retrieve_edges=False`.

### Fixed
- When querying, in the unpacking, the generated SDK no longer assumes that all
  edges pointing out/in of a node have unique edge type.

## [0.99.28] - 24-07-24
### Improved
- External ID factories: Updated domain_name to remove 'write' suffix only if domain_cls inherits from DomainModelWrite

## [0.99.27] - 24-07-18
### Added
- Support for advanced sort in generated `.list` method.
- Support for sort and advanced sort in generated `.search` method.

### Improved
- Pygen now longer generates code that is deprecated in `pydantic` or `cognite-sdk`.
- The generated SDK is now compliant with `mypy` with `pydantdic` plugin.

### Fixed
- The read data class for a view with required listable properties would generate as optional. This is now fixed.
- Using `client.upsert` with an optional listable `date` or `datetime` property set to `None` would raise
  an `AttributeError`. This is now fixed.
- Updated to match internal change for `cognite-sdk=7.54.3`.

## [0.99.26] - 24-06-24
### Added
- Support for `sort` in generated `.list` method.

## [0.99.25] - 13-06-24
### Added
- `ExternalIdFactory` class as a template for custom functions to define for `DataModelWrite.external_id_factory`
  - class methods for out of the box external id generators `create_external_id_factory`, `domain_name_factory`,
    `uuid_factory`, `sha256_factory`, `incremental_factory`
- Helper functions for unique external_id generation
  - `shorten_string` returns a shortened string
  - `domain_name` returns a clean string from the domain class name
  - `uuid` returns a uuid
  - `sha256` returns a sha256 has of the data
  - `incremental_id` returns an incremental integer based on domain class

### Deprecated
- The following functions in `external_id_factories` have been deprecated and will be removed in v1
  - `create_uuid_factory`
  - `uuid_factory`
  - `create_sha256_factory`
  - `sha256_factory`
  - `create_incremental_factory`

## [0.99.24] - 27-05-24
### Added
- Support for generating SDKs for views with list of direct relations properties.

## [0.99.23] - 27-05-24
### Added
- Support for list of direct relations and `SingleEdgeConnection` in the `cognite.pygen.utils.MockGenerator`.
- Support for recursive delete in the generated SDK for nodes and edges in the `.delete` method.

### Fixed
- The `MockGenerator` could produce different results for the same input and seed due to the internal creation order.
  This is now fixed by ensuring the creation order is always the same for the same input.
- Properties of type `SingleEdgeConnection` with direction `inwards` would raise `ValueError` if you called
  `.as_write()` on the generated read data class. This is now fixed.
- Multiple bugs for different types of connections in the `.as_read()` and `.as_write()` methods of the generated
  GraphQL data classes. This is now fixed.
- Set minimum version of `cognite-sdk` to `7.43.4` as this contains a bugfix for data modeling limits.

## [0.99.22] - 06-05-24
### Fixed
- When calling `.as_read()` or `.as_write()` on a `GraphQL` object in the generated SDK, with a file
  or sequence, the generated SDK would raise a `pydantic_core._pydantic_core.ValidationError`. This is now fixed.

## [0.99.21] - 02-05-24
### Fixed
- When calling the function `generate_sdk` or `generate_sdk_notebook` with a data model containing a view used for
  edge properties, there was an edge case that caused
  `IndentationError: expected an indented block after 'if' statement on line 22`. This is now fixed.

## [0.99.20] - 18-04-24
### Fixed
- Upgrading to `cognite-sdk>=7.37` caused the generated SDK to fail with:
  `ImportError: cannot import name 'ListablePropertyType' from 'cognite.client.data_classes.data_modeling.data_types'`.
  This is now fixed.

## [0.99.19] - 12-04-24
### Added
- All data classe generated by `pygen` now have a  `.dump` method that wraps `.model_dump` and `.dict` methods from `pydantic`.
  This is to have a consistent way of serializing the data classes to a dictionary independent of the `pydantic` version.
- PageInfo object to the `GraphQlList` object which is returned when calling `.graphql_query` in the generated SDK.
  This is to allow for pagination when querying the CDF with GraphQL.

### Fixed
- When retrieving `.graphql` with a CDF external resource type (`Timeseries`, `File`, or `Sequence`), the generated SDK
  would raise a `ValidationError`. This is now fixed.
- When generating a SDK for a Data Model with a View without any properties, the generated SDK would raise a
  `ValueError: APIGenerator has not been initialized` error. This is now fixed.

### Changed
- When `pygen` write a SDK to disk, it now will always use `\n` and `utf-8` for newline and encoding, respectively.
  This is to ensure that the generated SDK is consistent across platforms.
- When writing a SDK to disk, `pygen` will now remove `.py` files that are not part of the generated SDK when
  `overwrite=True` is set. This is to remove any old files generated from previous versions of the data
  model.

## [0.99.18] - 12-04-24
### Changed
* Typing extensions, `typing-extensions` is no longer a direct dependency of `pygen`. This is to avoid
  conflicts when installing `pygen` in a `Pyodide` environment.

## [0.99.17] - 03-04-24
### Added
* Support for generating a wheel file for the generated SDK. This is currently only
  available in code through `cognite.pygen.build_wheel` function.

### Fixed
* If the data model contained a view with name `Field`, the generated SDK would raise a
  `NameError: name 'Field' is not defined` when importing the client. This is now fixed.

## [0.99.16] - 23-03-24
### Fixed
* When setting `external_id` in a generated edge data class, it was ignored and used the
  `external_id_hook_factory` instead. This is now fixed.

## [0.99.15] - 23-03-23
### Added
* In the python query method, you can now filter on the end node of an edge connection. For example,
  `windmill_client.windmill(limit=2).blade(is_damaged=True).query()`. The `is_damaged=True` is a filter on the
  end node of the edge connection `blade`.
  **BREAKING CHANGE**: Before you could filter on edge properties, to distinguish between filtering on the edge
  and end node properties, the edge properties has been suffixed with `_edge`.

### Fixed
* In the Python Query method, if you included direct relations, e.g.,
  `windmill_client.windmill(limit=2).query(retrieve_rotor=True)`, the `limit` was not respected and instead returned all nodes.
  This is now fixed.

## [0.99.14] - 20-03-24
### Fixed
* The new `GraphQL` classes introduced in `0.99.12` fails for views which implements another view. This is now fixed.

## [0.99.13] - 13-03-24
### Changed
* Ignore extra arguments for Write classes instead of raising `ValidationError`. This is to allow for
  future changes in the data model.

## [0.99.12] - 12-03-24
### Added
* Support for `GraphQL` queries in the generated SDK. This is available through the `query` method in the generated
  SDK. For example, if you have a data model `Windmill` you can do `windmill_client.graphql_query(...)`.
* Support for view property type connection with `connectionType=single_edge_connection`.
* Added parameter `allow_version_increase` to `client.upsert(...)` call to allow set `existing_version` to None
  in all nodes when writing to CDF.

### Fixed
* Creation of the SDK no longer fails if there is a `connectionType=single_reverse_direct_relation`
  or `connectionType=multi_reverse_direct_relation` in any view property the data model.
* The creation of the SDK no longer fails with `NotImplementedError` if a direct relation
  is missing a source.

## [0.99.11] - 23-02-24
### Added
* Allowed `externalId` as alias for `external_id` in the generated data classes.

## [0.99.10] - 17-02-24
### Fixed
* When having Edges with properties, it was possible to hit a `RecursionError: maximum recursion depth exceeded` due
  to poor handling of recursion when using the `.upsert` method. This is now fixed.
* In the `MockGenerator` the `.deploy` command would fail if there are duplicates files, timeseries or sequences.
  This is now fixed.
* In the `MockGenerator` when `skip_interfaces` is set to `True`, it would skip generating connections
  (edges/direct relations) to the interfaces. This is now fixed by generating the connections to the children of the
  interfaces instead.

## [0.99.9] - 11-02-24
### Fixed
* The `data_class.__init__.py` file would reference write classes that does not exist in the generated SDK. This
  occurs if a view is non-writable (and is not an interface). This is now fixed.

## [0.99.8] - 11-02-24
### Fixed
* Using the `Apply` classes could raise `pydantic.errors.PydanticUserError: `<YOUR MODEL>` is not fully defined;...`
  This is now fixed.
### Added
* Helper method `.as_id` to `DomainRelation` base class.

## [0.99.7] - 10-02-24
### Changed
* The method `.apply` and write data classes, for example, `WindmillApply` has been renamed to `.upsert`
  and `WindmillWrite` to better reflect the operation. The old method and classes are still available, but
  will be removed in version 1.
* The name of the data classes generated by `pygen` is now always based on the `external_id` of the view. It used
  to use the `name` of the view if it was set, however, that can lead to name collisions as `name` for views
  does not have to be unique. This is a potential breaking change, but since `pygen` is still in beta, it is
  allowed.

### Fixed
* In the `.list` and `.retrieve` methods if the underlying view has two edges with the same edge type, the
  returning nodes would se the external id of the edges incorrectly. This is now fixed.
* When having more then `1000` instances the query method, for example, `windmill().query()` would fail to
  retrieve all nodes that directly relates to the nodes. This is now fixed by increating the query
  limit to `10000`.

## [0.99.6] - 08-02-24
### Fixed
* The `MockGenerator` would fail with a `KeyError` if a data models passed that contains a view that references
  another view that is not in the data model. This is now fixed and a warning is issued instead.

## [0.99.5] - 05-02-24
### Fixed
* The `.apply` method failed with `AttributeError: 'list' object has no attribute 'nodes'` when using the generated SDK
  with a list of nodes. This is now fixed.

## [0.99.4] - 04-02-24
### Added
* Property `data_records` to generated list classes.

### Changed
* The `to_pandas` method of list classes now returns columns in order `space`, `external_id`, `[other properties]`,
  `node_type`, `data_record`.

### Fixed
* The `MockGenerator` now handles non-writable views with properties. This was an issue when generating nodes for
  data models that contain non-writable views. This is now fixed, the nodes from the non-writable views are
  created without properties.
* In the `query` call it was possible to get negative `limit` which would raise a `raise CogniteAPIError`.
  This is now fixed.
* In the `query` call when using `limit=-1` the default API limit was used in the generated SDK. This caused the
  `query` to return only the first 100 results. This is now fixed so that `limit=-1` returns all results,
  also for nested calls.
* Filtering an integer or float value in the generate SDK on `0` or `0.0` would return all values. This is now fixed.
* Filtering on an empty string in the generated SDK would return all values. This is now fixed.
* With `cognite-sdk>=7.16.0` the generated SDK method `.search` failed with `AttributeError: 'int' has no attribute '_involved_filter_types`.
  This is now fixed.
* In the `timeseries` query method, if a timeseries type property is nullable and the value is missing, the generated
  SDK would raise `KeyError` when trying to retrieve timeseries or datapoints. This is now fixed.

### Changed
* The generated SDK now complies with `black` `0.24.0` formatting.

## [0.99.3] - 02-02-24
### Added
* The `MockGenerator` now supports generating nodes for views with filters on `node.type` which are nested. For example,
  if a view has a the filter `And(Equals(["node", "type"], {"space": "sp_types", "externalId": "myType"},
  HasData(...))`, then the `MockGenerator` will generate nodes with the `node.type` set to
  `{"space": "sp_types", "externalId": "myType"}`.

## [0.99.2] - 01-02-24
### Fixed
* The `MockGenerator` raised a `NotImplementedError` when a direct relation was missing source. Now, it
  will create a warning instead and skip the relation.

## [0.99.1] - 31-01-24
### Added
* Option for skipping interfaces in `cognite.pygen.utils.MockGenerator`.

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
