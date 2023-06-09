# Non-GraphQL DM API

This is an experimental module which wraps non-GraphQL API for DM.

It provides a simplified set of features for interacting with DM in an intuitive way and without needing to work with
the underlying data structures.

**[DISCLAIMER!]** This project is in a highly experimental no guarantees are made for consistency between versions. The
project may also become deprecated if the experimentation turns out to be a dead end.


## Motivation

Uploading some data into a DM data model was cumbersome when interacting directly with the DM API.


## Concepts

`dm_clients` package takes Python code as the "source of truth". So a normal workflow is like this:

1. Define use case models in Python (in a "schema" module).
2. Use CLI from this package to generate GraphQL schema.
3. Use CLI from this package to upload this schema to DM.
4. Populate and manipulate data items / instances in DM using a `DomainClient` provided in this package.


> Note: This might change in the future: as `pygen` module evolves we should be able to dynamically create Python
> data types from existing GraphQL schemas in DM.


## Future

Uncertain. No guarantees are made about maintenance of this project. Contributions, however, are very welcome!

With the (future) arrival of GraphQL mutations in DM, this package might become obsolete.


## Glossary

 * DM - CDF Data Modeling, data service by Cognite. For our purposes, it is "the API".
 * `dm_clients` - this package
 * Space - DM object, a high-level container for all things in DM. Like a namespace. Subordinate only to CDF project.
 * Data Model - Often used interchangeably with "Schema", a set of Domain Models (a.k.a. schema types) that together
   help model a use case.
 * Domain - knowledge domain, similar to use case (though we can have multiple use cases that use a single Domain,
   like several clients in a very similar business)
 * Domain Model - a `dm_clients` term - often used interchangeably with "schema type" - a single class of objects with
   all its fields, e.g. "pump" or "generator".
 * Item - a `dm_clients` term - an instance of a Domain Model, e.g. generator "G42".
 * Instance - a DM term - a data entity, either a Node or an Edge (see below).
 * Node - a DM term - a data entity, can contain multiple fields.
 * Edge - a DM term - a data entity that connects two nodes (directionally).
 * DomainModelAPI - a `dm_clients` term - a Python class (or class instance) which provides management over a single
   DomainModel, e.g. create new pumps, delete a pump, list all pumps...
 * DomainClient - a `dm_clients` term - a Python class (or class instance) which serves as a namespace for easy access
   to all DomainModelAPIs in a use case.


## Installation

1. `pip install cognite-pygen`
2. Create a `settings.toml` file, see [settings.toml](./settings.toml) for a template.
   * this step is optional, all settings can be specified in code or via env variables
   * `settings.toml` should contain only things which are safe to commit to git
   * also create `.secrets.toml` and put sensitive settings here
      * the structure of both files (`settings.toml` and `.secrets.toml`) is the same, and the values are merged together
      * example `.secrets.toml`:
        ```toml
        [cognite]
        client_secret = "the secret goes here"
        ```
    * `.secrets.toml` is in `.gitignore`, `settings.toml` is not
      * hint: if you don't want to commit any settings, it's ok to put everything in `.secrets.toml`
    * settings (including secrets) are handled by [Dynaconf](https://www.dynaconf.com/)
       * it is simple to override these settings via env variables, e.g. `export DM_CLIENTS__COGNITE__TENANT_ID = ...`
3. Test if it works:
   ```
   $ python
   >>> from cognite.pygen.dm_clients.domain_modeling.domain_client import get_empty_domain_client
   >>> c = get_empty_domain_client()
   >>> c._client.spaces.list()
   [Space(space="...
   ```
4. Install CDF CLI client
   - [setup instructions](https://docs.cognite.com/cdf/cli/)
   - But basically:
     ```
     npm i -g @cognite/cdf-cli
     ```


## Usage

### Create Schema

To create a schema, see [examples/cinematography_domain/schema.py](../../examples/cinematography_domain/schema.py)
example.


Required features of a schema module:
 - Instantiates a `Schema`.
 - Defines at least one Model class.
   - Model classes need to inherit from `DomainModel`.
   - They need to be registered using `@my_schema.register_type`.
     - Exactly one Model class needs to be registered as "root type":
       - `@my_schema.register_type(root_type=True)`.
- After the last Model class definition, call `my_schema.close()`
- When the module is executed directly, output the GraphQL schema to stdout:
  - ```
    if __name__ == "__main__":
        print(my_schema.as_str())
    ```

#### Upload the schema

> Before proceeding, make sure that `settings.toml` and / or `.secrets.toml` is populated with credentials and
> configuration.


##### Step 1: Render Schema

GraphQL schema file is committed to this repo, so this step is not required, but it is here for completeness.

Execute `dm togql`. This will update the schema file (defined in `settings.toml`) according to Python code in
the schema module.


##### Step 2: Upload Schema

Authenticate against CDF by running `dm signin`. The authentication data is cached on the filesystem
locally, so this is needed rarely (every few? days).

Execute `dm upload`. This will upload the schema to CDF / DM.

> Note: Depending on the changes made to the schema, you might be required to update the schema version in config.yaml.
> This happens when the changes are not backwards-compatible, e.g. deleting a field.


### Manipulate Items

See in [examples/cinematography_domain/usage.py](../../examples/cinematography_domain/usage.py) for a simple
usage example.

A few general notes:

 * `DomainClient` dynamically instantiates a `DomainModelAPI` for every model in the schema.
   * These APIs are named after the model name.
     * e.g. if the model class is names `MyModel`, the API will be accessible at `my_domain_client.my_model`.
 * Most methods on `DomainModelAPI` take a list of items.
   * When creating a single item, remember to wrap it in a list!
 * To update an existing item, use `apply()`.
   * But make sure that the `externalId` of the item is populated (otherwise the API will create a new item).
 * Items with missing `externalId` will get one randomly-generated when passed to `apply()`.


### Low-level API

To manipulate nodes and edges directly, access this API via `_client`:

``` python
model1_instances = my_client._client.nodes.list(my_client.model1.view)
```

Note that `_client` is a subclass of `CogniteClient` and can be used to access "classic" data types (Assets,
Timeseries, etc.)

## Pros and Cons

Pros:

 * Simplifies interaction with DM API
 * Uses Pydantic to validate the items before sending then to the API.

Drawbacks and Limitations:

 * Not all features of DM are supported.
   * `dm_clients` is only tested to work within a single space.
     * Across-space relationships are in principle possible, but some work would be needed to implement it.
   * Nodes (and consequently items) work with properties from only one view.
     * No hard barriers to implementing this in the future.
 * Not terribly efficient.
   * Developed mostly as a tool for getting things done, rather than an optimal implementation.
 * Unpolished, possibly with some bugs.
   * External IDs are constructed, not safely considering the char limit.
   * ...


### TODO

 * Add support for retrieving items via graphql endpoint.
 * Expand unit test coverage
 * Test use on Windows, particularly `dm signin` and `dm upload`
 * Lots of TODOs in the code


## Example: Cinematography Domain

Contained in `examples/cinematography_domain`, this is a minimal toy example of the basic features of this package.
`schema.py` shows implementation of a schema (a.k.a. data model), and `client.py` shows how to manipulate items
programmatically once the schema is in place.


## Contributing

Contributions welcome!

See [TODO](#todo) above.

### Project Structure

Important modules:
 * `cognite/dm_clients/cdf`
    * general-purpose DM client (nothing specific to PowerOps in here)
    * inspired by https://github.com/cognitedata/tech-demo-powerops/ :]
 * `cognite/dm_clients/domain_modeling`
    * base classes for use cases, "boilerplate"
 * `cognite/dm_clients/custom_types`
    * support for Cognite-specific GraphQL scalar types (e.g. JSONObject, Timeseries)
 * `examples/cinematography_domain`
    * a toy example use case with minimal code
