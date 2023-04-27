Cognite GraphQL Python Generator
==========================
[![build](https://github.com/cognitedata/cognite-gql-pygen/actions/workflows/release.yaml/badge.svg)](https://github.com/cognitedata/cognite-gql-pygen/actions/workflows/release.yaml)
[![GitHub](https://img.shields.io/github/license/cognitedata/cognite-gql-pygen)](https://github.com/cognitedata/cognite-gql-pygen/blob/master/LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

This is the Cognite GraphQL Python Generator, `gqlpygen`. The purpose of this package is to help developers to
work with Cognite Data Fusion's (CDF) Data Models (DM) in Python.

**[DISCLAIMER!]** This project is in a highly experimental no guarantees are made for consistency between versions. The
project may also become deprecated if the experimentation turns out to be a dead end.


The core functionality is to provide a Python client that match a data model. This enables the developer the following
benefits

* Client side validation of the data before writing it to CDF.
* Autocompletion matching the data model in the integrated developer environment (IDE). This is important as it enables:
  * Discoverability of data model through Python.
  * Reduced typing errors in development.
* Keeping the language domain specific for the developer. Instead of working with generic concepts such as instances,
  nodes, edges, the developer can work with the concepts in the data model.


## Installation

To install this package:
```bash
$ pip install cognite-gql-pygen
```

## Usage

The goal of the package is to have representations of all the types in a given data model with API calls to *.list()*,
*.apply()*, *.delete()*, and *.retrieve()* individuals for each type.

![image](https://user-images.githubusercontent.com/60234212/234041823-f72a27e3-6450-4f05-99dc-50e87f762d0f.png)



You can specify the data models either as a `.graphql` schema or a `pydantic` classes in a `.py` file. Then, you can
use the CLI to automatically generate the other representation as well as the `client.py` file which creates
the API and the convenience method get_[client_name]_client().

To generate from a `.graphql` schema you use the following command.

```bash
dm topython 'PATH_TO_SCHEMA'
```

This will create a `schema.py` and a `client.py` file in the directory you are running the command.

To generate from `schema.py` use the following command

```bash
dm togql 'PATH_TO_FILE'
```

This will load the python module and create a `schema.graphql` file in the directory you are running the command.

`PATH_TO_FILE` can be either a path to a `.py` file or a Python dot-notation to a package
(e.g. `my_project.schema_module` make sure that the package in which case the module must be in Python path).

Note the `schema.py` file must follow a specific structure, see [examples/cinematography_domain](https://github.com/cognitedata/cognite-gql-pygen/blob/main/examples/cinematography_domain/schema.py) for an example.
The overall structure is as follows:

1. Instantiate a new schema with the line, `myschema: Schema[DomainModel] = Schema()`
2. Register all you Types with `@myschema.register_type`
3. Close the schema with `myschema.close()`

To get a concrete example is available in [examples/cinematography_domain](https://github.com/cognitedata/cognite-gql-pygen/blob/main/examples/cinematography_domain),
it consists of four files.

* `schema.graphql` The schema defined in GraphQL language.
* `schema.py` The schema defined in `pydantic` classes.
* `client.py` Which sets up the client for the data model.
* `usage.py` Demonstrates the usage of the client.


### Settings File

`dm togql` and `dm topython` take their defaults form `settings.toml` if present. See
[settings.toml](./cognite/dm_clients/settings.toml) for an example, section `[local]` is relevant for `togql` and
`topython` commands.


### DM Non-GraphQl API

The API developed is based on the non-GraphQL endpoints in Data Model API v3. There is a simplified wrapper which is available
in `cognite.dm_clients`.

See [dm_clients/README.md](cognite/dm_clients/README.md) for more details.__


## Changelog
Wondering about upcoming or previous changes to the SDK? Take a look at the [CHANGELOG](https://github.com/cognitedata/cognite-gql-pygen/blob/master/CHANGELOG.md).

## Contributing
Want to contribute? Check out [CONTRIBUTING](https://github.com/cognitedata/cognite-gqlpygen/blob/master/CONTRIBUTING.md).
