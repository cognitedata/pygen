Cognite GraphQL Python Generator
==========================
[![build](https://github.com/cognitedata/cognite-gql-pygen/actions/workflows/release.yaml/badge.svg)](https://github.com/cognitedata/cognite-gql-pygen/actions/workflows/release.yaml)
[![GitHub](https://img.shields.io/github/license/cognitedata/cognite-gql-pygen)](https://github.com/cognitedata/cognite-gql-pygen/blob/master/LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

This is the Cognite GraphQL Python Generator, `gqlpygen`, which is a CLI for automatically generating Python SDK code
from `.graphql` to interact with Cognite Data Fusion's (CDF) Flexible Data Models (FDM). It also supports other cases,
such as generating `.graphql` from `pydantic` classes.

**[DISCLAIMER!]** This project is in a highly experimental no guarantees are made for consistency between versions. The
project may also become deprecated if the experimentation turns out to be a dead end.


### FDM Non-GraphQl API

This package also contains `cognite.fdm` which is a simplified wrapper for non-GraphQL endpoints in FDM API v3.

See [fdm/README.md](./fdm/README.md) for more details.

## Installation

To install this package:
```bash
$ pip install cognite-gql-pygen
```
## Usage

Currently (v0.1.0), there is no functionality, just some demo functions:

Type the following command for help.
```bash
cogpygen --help
```


## Changelog
Wondering about upcoming or previous changes to the SDK? Take a look at the [CHANGELOG](https://github.com/cognitedata/cognite-gql-pygen/blob/master/CHANGELOG.md).

## Contributing
Want to contribute? Check out [CONTRIBUTING](https://github.com/cognitedata/cognite-gqlpygen/blob/master/CONTRIBUTING.md).
