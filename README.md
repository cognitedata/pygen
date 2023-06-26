Cognite Python SDK Generator
==========================
[![build](https://github.com/cognitedata/pygen/actions/workflows/release.yaml/badge.svg)](https://github.com/cognitedata/pygen/actions/workflows/release.yaml)
[![GitHub](https://img.shields.io/github/license/cognitedata/pygen)](https://github.com/cognitedata/pygen/blob/master/LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

This is the Cognite Python SDK Generator, `pygen`. The purpose of this package is to help developers to
work with Cognite Data Fusion's (CDF) Data Models (DM) in Python.

**[DISCLAIMER!]** This project is in a highly experimental state, and no guarantees are made for consistency between
versions. The project may also become deprecated if the experimentation turns out to be a dead end.


The core functionality is to provide a Python client that matches a data model. This enables the developer for the following
benefits

* Client side validation of the data before writing it to CDF.
* Autocompletion is matching the data model in the integrated developer environment (IDE). This is important as it enables:
  * Discoverability of a data model through Python.
  * Reduced typing errors in development.
* Keeping the language domain specific for the developer. Instead of working with generic concepts such as instances,
  nodes, edges, the developer can work with the concepts in the data model.


## Installation

### Without any optional dependencies

To install this package without CLI support:
```bash
pip install cognite-pygen
```

### With optional dependencies

* `cli` This includes CLI support such that you can run the package from the command line.

```bash
pip install cognite-pygen[cli]
```

## Usage

The goal of the package is to have representations of all the types in a given data model with API calls to *.list()*,
*.apply()*, *.delete()*, and *.retrieve()* individuals for each type.

![image](https://github.com/cognitedata/pygen/assets/60234212/b9942595-424c-4c5e-8a9c-37a43e0a5a7c)

![image](https://github.com/cognitedata/pygen/assets/60234212/70a5f6b0-cec0-4178-93e1-4f9902658638)


## Creating a Python SDK from a Data Model

Given a Data Model with external id `Movie` in the space `movies` in CDF, the following command will generate a Python SDK
```bash
pygen --space movies --external-id Movie --tenant-id <tenant-id> --client-id <client-id> --client-secret <client-secret> --cdf-cluster <cdf-cluster> --cdf-procect <cdf-project>
```

## Changelog
Wondering about previous changes to the SDK? Take a look at the [CHANGELOG](https://github.com/cognitedata/pygen/blob/master/CHANGELOG.md).

## Contributing
Want to contribute? Check out [CONTRIBUTING](https://github.com/cognitedata/pygen/blob/master/CONTRIBUTING.md).
