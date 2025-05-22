## Development Instructions

`pygen` is currently mainly developed by [@doctrino](https://github.com/doctrino) which also functions as
the `BDFL` of the project.

Contributions are welcome, but please open an issue first to discuss the proposed changes. If you are a Cognite employee,
please join the `#topic-pygen` channel on Slack and post issues there.

### Installation

Get the code!

```bash
git clone https://github.com/cognitedata/pygen.git
cd pygen
```

We use [uv](https://pypi.org/project/uv/) for dependency- and virtual environment management.

When developing `pygen` use Python `3.10`. To specify the python version with `uv` you can use the command
```bash
uv venv --python 3.10
```
See [uv docs](https://docs.astral.sh/uv/pip/environments/#creating-a-virtual-environment) for more information on managing environments.

Install `pygen` with all the extra and dev dependencies:

```bash
uv sync --all-extras --all-groups
```

`pygen` is using [pre-commit](https://pre-commit.com/) to run static code checks.

Install pre-commit hooks to run static code checks on every commit:

```bash
pre-commit install
```

You can also manually trigger the static checks with:

```bash
pre-commit run --all-files
```

### Testing

There are two set categories of tests in `pygen`, the `tests` folder structure reflects this:
```
📦tests
 ┣ 📂dms_data_models - The Domain Model Storage representation of the data models used in the examples.
 ┣ 📂test_integration - Tests that requires CDF.
 ┃ ┣ 📂test_generator - Test that check that pygen generates SDK(s) as expected.
 ┃ ┗ 📂test_sdks - Test that checks the that the generated SDK work as expected.
 ┣ 📂test_unit - Tests that can be run locally without any external connection.
 ┃ ┣ 📂test_generator
 ┃ ┗ 📂test_sdks
 ┗ 📜constants.py - Defines the Example SDKs and which files are manually maintained.
```

Note the distinction between the `generation` and `sdks` tests. The generated SDKs are checked into the repository and
are found in

 * [examples](/examples) - SDKs generated for `pydantic` `v2`
 * [examples-pydantic-v1](/examples-pydantic-v1) - SDKs generated for `pydantic` `v1`


To run all tests, run the following command from the root directory:
```
pytest
```

To run only the unit tests
```
pytest tests/test_unit cognite/
```
**Note** The `cognite/` is required to run the doctests in the `pygen` package.

### Recommended Development Workflow

First make sure you have discussed the changes with the `BDFL`,
this will save you the risk of getting all your hard work rejected later.

#### GitHub Workflow
Create a new branch for your changes, and make sure you are up-to-date with the `main` branch. After first,
commit push to GitHub and create a draft PR:

1. Create a new branch
```bash
git checkout -b my-new-feature
```
2. Create a draft PR on GitHub. This will allow you to get feedback on your changes early,
   as well as communicating that you are working on a feature/bugfix.

#### Coding Workflow

1. Write a test for one of the example SDKs for the feature you are implementing. Or in the case of a bugfix,
   write a test that fails without your changes. Check which of the generated SDK files that are manually maintained
   in [constants.py](/tests/constants.py).
2. Implement the fix/feature in the example SDK.
3. Ensure there is a generation test that checks that the SDK is generated as expected.
4. Update the `pygen` itself to support generating the SDK.


### Generating Example SDKs
When you are developing `pygen` you will likely need to generate the example SDKs. To do this run the following command from the root directory:
```bash
python dev.py generate
```
This command must be run with both Python environments, `pydantic` `v1` and `v2`.

### Developer CLI
Note that `python dev.py` is a CLI with a few commands that can be useful when developing `pygen`.
To see the available commands run:
```bash
python dev.py --help
```

This can (should) be use for bumping the version number:
```bash
python dev.py bump --patch
```
(replace `--patch` with `--minor` or `--major` per [semantic versioning](https://semver.org/))

### Documentation

We use material docs for the documentation. You can build and serve the documentation locally with:

```bash
mkdocs serve
```

The documentation is kept in the `docs` folder, while the `mkdocs.yml` file contains the configuration for the documentation.

### Release version conventions

See https://semver.org/

### Test Data and Model

The test models are managed by `Cognite Toolkit` with the modules located in the `tests/modules` folder.

To deploy the test models to CDF, run the following command:
```bash
cdf build
```

and
```bash 
cdf deploy
```

To generate mock data for the test model `Omni`, run the following command:
```bash
python dev.py mock
```
You can also deploy to CDF by adding the `--deploy` flag:
```bash
python dev.py mock --deploy
```

Finally, downloading read version of models and data can be done with:
```bash
python dev.py download
```