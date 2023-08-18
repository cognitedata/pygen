## Installation

**Prerequisites**: Installed Python 3.9 or newer, see [python.org](https://www.python.org/downloads/)

1. Create a virtual environment.
2. Activate your virtual environment.
3. Install `cognite-pygen` with the all option.

=== "Windows"

    ```
    python -m venv venv
    ```
    ```
    venv\Scripts\activate.bat
    ```
    ```
    pip install cognite-pygen[all]
    ```

=== "Mac/Linux"

    ``` bash
    python -m venv venv
    ```
    ``` bash
    source venv/bin/activate
    ```
    ``` bash
    pip install "cognite-pygen[all]"
    ```

### Installation Options

=== "No extras"

    ```
    pip install cognite-pygen
    ```

    This only installs the core dependencies for `cognite-pygen` and is useful if you only want to generate the SDK
    from a Python script, for example, in a notebook.

=== "cli"

    ```
    pip install cognite-pygen[cli]
    ```

    This installs the core dependencies for `cognite-pygen` and the dependencies for the CLI. This is useful if you
    want to generate the SDK from the command line.

=== "format"

    ```
    pip install cognite-pygen[format]
    ```

    This installs the core dependencies for `cognite-pygen` and the dependencies for formatting the generated SDK.
    This is useful if you want to format the generated SDK code with black.

=== "all"

    ```
    pip install cognite-pygen[all]
    ```

    This installs the core dependencies for `cognite-pygen`, as well as the CLI and code formatting dependencies.

## Create Python SDK

Given a Data Model with external id `Movie` in the space `movies` in CDF, the following command will generate a Python SDK

```bash
pygen generate --space movies \
    --external-id Movie \
    --version 1 \
    --tenant-id <tenant-id> \
    --client-id <client-id> \
    --client-secret <client-secret> \
    --cdf-cluster <cdf-cluster> \
    --cdf-procect <cdf-project>
```

In addition, the following options are available and recommended to be used:

* `--output-dir` This is the directory where the generated SDK will be placed.
* `--top-level-package` The top level package for where to place the SDK, for example `movie_sdk.client`.
* `--client-name` Client name for the generated client expected to be given in `PascalCase`, for example `MovieClient`.

For more information about the available options, see `pygen --help`.

### CLI
You can use the `pygen` command to generate a Python client for a Cognite Data Fusion Data Model.

To see the available options, run:

```bash
pygen --help
```

When running the command `pygen`, the program looks for a `pyproject` toml in the current working directory. If
the `pyproject.toml` file is found and has a `[tool.pygen]` section, the values from this section will be used as
default values for the command line arguments. The exception is the `--client-secret` argument which always
have to be specified on the command line.

Below is an example of the values that can be set in the `pyproject.toml` file.

```toml
[tool.pygen]
space = "IntegrationTestsImmutable"
external_id = "Movie"
version = "2"
tenant_id = "<cdf-project>"
client_id = "<client-id>"
cdf_cluster = "<cdf-cluster>"
cdf_project = "<cdf-project>"
top_level_package = "movie_domain.client"
client_name = "MovieClient"
```
