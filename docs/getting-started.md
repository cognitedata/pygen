## Installation

**Prerequisites**: Installed Python 3.9 or newer, see [python.org](https://www.python.org/downloads/)

1. Create a virtual environment.
2. Activate your virtual environment.
3. Install `cognite-pygen` with the CLI option.

=== "Windows"

    ```
    python -m venv venv
    ```
    ```
    venv\Scripts\activate.bat
    ```
    ```
    pip install cognite-pygen[cli]
    ```

=== "Mac/Linux"

    ``` bash
    python -m venv venv
    ```
    ``` bash
    source venv/bin/activate
    ```
    ``` bash
    pip install "cognite-pygen[cli]"
    ```


## Create Python SDK

Given a Data Model with external id `Movie` in the space `movies` in CDF, the following command will generate a Python SDK

```bash
pygen --space movies --external-id Movie --tenant-id <tenant-id> --client-id <client-id> --client-secret <client-secret> --cdf-cluster <cdf-cluster> --cdf-procect <cdf-project>
```

In addition, the following options are available and recommended to be used:

* `--output-dir` This is the directory where the generated SDK will be placed.
* `--top-level-package` The top level package for where to place the SDK, for example `movie_sdk.client`.
* `--client-name` Client name for the generated client expected to be given in `PascalCase`, for example `MovieClient`.

For more information about the available options, see `pygen --help`.
