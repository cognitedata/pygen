## Installation Local
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

## Installation Options

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


## Pyodide Installation

In a `CDF Notebook` you can install `cognite-pygen` by running the following code in a cell:

```python
%pip install cognite-pygen
```

## Pyodide Troubleshooting

## Can't find a pure Python 3 wheel: 'cognite-pygen'
