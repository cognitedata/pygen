## Introduction

After you have built your logic with the newly generated data model SDK, you might want to deploy it to
Cognite Functions.

Cognite Functions enables Python code to be hosted and executed in the cloud, on demand or by using a schedule.

A few useful links for Cognite Functions:

- [Cognite Functions documentation](https://docs.cognite.com/cdf/functions/)
- Deploy and manage your Cognite Functions with [Cognite Toolkit](https://docs.cognite.com/cdf/deploy/cdf_toolkit/)
- Expected folder structure for Cognite Toolkit [here](https://docs.cognite.com/cdf/deploy/cdf_toolkit/references/configs#functions)
- [What is a wheel?](https://realpython.com/python-wheels/)

## Deploying your generated SDK to Cognite Functions

Cognite Functions supports private packages in your function deployment, and `pygen` can generate a `wheel` package of
your data model SDK. To generate a `wheel` package, use the [build_wheel](api/api.html#cognite.pygen.build_wheel) function.

For example, given the data model (`power-models`, `Windmill`, `1`) we generated in the
[Generation](usage/generation.html) guide,
we can build a `wheel` package with the following code:

```python
from cognite.client import CogniteClient
from cognite.pygen import build_wheel

client = CogniteClient()

build_wheel(("power-models", "Windmill", 1), client)
```

This will output a `wheel` package, `windmill-1.0.0-py3-none-any.whl`, in the `dist` folder of the current working directory.
**Note** The version of the package will always be `1.0.0` as the version of a data model can be an arbitrary string,
while the version of a package must be a valid semantic version.

To deploy generated SDK to Cognite Functions, you can use the `cognite-toolkit` CLI with the following structure,
```
📦functions/
┣ 📂my_function - The function folder
┃ ┣ 📦windmill-1.0.0-py3-none-any.whl - The generated wheel package
┃ ┣ 📜__init__.py - Empty file (required to make the function into a package)
┃ ┣ 📜handler.py -  Module with script inside a handle function
┃ ┗ 📜requirements.txt - Explicitly states the dependencies needed to run the handler.py script.
┣ 📜my_function.Function.yaml - The configuration file for the function
┣ 📜my_schedule.Schedule.yaml - The configuration file for the function schedule(s)
```

=== "handler.py"

    ```python
    from cognite.client import CogniteClient
    from windmill WindmillClient


    def handle(client: CogniteClient, data: dict):
        windmill_client = WindmillClient(client)
        windmills = windmill_client.windmill.list(limit=10)
        print(windmills)
        return "Success"

    ```

=== "requirements.txt"

    ```txt
    # The Cognite SDK should be specified and match the version used to generate the SDK.
    cognite-sdk==7.54.4
    # This is the syntax for adding a private wheel in a Cognite Function.
    # Note the 'function' prefix is not a placeholder, it is a required prefix for private packages.
    function/windmill-1.0.0-py3-none-any.whl
    ```

=== "my_function.Function.yaml"

    ```yaml
    name: my_function
    externalId: fn_my_function
    owner: Anonymous
    description: 'Demo of a function with a pygen generated SDK'
    runtime: 'py311'
    ```

=== "my_schedule.Schedule.yaml"

    ```yaml
    - name: "daily-8am-utc"
      functionExternalId: fn_my_function
      description: "Run every day at 8am UTC"
      cronExpression: "0 8 * * *"
      data:
        some: "data"
    ```

After you have the folder structure set up, you can deploy the function using the `cognite-toolkit` CLI,
go to [Deploy](https://docs.cognite.com/cdf/deploy/cdf_toolkit/guides/configure_deploy_modules)
for more information on how to use the CLI.
