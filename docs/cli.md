## Create Python SDK using CLI

Given a Data Model with external id `Movie` in the space `movies` in CDF, the following command will generate a Python SDK


=== "Microsoft Entra ID (Azure AD)"

    ```bash
    pygen generate --space movies \
        --external-id Movie \
        --version 1 \
        --tenant-id <tenant-id> \
        --client-id <client-id> \
        --client-secret <client-secret> \
        --cdf-cluster <cdf-cluster> \
        --cdf-project <cdf-project>
        --instance-space <my_instance_space>
    ```

=== "Generic OIDC authentication"

    ```bash
    pygen generate --space movies \
        --external-id Movie \
        --version 1 \
        --token-url <token-url> \
        --scopes <scopes> \
        --audience <audience> \
        --client-id <client-id> \
        --client-secret <client-secret> \
        --cdf-cluster <cdf-cluster> \
        --cdf-project <cdf-project>
        --instance-space <my_instance_space>
    ```

In addition, the following options are available and recommended to be used:

* `--output-dir` This is the directory where the generated SDK will be placed.
* `--top-level-package` The top level package for where to place the SDK, for example `movie_sdk.client`.
* `--client-name` Client name for the generated client expected to be given in `PascalCase`, for example `MovieClient`.
* `--instance-space` The instance space to use for the generated SDK.

For more information about the available options, see `pygen --help`.

!!! warning "Multiple Data Models"
    The CLI does NOT support generating an SDK for multiple data models. Then, you have to use a `pyproject.toml` file,
    see below.


### <code>pyproject.toml</code>
When running the command `pygen generate`, the program looks for a `pyproject.toml` and `.secret.toml` file in the current
working directory. If the `pyproject.toml` file is found and has a `[tool.pygen]` section, the values from this section will be used as
default values for the command line arguments. The `.secret.toml` is expected to have a `[cognite]` section with a `client_secret` value.

Below is an example of the values that can be set in the `pyproject.toml` and `.secret.toml` files.

=== "pyproject.toml"

    ```toml
    [tool.pygen]
    data_models = [
        ["IntegrationTestsImmutable", "Movie", "2"],
    ]
    tenant_id = "<cdf-project>"
    client_id = "<client-id>"
    cdf_cluster = "<cdf-cluster>"
    cdf_project = "<cdf-project>"
    top_level_package = "movie_domain.client"
    client_name = "MovieClient"
    output_dir = "."
    instance_space = "my_instance_space"
    ```

=== ".secret.toml"

    ``` bash
    [cognite]
      client_secret = "<client-id>"
    ```
