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
