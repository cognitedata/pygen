## Development Instructions

`pygen` is currently mainly developed by [@doctrino](https://github.com/doctrino) which also functions as
the `BDFL` of the project.

Contributions are welcome, but please open an issue first to discuss the proposed changes. If you are a Cognite employee,
please join the `#topic-pygen` channel on Slack and post issues there.

### Installation

Get the code!

```bash
https://github.com/cognitedata/pygen.git
cd cognite-pygen
```

We use [poetry](https://pypi.org/project/poetry/) for dependency- and virtual environment management.

When developing `pygen` use Python `3.9`. To specify the python version with `poetry` you can use the command
```bash
poetry env use 3.9
```
See [poetry docs](https://python-poetry.org/docs/managing-environments/) for more information on managing environments.

Install `pygen` with all the extra dependencies and then activate the virtual environment, with these commands:

```bash
poetry install --all-extras
poetry shell
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

### Installation `pydantic` `v1`

`pygen` is supporting `pydantic` `v1` and `v2`. It is recommended that you have two virtual environments, one for each version of `pydantic`.
To create a virtual environment for `pydantic` `v1` follow the steps above and then run the following command:

```bash
pip install pydantic==1.10.7
```
**Note** IDEs such as PyCharm supports multiple virtual environments, so you can have one project with two virtual environments, one for each version of `pydantic`.
See [PyCharm docs](https://www.jetbrains.com/help/pycharm/creating-virtual-environment.html) for more information on how to create virtual environments in PyCharm.

### Testing

Initiate unit tests by running the following command from the root directory:

`pytest`


### Generating Example SDKs


### Release version conventions

See https://semver.org/
