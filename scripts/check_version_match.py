from pathlib import Path

import toml

from cognite.gqlpygen import version

with (Path(__file__).parent.parent / "pyproject.toml").open() as fh:
    pyproject_toml = toml.load(fh)

version_in_pyproject_toml = pyproject_toml["tool"]["poetry"]["version"]


if version.__version__ != version_in_pyproject_toml:
    print(
        f"Version in pyproject.toml ({version_in_pyproject_toml}) does not match version in "
        f"cognite/gpqpygen/version ({version.__version__})"
    )
    raise SystemExit(1)
