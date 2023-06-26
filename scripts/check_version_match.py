from pathlib import Path

import toml

from cognite import pygen

with (Path(__file__).parent.parent / "pyproject.toml").open() as fh:
    pyproject_toml = toml.load(fh)

version_in_pyproject_toml = pyproject_toml["tool"]["poetry"]["version"]


if pygen.__version__ != version_in_pyproject_toml:
    print(
        f"Version in pyproject.toml ({version_in_pyproject_toml}) does not match version in "
        f"cognite/pygen/version ({pygen.__version__})"
    )
    raise SystemExit(1)
