from pathlib import Path

import toml

from cognite.gqlpygen import version


def test_matching_versions():
    with (Path(__file__).parent.parent / "pyproject.toml").open() as fh:
        pyproject_toml = toml.load(fh)

    version_in_pyproject_toml = pyproject_toml["tool"]["poetry"]["version"]

    assert version.__version__ == version_in_pyproject_toml, (
        f"Version in pyproject.toml ({version_in_pyproject_toml}) does not match version in "
        f"cognite/gpqpygen/version ({version.__version__})"
    )
