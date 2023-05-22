import toml

from cognite.pygen import version
from tests.constants import REPO_ROOT


def test_matching_versions():
    with (REPO_ROOT / "pyproject.toml").open() as fh:
        pyproject_toml = toml.load(fh)

    version_in_pyproject_toml = pyproject_toml["tool"]["poetry"]["version"]

    assert version.__version__ == version_in_pyproject_toml, (
        f"Version in pyproject.toml ({version_in_pyproject_toml}) does not match version in "
        f"cognite/gpqpygen/version ({version.__version__})"
    )


def remove_top_lines(text: str, lines: int) -> str:
    return "\n".join(text.split("\n")[lines:])


def test_index_matching_readme():
    # Arrange
    readme = (REPO_ROOT / "README.md").read_text()
    index = (REPO_ROOT / "docs" / "index.md").read_text()

    # Assert
    assert remove_top_lines(readme, 5) == remove_top_lines(index, 1)
