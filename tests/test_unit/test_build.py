import toml

from cognite import pygen
from tests.constants import repo_root


def test_matching_versions():
    with (repo_root / "pyproject.toml").open() as fh:
        pyproject_toml = toml.load(fh)

    version_in_pyproject_toml = pyproject_toml["tool"]["poetry"]["version"]

    assert pygen.__version__ == version_in_pyproject_toml, (
        f"Version in pyproject.toml ({version_in_pyproject_toml}) does not match version in "
        f"cognite/pygen/version ({pygen.__version__})"
    )


def remove_top_lines(text: str, lines: int) -> str:
    return "\n".join(text.split("\n")[lines:])


def test_index_matching_readme():
    # Arrange
    readme = (repo_root / "README.md").read_text()
    index = (repo_root / "docs" / "index.md").read_text()

    # Assert
    assert remove_top_lines(readme, 5) == remove_top_lines(index, 1)
