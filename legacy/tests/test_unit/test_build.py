"""
These test are used to ensure that the pygen build is setup correctly.
For example, that version in pyproject.toml matches the version in cognite/pygen/_version.py
"""

from tests.constants import REPO_ROOT


def remove_top_lines(text: str, lines: int) -> str:
    return "\n".join(text.split("\n")[lines:])


def test_index_matching_readme():
    # Arrange
    readme = (REPO_ROOT / "README.md").read_text()
    index = (REPO_ROOT / "docs" / "index.md").read_text()

    # Assert
    assert remove_top_lines(readme, 2) == remove_top_lines(index, 1)
