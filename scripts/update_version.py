"""
This is a convince script to update the version number in the package.
"""
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.resolve()
EXAMPLES_DIR = REPO_ROOT / "examples"
EXAMPLES_DIR_PYDANTIC_V1 = REPO_ROOT / "examples-pydantic-v1"


def main():
    last_version = "0.15.2"
    new_version = "0.15.3"

    pyproject_toml = REPO_ROOT / "pyproject.toml"
    version_py = REPO_ROOT / "cognite" / "pygen" / "_version.py"
    api_client_files = list(EXAMPLES_DIR.glob("**/_api_client.py")) + list(
        EXAMPLES_DIR_PYDANTIC_V1.glob("**/_api_client.py")
    )

    for file in [pyproject_toml, version_py, *api_client_files]:
        content = file.read_text().replace(last_version, new_version)
        file.write_text(content)
        print(f"Updated {file.relative_to(REPO_ROOT)}, replacing {last_version} with {new_version}.")
    print("Done")


if __name__ == "__main__":
    main()
