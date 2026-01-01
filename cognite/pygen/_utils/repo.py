import subprocess
from pathlib import Path


def repo_root() -> Path:
    """Get the root path of the git repository."""
    # Raise an error if not in a git repository
    try:
        result = subprocess.run("git rev-parse --show-toplevel".split(), stdout=subprocess.PIPE)
    except Exception as e:
        raise RuntimeError("Git is not installed or not found in PATH") from e
    output = result.stdout.decode().strip()
    if not output:
        raise RuntimeError("Not in a git repository")
    return Path(output)
