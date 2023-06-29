from pathlib import Path

from ._core.sdk_generator import SDKGenerator
from ._version import __version__

__all__ = ["__version__", "SDKGenerator", "write_sdk_to_disk"]


def write_sdk_to_disk(sdk: dict[Path, str], output_dir: Path):
    """Write a generated SDK to disk.

    Args:
        sdk: The generated SDK.
        output_dir: The output directory to write to.
    """
    for path, content in sdk.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as fh:
            fh.write(content)
