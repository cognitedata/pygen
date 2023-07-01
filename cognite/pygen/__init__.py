from pathlib import Path

from ._core.sdk_generator import APIGenerator, SDKGenerator
from ._version import __version__

__all__ = ["__version__", "SDKGenerator", "APIGenerator", "write_sdk_to_disk"]


def write_sdk_to_disk(sdk: dict[Path, str], output_dir: Path):
    """Write a generated SDK to disk.

    Args:
        sdk: The generated SDK.
        output_dir: The output directory to write to.
    """
    for file_path, file_content in sdk.items():
        path = output_dir / file_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(file_content)
