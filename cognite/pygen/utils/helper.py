import contextlib
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Literal

from pydantic.version import VERSION as PYDANTIC_VERSION


def get_pydantic_version() -> Literal["v1", "v2"]:
    """Infer the pydantic version based on the environment."""
    if PYDANTIC_VERSION.startswith("1."):
        return "v1"
    elif PYDANTIC_VERSION.startswith("2."):
        return "v2"
    else:
        raise ValueError(f"Unknown pydantic version: {PYDANTIC_VERSION}")


@contextlib.contextmanager
def chdir(new_dir: Path) -> Iterator[None]:
    """
    Change directory to new_dir and return to the original directory when exiting the context.

    Args:
        new_dir: The new directory to change to.

    """
    current_working_dir = Path.cwd()
    os.chdir(new_dir)

    try:
        yield

    finally:
        os.chdir(current_working_dir)
