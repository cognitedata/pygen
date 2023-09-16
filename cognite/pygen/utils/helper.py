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
