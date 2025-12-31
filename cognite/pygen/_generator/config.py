from collections.abc import Set
from typing import Literal, TypeAlias

from pydantic import BaseModel, Field

NamingConvention: TypeAlias = Literal["camelCase", "PascalCase", "snake_case", "language_default"]


class NamingConfig(BaseModel):
    class_name: NamingConvention = "language_default"
    field_name: NamingConvention = "language_default"


class PygenSDKConfig(BaseModel):
    """
    Configuration settings for the Pygen SDK generator.
    """

    top_level_package: str = "pygen_sdk"
    client_name: str = "PygenClient"
    default_instance_space: str | None = None
    output_directory: str = "."
    overwrite: bool = False
    format_code: bool = True
    exclude_views: Set[str] = Field(default_factory=set)
    exclude_spaces: Set[str] = Field(default_factory=set)
    naming: NamingConfig = Field(default_factory=NamingConfig)
