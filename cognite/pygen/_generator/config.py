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

    top_level_package: str
    client_name: str
    default_instance_space: str
    output_directory: str
    overwrite: bool
    format_code: bool
    exclude_views: Set[str]
    exclude_spaces: Set[str]
    naming: NamingConfig = Field(default_factory=NamingConfig)
