from collections.abc import Set

from pydantic import BaseModel, Field

from ._types import UserCasing


class NamingConfig(BaseModel):
    class_name: UserCasing = "language_default"
    field_name: UserCasing = "language_default"


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
    pygen_as_dependency: bool = True
    exclude_views: Set[str] = Field(default_factory=set)
    exclude_spaces: Set[str] = Field(default_factory=set)
    naming: NamingConfig = Field(default_factory=NamingConfig)
