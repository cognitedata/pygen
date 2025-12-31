from collections.abc import Set

from pydantic import BaseModel


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
