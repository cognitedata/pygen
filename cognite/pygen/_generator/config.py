from collections.abc import Set

from pydantic import BaseModel, Field
from pydantic.alias_generators import to_camel, to_pascal, to_snake

from ._types import Casing, OutputFormat


class NamingConfig(BaseModel):
    class_name: Casing
    field_name: Casing
    file_name: Casing


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


class InternalPygenSDKConfig(PygenSDKConfig):
    naming: NamingConfig
    max_line_length: int = 120


def create_internal_config(config: PygenSDKConfig, output_format: OutputFormat) -> InternalPygenSDKConfig:
    """Creates an internal SDK configuration with strict naming from the user-provided configuration.

    Args:
        config (PygenSDKConfig): The user-provided SDK configuration.
        output_format (OutputFormat): The desired output format for the generated code.
    Returns:
        InternalPygenSDKConfig: The internal SDK configuration with strict naming.
    """
    naming = _get_naming_config(output_format)
    return InternalPygenSDKConfig.model_construct(**config.model_dump(), naming=naming)


def _get_naming_config(output_format: OutputFormat) -> NamingConfig:
    if output_format == "python":
        return NamingConfig(class_name="PascalCase", field_name="snake_case", file_name="snake_case")
    elif output_format == "typescript":
        return NamingConfig(class_name="PascalCase", field_name="camelCase", file_name="camelCase")
    raise NotImplementedError(f"Naming config for output format {output_format} is not implemented.")


def to_casing(name: str, casing: Casing) -> str:
    # First convert to snake_case as an intermediate step to handle
    # mixed case input (e.g., "CategoryNode" or "categoryName")
    snake = to_snake(name)
    if casing == "camelCase":
        return to_camel(snake)
    elif casing == "PascalCase":
        return to_pascal(snake)
    elif casing == "snake_case":
        return snake
    else:
        raise NotImplementedError(f"Unsupported casing: {casing}")
