"""Transforms a data model into the PygenModel used for code generation."""

from cognite.pygen._client.models import DataModelResponseWithViews, ViewResponse
from cognite.pygen._pygen_model import APIClassFile, DataClassFile, PygenSDKModel

from ._types import OutputFormat
from .config import NamingConfig, PygenSDKConfig


def to_pygen_model(
    data_model: DataModelResponseWithViews, output_format: OutputFormat, config: PygenSDKConfig | None = None
) -> PygenSDKModel:
    """Transforms a DataModelResponse into a PygenSDKModel for code generation.

    Args:
        data_model (DataModelResponse): The data model to transform.
        output_format (OutputFormat): The desired output format for the generated code.
        config (PygenSDKConfig): The SDK configuration.

    Returns:
        PygenSDKModel: The transformed PygenSDKModel.
    """
    if data_model.views is None:
        raise ValueError("Data model must have views to transform into PygenSDKModel.")
    config = config or PygenSDKConfig()
    naming = _create_naming(config.naming, output_format)

    model = PygenSDKModel(data_classes=[], api_classes=[])
    for view in data_model.views:
        if view.external_id in config.exclude_views:
            continue
        data_class = _create_data_class(view, naming, config)
        api_class = _create_api_class(data_class, view, naming, config)
        model.data_classes.append(data_class)
        model.api_classes.append(api_class)
    return model


def _create_naming(config: NamingConfig, output_format: OutputFormat) -> NamingConfig:
    """Creates a naming strategy based on the configuration and output format.

    Args:
        config (NamingConfig): The naming configuration.
        output_format (OutputFormat): The desired output format for the generated code.
    Returns:
        Naming strategy object.
    """
    language = _get_naming_config(output_format)
    return NamingConfig(
        class_name=language.class_name if config.class_name == "language_default" else config.class_name,
        field_name=language.field_name if config.field_name == "language_default" else config.field_name,
    )


def _get_naming_config(output_format: OutputFormat) -> NamingConfig:
    if output_format == "python":
        return NamingConfig(class_name="PascalCase", field_name="snake_case")
    elif output_format == "typescript":
        return NamingConfig(class_name="PascalCase", field_name="camelCase")
    raise NotImplementedError(f"Naming config for output format {output_format} is not implemented.")


def _create_data_class(view: ViewResponse, naming: NamingConfig, config: PygenSDKConfig) -> DataClassFile:
    raise NotImplementedError


def _create_api_class(
    data_class: DataClassFile,
    view: ViewResponse,
    naming: NamingConfig,
    config: PygenSDKConfig,
) -> APIClassFile:
    raise NotImplementedError()
