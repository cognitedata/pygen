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
        sdk_config (PygenSDKConfig): The SDK configuration.

    Returns:
        PygenSDKModel: The transformed PygenSDKModel.
    """
    if data_model.views is None:
        raise ValueError("Data model must have views to transform into PygenSDKModel.")
    config = config or PygenSDKConfig()
    naming = _create_naming(config.naming, output_format)

    view_data_class_pairs = _create_data_classes(data_model.views, naming, config)
    api_classes = _create_api_classes(view_data_class_pairs, naming, config)
    return PygenSDKModel(
        data_classes=[dc for _, dc in view_data_class_pairs],
        api_classes=api_classes,
    )


def _create_naming(config: NamingConfig, output_format: OutputFormat) -> NamingConfig:
    """Creates a naming strategy based on the configuration and output format.

    Args:
        config (NamingConfig): The naming configuration.
        output_format (OutputFormat): The desired output format for the generated code.
    Returns:
        Naming strategy object.
    """
    raise NotImplementedError()


def _create_data_classes(
    views: list[ViewResponse], naming: NamingConfig, config: PygenSDKConfig
) -> list[tuple[ViewResponse, DataClassFile]]:
    raise NotImplementedError()


def _create_api_classes(
    data_classes: list[tuple[ViewResponse, DataClassFile]],
    naming: NamingConfig,
    config: PygenSDKConfig,
) -> list[APIClassFile]:
    raise NotImplementedError()
