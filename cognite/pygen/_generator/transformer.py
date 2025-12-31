"""Transforms a data model into the PygenModel used for code generation."""

from cognite.pygen._client.models import DataModelResponseWithViews
from cognite.pygen._pygen_model import PygenSDKModel

from .config import PygenSDKConfig


def to_pygen_model(data_model: DataModelResponseWithViews, sdk_config: PygenSDKConfig | None = None) -> PygenSDKModel:
    """Transforms a DataModelResponse into a PygenSDKModel for code generation.

    Args:
        data_model (DataModelResponse): The data model to transform.
        sdk_config (PygenSDKConfig): The SDK configuration.

    Returns:
        PygenSDKModel: The transformed PygenSDKModel.
    """
    raise NotImplementedError()
