"""Transforms a data model into the PygenModel used for code generation."""

from cognite.pygen._client.models import DataModelResponse
from cognite.pygen._pygen_model import PygenSDKModel

from .config import PygenSDKConfig


def to_pygen_model(data_model: DataModelResponse, sdk_config: PygenSDKConfig) -> PygenSDKModel:
    """Transforms a DataModelResponse into a PygenSDKModel for code generation.

    Args:
        data_model (DataModelResponse): The data model to transform.
        sdk_config (PygenSDKConfig): The SDK configuration.

    Returns:
        PygenSDKModel: The transformed PygenSDKModel.
    """
    raise NotImplementedError()
