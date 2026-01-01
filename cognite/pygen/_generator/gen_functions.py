from pathlib import Path
from typing import Any

from cognite.pygen._client import PygenClient
from cognite.pygen._client.models import DataModelReference, DataModelResponseWithViews
from cognite.pygen._python.instance_api.config import PygenClientConfig

from ._types import OutputFormat
from .config import PygenSDKConfig
from .generator import Generator
from .python import PythonGenerator
from .typescript import TypeScriptGenerator


def _get_generator(output_format: OutputFormat) -> type[Generator]:
    """Returns the appropriate generator class based on the output format.

    Args:
        output_format (Literal["python", "typescript"]): The output format of the SDK.

    Returns:
        type[Generator]: The generator class corresponding to the specified output format.
    """
    if output_format == "python":
        return PythonGenerator
    elif output_format == "typescript":
        return TypeScriptGenerator
    else:
        raise NotImplementedError(f"Unsupported output format: {output_format}")


def generate_sdk(
    space: str,
    external_id: str,
    version: str,
    client_config: PygenClientConfig | None = None,
    sdk_config: PygenSDKConfig | None = None,
    output_format: OutputFormat = "python",
) -> dict[Path, str]:
    """Generates an SDK for the specified Data Model in Cognite Data Fusion.

    Args:
        space (str): The space of the data model.
        external_id (str): The external ID of the data model.
        version (str): The version of the data model.
        client_config (PygenClientConfig | None): Configuration for the client generation. If None,
            default configuration is used.
        sdk_config (PygenSDKConfig | None): Configuration for the SDK generation.
            If None, default configuration is used.
        output_format (Literal["python", "typescript"]): The output format of the SDK. Defaults to "python".

    Returns:
        dict[Path, str]: A dictionary where keys are file paths and values are the corresponding file contents.
    """
    data_model = _retrieve_data_model(space, external_id, version, config=client_config)
    generator_cls = _get_generator(output_format)
    generator = generator_cls(data_model=data_model, config=sdk_config)
    return generator.generate()


def _retrieve_data_model(
    space: str, external_id: str, version: str, config: PygenClientConfig | None = None
) -> DataModelResponseWithViews:
    """Retrieves the data model from Cognite Data Fusion.

    Args:
        space (str): The space of the View.
        external_id (str): The external ID of the View.
        version (str | None): The version of the View. If None, the latest version is used.

    Returns:
        DataModelResponseWithViews: The retrieved data model.
    """
    config = config or PygenClientConfig.default()
    with PygenClient(config=config) as client:
        data_models = client.data_models.retrieve(
            [DataModelReference(space=space, external_id=external_id, version=version)], inline_views=True
        )
    if not data_models:
        raise ValueError(
            f"Data model with space '{space}', external_id '{external_id}', and version '{version}' not found."
        )
    return data_models[0]


def generate_sdk_notebook(
    space: str,
    external_id: str,
    version: str | None = None,
    client_config: PygenClientConfig | None = None,
    sdk_config: PygenSDKConfig | None = None,
) -> Any:
    """Generates a Jupyter notebook for the specified View in Cognite Data Fusion.

    Args:
        space (str): The space of the View.
        external_id (str): The external ID of the View.
        version (str | None): The version of the View. If None, the latest version is used.
        client_config (PygenClientConfig | None): Configuration for the client generation. If None,
            default configuration is used.
        sdk_config (PygenSDKConfig | None): Configuration for the SDK generation.
            If None, default configuration is used.

    Returns:
        The instantiated generated client class.
    """
    raise NotImplementedError()
