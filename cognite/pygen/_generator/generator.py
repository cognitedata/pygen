from pathlib import Path

from cognite.pygen._python.instance_api.config import PygenClientConfig

from .config import PygenSDKConfig


def generate_sdk(
    space: str,
    external_id: str,
    version: str | None = None,
    client_config: PygenClientConfig | None = None,
    config: PygenSDKConfig | None = None,
) -> dict[Path, str]:
    """Generates an SDK for the specified View in Cognite Data Fusion.

    Args:
        space (str): The space of the View.
        external_id (str): The external ID of the View.
        version (str | None): The version of the View. If None, the latest version is used.
        client_config (PygenClientConfig | None): Configuration for the client generation. If None,
            default configuration is used.
        config (PygenSDKConfig | None): Configuration for the SDK generation. If None, default configuration is used.

    Returns:
        dict[Path, str]: A dictionary where keys are file paths and values are the corresponding file contents.
    """
    raise NotImplementedError()
