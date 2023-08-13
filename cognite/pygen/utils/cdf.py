from __future__ import annotations

from pathlib import Path

from cognite.client import CogniteClient


def get_cognite_client_from_toml(
    toml_file: Path | str = "config.toml", section: str | None = "cognite"
) -> CogniteClient:
    """
    Get a CogniteClient with configurations from a toml file.

    Parameters
    ----------
    toml_file : Path | str
        Path to toml file
    section: str | None
        Name of the section in the toml file to use. If None, use the top level of the toml file.

    Returns
    -------
    CogniteClient: A CogniteClient with configurations from the toml file.

    """
    import toml

    return CogniteClient.default_oauth_client_credentials(**toml.load(toml_file)[section])
