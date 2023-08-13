from __future__ import annotations

from pathlib import Path

from cognite.client import CogniteClient


def load_cognite_client_from_toml(
    toml_file: Path | str = "config.toml", section: str | None = "cognite"
) -> CogniteClient:
    """
    This is a small helper function to load a CogniteClient from a toml file.

    Args:
        toml_file: Path to toml file
        section: Name of the section in the toml file to use. If None, use the "cognite" level of the toml file.

    Returns:
        A CogniteClient with configurations from the toml file.
    """
    import toml

    return CogniteClient.default_oauth_client_credentials(**toml.load(toml_file)[section])
