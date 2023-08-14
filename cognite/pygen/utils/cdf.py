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
        section: Name of the section in the toml file to use. If None, use the top level of the toml file.
                 Defaults to "cognite".

    Returns:
        A CogniteClient with configurations from the toml file.
    """
    import toml

    toml_content = toml.load(toml_file)
    if section is not None:
        toml_content = toml_content[section]

    return CogniteClient.default_oauth_client_credentials(**toml_content)
