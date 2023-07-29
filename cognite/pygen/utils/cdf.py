from __future__ import annotations

from pathlib import Path

from cognite.client import CogniteClient


def get_cognite_client_from_toml(toml_file: Path | str, section: str | None = "cognite") -> CogniteClient:
    import toml

    return CogniteClient.default_oauth_client_credentials(**toml.load(toml_file)[section])
