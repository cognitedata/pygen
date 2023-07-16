from __future__ import annotations

import getpass
from pathlib import Path

import pytest
import toml
from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials


@pytest.fixture(scope="session")
def client_config() -> dict[str, str]:
    return toml.load(Path(__file__).parent / "config.toml")["cognite"]


def create_cognite_client_config(
    tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
) -> ClientConfig:
    base_url = f"https://{cdf_cluster}.cognitedata.com/"
    credentials = OAuthClientCredentials(
        token_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=[f"{base_url}.default"],
    )
    return ClientConfig(
        project=project,
        credentials=credentials,
        client_name=getpass.getuser(),
        base_url=base_url,
    )


@pytest.fixture()
def cognite_client(client_config) -> CogniteClient:
    return CogniteClient(create_cognite_client_config(**client_config))
