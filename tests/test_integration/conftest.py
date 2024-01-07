from __future__ import annotations

import getpass
from pathlib import Path

import pytest
import toml
from cognite.client import ClientConfig, CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials

from tests.constants import IS_PYDANTIC_V2, OMNI_SDK

if IS_PYDANTIC_V2:
    from omni import OmniClient
else:
    from omni_pydantic_v1 import OmniClient


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


@pytest.fixture(scope="session")
def cognite_client(client_config) -> CogniteClient:
    return CogniteClient(create_cognite_client_config(**client_config))


@pytest.fixture(scope="session")
def omni_client(cognite_client: CogniteClient) -> OmniClient:
    return OmniClient(cognite_client)


@pytest.fixture(scope="session")
def omni_views_by_external_id(omni_client: OmniClient) -> dict[str, dm.View]:
    data_model = OMNI_SDK.load_data_model()
    return {view.external_id: view for view in data_model._views}
