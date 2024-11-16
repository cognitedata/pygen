from __future__ import annotations

import getpass
import os
from pathlib import Path

import pytest
import toml
from cognite.client import ClientConfig, CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials
from cognite_core import CogniteCoreClient
from omni import OmniClient
from omni_sub import OmniSubClient
from wind_turbine import WindTurbineClient

from tests.constants import OMNI_SDK


@pytest.fixture(scope="session")
def client_config() -> dict[str, str]:
    config_file = Path(__file__).parent / "config.toml"
    if config_file.exists():
        return toml.load(config_file)["cognite"]
    else:
        return {
            "tenant_id": os.environ["IDP_TENANT_ID"],
            "client_id": os.environ["IDP_CLIENT_ID"],
            "client_secret": os.environ["IDP_CLIENT_SECRET"],
            "cdf_cluster": os.environ["CDF_CLUSTER"],
            "project": os.environ["CDF_PROJECT"],
        }


@pytest.fixture(scope="session")
def client_config_alpha() -> dict[str, str]:
    config_file = Path(__file__).parent.parent.parent / "alpha_config.toml"
    if config_file.exists():
        return toml.load(config_file)["cognite"]
    pytest.skip("No config_alpha.toml file found")


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
def cognite_client_alpha(client_config_alpha: dict[str, str]) -> CogniteClient:
    return CogniteClient(create_cognite_client_config(**client_config_alpha))


@pytest.fixture(scope="session")
def omni_client(cognite_client: CogniteClient) -> OmniClient:
    return OmniClient(cognite_client)


@pytest.fixture(scope="session")
def core_client(cognite_client: CogniteClient) -> CogniteCoreClient:
    return CogniteCoreClient(cognite_client)


@pytest.fixture(scope="session")
def omnisub_client(cognite_client: CogniteClient) -> OmniSubClient:
    return OmniSubClient(cognite_client)


@pytest.fixture(scope="session")
def turbine_client(cognite_client: CogniteClient) -> WindTurbineClient:
    return WindTurbineClient(cognite_client)


@pytest.fixture(scope="session")
def omni_views_by_external_id(omni_client: OmniClient) -> dict[str, dm.View]:
    data_model = OMNI_SDK.load_data_model()
    return {view.external_id: view for view in data_model.views}


@pytest.fixture
def omni_tmp_space(cognite_client: CogniteClient) -> dm.Space:
    space = dm.SpaceApply(space="sp_omni_test_tmp", description="Temporary space for omni integration tests")
    retrieved = cognite_client.data_modeling.spaces.retrieve(space.space)
    if retrieved:
        return retrieved
    created = cognite_client.data_modeling.spaces.apply(space)
    return created
