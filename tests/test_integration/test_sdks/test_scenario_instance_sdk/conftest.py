import pytest

from tests.constants import IS_PYDANTIC_V1

if IS_PYDANTIC_V1:
    from scenario_instance_pydantic_v1.client import ScenarioInstanceClient
else:
    from scenario_instance.client import ScenarioInstanceClient


@pytest.fixture()
def scenario_instance_client(client_config) -> ScenarioInstanceClient:
    return ScenarioInstanceClient.azure_project(**client_config)
