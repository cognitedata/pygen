import pytest
from scenario_instance.client import ScenarioInstanceClient


@pytest.fixture()
def client(client_config) -> ScenarioInstanceClient:
    return ScenarioInstanceClient.azure_project(**client_config)
