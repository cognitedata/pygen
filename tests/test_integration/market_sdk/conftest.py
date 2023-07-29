import pytest
from cognite.client import CogniteClient

from tests.constants import IS_PYDANTIC_V1

if IS_PYDANTIC_V1:
    from markets_pydantic_v1.client import MarketClient
else:
    from markets.client import MarketClient


@pytest.fixture(scope="session")
def market_client(client_config) -> MarketClient:
    return MarketClient.azure_project(**client_config)


@pytest.fixture(scope="session")
def cognite_client(market_client: MarketClient) -> CogniteClient:
    return market_client.pygen_pool.markets._client
