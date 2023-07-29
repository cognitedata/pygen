import pytest

from tests.constants import IS_PYDANTIC_V1

if IS_PYDANTIC_V1:
    from shop_pydantic_v1.client import ShopClient
else:
    from shop.client import ShopClient


@pytest.fixture()
def shop_client(client_config) -> ShopClient:
    return ShopClient.azure_project(**client_config)
