import pytest

from examples.shop.client import ShopClient


@pytest.fixture()
def shop_client(client_config) -> ShopClient:
    return ShopClient.azure_project(**client_config)
