from __future__ import annotations

import pytest

from cognite.pygen._core.dms_to_python import MultiModelSDKGenerator
from tests.constants import MarketSDKFiles

# from black import Mode, Report, WriteBack, reformat_one


@pytest.fixture
def top_level_package() -> str:
    return "markets.client"


@pytest.fixture
def sdk_generator(pygen_pool_model, cog_pool_model, top_level_package) -> MultiModelSDKGenerator:
    return MultiModelSDKGenerator(top_level_package, "MarketClient", [pygen_pool_model, cog_pool_model])


def test_generate__api_client(sdk_generator: MultiModelSDKGenerator):
    # Arrange
    expected = MarketSDKFiles.client.read_text()

    # Act
    actual = sdk_generator.generate_api_client_file()

    # Assert
    assert actual == expected
