from __future__ import annotations

import platform

import pytest

from cognite.pygen._core.generators import SDKGenerator
from cognite.pygen._generator import CodeFormatter
from tests.constants import IS_PYDANTIC_V1, MarketSDKFiles


@pytest.fixture
def top_level_package() -> str:
    if IS_PYDANTIC_V1:
        return "markets_pydantic_v1.client"
    else:
        return "markets.client"


@pytest.fixture
def client_name() -> str:
    return "MarketClient"


@pytest.fixture
def sdk_generator(pygen_pool_model, cog_pool_model, top_level_package, client_name: str) -> SDKGenerator:
    return SDKGenerator(
        top_level_package=top_level_package, client_name=client_name, data_model=[pygen_pool_model, cog_pool_model]
    )


@pytest.mark.skipif(
    not platform.platform().startswith("Windows"),
    reason="There is currently some strange problem with the diff on non-windows",
)
def test_generate_api_client(sdk_generator: SDKGenerator, code_formatter: CodeFormatter):
    # Arrange
    expected = MarketSDKFiles.client.read_text()

    # Act
    actual = sdk_generator._generate_api_client_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected
