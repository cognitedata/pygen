from __future__ import annotations

import platform

import pytest
from cognite.client import data_modeling as dm

from cognite.pygen._core.dms_to_python import APIGenerator, SDKGenerator
from cognite.pygen._generator import CodeFormatter
from tests.constants import IS_PYDANTIC_V1, MarketSDKFiles


@pytest.fixture
def top_level_package() -> str:
    if IS_PYDANTIC_V1:
        return "markets_pydantic_v1.client"
    else:
        return "markets.client"


@pytest.fixture
def sdk_generator(pygen_pool_model, cog_pool_model, top_level_package) -> SDKGenerator:
    return SDKGenerator(
        top_level_package=top_level_package, client_name="MarketClient", data_model=[pygen_pool_model, cog_pool_model]
    )


def test_generate__api_client(sdk_generator: SDKGenerator):
    # Arrange
    expected = MarketSDKFiles.client.read_text()

    # Act
    actual = sdk_generator._generate_api_client_file()

    # Assert
    assert actual == expected


def test_generate_date_transformation_pairs_data_class(
    date_transformation_pair_view: dm.View, top_level_package: str, code_formatter: CodeFormatter
):
    # Arrange
    expected = MarketSDKFiles.date_transformation_pair_data.read_text()

    # Act
    actual = APIGenerator(date_transformation_pair_view, top_level_package).generate_data_class_file()

    # Assert
    actual = code_formatter.format_code(actual)
    assert actual == expected


@pytest.mark.skipif(platform.platform() == "Windows", reason="Only works on windows.")
def test_generate_date_transformation_pairs_data_api(
    date_transformation_pair_view: dm.View, top_level_package: str, code_formatter: CodeFormatter
):
    # Arrange
    expected = MarketSDKFiles.date_transformation_pair_api.read_text()

    # Act
    actual = APIGenerator(date_transformation_pair_view, top_level_package).generate_api_file(top_level_package)

    # Assert
    actual = code_formatter.format_code(actual)
    assert actual == expected
