from __future__ import annotations

import platform

import pytest
from cognite.client import data_modeling as dm

from cognite.pygen._core.generators import APIGenerator, MultiAPIGenerator, SDKGenerator
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


@pytest.fixture
def multi_api_generator(sdk_generator: SDKGenerator) -> SDKGenerator:
    return sdk_generator._multi_api_generator


@pytest.fixture
def date_transformation_generator(
    multi_api_generator: MultiAPIGenerator, date_transformation_pair_view: dm.View
) -> APIGenerator:
    api_generator = multi_api_generator[date_transformation_pair_view]
    assert api_generator is not None, "Could not find API generator for date transformation view"
    return api_generator


@pytest.mark.skipif(
    platform.platform() != "Windows", reason="There is currently some strange problem with the diff on non-windows"
)
def test_generate_api_client(sdk_generator: SDKGenerator, code_formatter: CodeFormatter):
    # Arrange
    expected = MarketSDKFiles.client.read_text()

    # Act
    actual = sdk_generator._generate_api_client_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_date_transformation_pairs_data_class(
    date_transformation_generator: APIGenerator, code_formatter: CodeFormatter
):
    # Arrange
    expected = MarketSDKFiles.date_transformation_pair_data.read_text()

    # Act
    actual = date_transformation_generator.generate_data_class_file()

    # Assert
    actual = code_formatter.format_code(actual)
    assert actual == expected


def test_generate_date_transformation_pairs_data_api(
    date_transformation_generator: APIGenerator, top_level_package: str, code_formatter: CodeFormatter
):
    # Arrange
    expected = MarketSDKFiles.date_transformation_pair_api.read_text()

    # Act
    actual = date_transformation_generator.generate_api_file(top_level_package)

    # Assert
    actual = code_formatter.format_code(actual)
    assert actual == expected
