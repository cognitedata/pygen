from __future__ import annotations

import pytest
from cognite.client import data_modeling as dm

from cognite.pygen._core.dms_to_python import APIGenerator, MultiModelSDKGenerator
from tests.constants import IS_PYDANTIC_V1, MarketSDKFiles

# from black import Mode, Report, WriteBack, reformat_one


@pytest.fixture
def top_level_package() -> str:
    if IS_PYDANTIC_V1:
        return "markets_pydantic_v1.client"
    else:
        return "markets.client"


@pytest.fixture
def sdk_generator(pygen_pool_model, cog_pool_model, top_level_package) -> MultiModelSDKGenerator:
    return MultiModelSDKGenerator(top_level_package, "MarketClient", [pygen_pool_model, cog_pool_model])


@pytest.mark.skip("iSort sorts differently than sort.")
def test_generate__api_client(sdk_generator: MultiModelSDKGenerator):
    # Arrange
    expected = MarketSDKFiles.client.read_text()

    # Act
    actual = sdk_generator.generate_api_client_file()

    # Assert
    assert actual == expected


def test_generate_date_transformation_pairs_data_class(date_transformation_pair_view: dm.View, top_level_package: str):
    # Arrange
    expected = MarketSDKFiles.date_transformation_pair_data.read_text()

    # Act
    actual = APIGenerator(date_transformation_pair_view, top_level_package).generate_data_class_file()

    # Assert
    assert actual == expected


@pytest.mark.skip("Black causes this one to fail.")
def test_generate_date_transformation_pairs_data_api(date_transformation_pair_view: dm.View, top_level_package: str):
    # Arrange
    expected = MarketSDKFiles.date_transformation_pair_api.read_text()

    # Act
    actual = APIGenerator(date_transformation_pair_view, top_level_package).generate_api_file(top_level_package)

    # Assert
    assert actual == expected
