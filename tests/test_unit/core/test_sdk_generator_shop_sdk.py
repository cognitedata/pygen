import pytest

# from black import Mode, Report, WriteBack, reformat_one
from cognite.client import data_modeling as dm

from cognite.pygen._core.sdk_generator import SDKGenerator
from tests.constants import ShopSDKFiles


@pytest.fixture
def sdk_generator():
    return SDKGenerator("movie_domain", "Movie")


def test_create_view_data_classes_case(sdk_generator: SDKGenerator, case_view: dm.View):
    # Arrange
    expected = ShopSDKFiles.cases_data.read_text()

    # Act
    actual = sdk_generator.view_to_data_classes(case_view)

    # Assert
    assert actual == expected


def test_create_view_data_classes_command_configs(sdk_generator: SDKGenerator, command_config_view: dm.View):
    # Arrange
    expected = ShopSDKFiles.command_configs_data.read_text()

    # Act
    actual = sdk_generator.view_to_data_classes(command_config_view)

    # Assert
    assert actual == expected
