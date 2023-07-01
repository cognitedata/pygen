import pytest

# from black import Mode, Report, WriteBack, reformat_one
from cognite.client import data_modeling as dm

from cognite.pygen._core.sdk_generator import APIGenerator, SDKGenerator
from tests.constants import ShopSDKFiles


@pytest.fixture
def sdk_generator(shop_model: dm.DataModel):
    return SDKGenerator("shop.client", "ShopClient", shop_model)


def test_create_view_data_classes_case(sdk_generator: SDKGenerator, case_view: dm.View):
    # Arrange
    expected = ShopSDKFiles.cases_data.read_text()

    # Act
    actual = APIGenerator(case_view).generate_data_class_file()

    # Assert
    assert actual == expected


def test_generate_data_class_file_command_configs(command_config_view: dm.View):
    # Arrange
    expected = ShopSDKFiles.command_configs_data.read_text()

    # Act
    actual = APIGenerator(command_config_view).generate_data_class_file()

    # Assert
    assert actual == expected


def test_create_view_api_classes_command_configs(command_config_view: dm.View):
    # Arrange
    expected = ShopSDKFiles.command_configs_api.read_text()

    # Act
    actual = APIGenerator(command_config_view).generate_api_file("shop.client")

    assert actual == expected


def test_create_api_classes(
    sdk_generator: SDKGenerator,
):
    # Arrange
    expected = ShopSDKFiles.data_init.read_text()

    # Act
    actual = sdk_generator.generate_data_classes_init_file()

    # Assert
    assert actual == expected
