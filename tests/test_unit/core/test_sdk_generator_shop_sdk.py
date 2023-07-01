import pytest

# from black import Mode, Report, WriteBack, reformat_one
from cognite.client import data_modeling as dm

from cognite.pygen._core.sdk_generator import APIGenerator, SDKGenerator
from tests.constants import ShopSDKFiles


@pytest.fixture
def sdk_generator():
    return SDKGenerator("shop.client", "ShopClient")


def test_create_view_data_classes_case(sdk_generator: SDKGenerator, case_view: dm.View):
    # Arrange
    expected = ShopSDKFiles.cases_data.read_text()

    # Act
    actual = APIGenerator(case_view).generate_data_class()

    # Assert
    assert actual == expected


def test_create_view_data_classes_command_configs(sdk_generator: SDKGenerator, command_config_view: dm.View):
    # Arrange
    expected = ShopSDKFiles.command_configs_data.read_text()

    # Act
    actual = APIGenerator(command_config_view).generate_data_class()

    # Assert
    assert actual == expected


def test_create_view_api_classes_command_configs(sdk_generator: SDKGenerator, command_config_view: dm.View):
    # Arrange
    expected = ShopSDKFiles.command_configs_api.read_text()

    # Act
    actual = sdk_generator.view_to_api(command_config_view)

    assert actual == expected


def test_create_api_classes(sdk_generator: SDKGenerator, monkeypatch):
    # Arrange
    expected = ShopSDKFiles.data_init.read_text()
    monkeypatch.setattr(
        sdk_generator,
        "_dependencies_by_view_name",
        {
            "Case": {"Command_Config"},
        },
    )
    monkeypatch.setattr(
        sdk_generator,
        "_view_names",
        {
            "Case",
            "Command_Config",
        },
    )

    # Act
    actual = sdk_generator.create_data_classes_init()

    # Assert
    assert actual == expected
