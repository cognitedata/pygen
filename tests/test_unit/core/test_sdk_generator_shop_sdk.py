from pathlib import Path

import pytest

# from black import Mode, Report, WriteBack, reformat_one
from cognite.client import data_modeling as dm

from cognite import pygen
from cognite.pygen._core.dms_to_python import APIGenerator, SDKGenerator
from tests.constants import ShopSDKFiles, examples_dir


@pytest.fixture
def sdk_generator(shop_model: dm.DataModel):
    return SDKGenerator("shop.client", "ShopClient", shop_model)


def test_create_view_data_classes_case(sdk_generator: SDKGenerator, case_view: dm.View):
    # Arrange
    expected = ShopSDKFiles.cases_data.read_text()

    # Act
    actual = APIGenerator(case_view).generate_data_class_file()

    # Assert
    assert expected == actual


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


def test_generate_sdk(sdk_generator: SDKGenerator, movie_model: dm.DataModel, tmp_path: Path):
    # Act
    files_by_path = sdk_generator.generate_sdk()
    pygen.write_sdk_to_disk(files_by_path, tmp_path)

    # Assert
    for file_path in tmp_path.glob("**/*.py"):
        relative_path = file_path.relative_to(tmp_path)
        expected = (examples_dir / relative_path).read_text()
        assert file_path.read_text() == expected
