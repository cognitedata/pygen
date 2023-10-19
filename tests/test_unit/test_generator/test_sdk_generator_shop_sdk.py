from pathlib import Path

import pytest
from cognite.client import data_modeling as dm

from cognite.pygen._core.generators import APIGenerator, MultiAPIGenerator, SDKGenerator
from cognite.pygen._generator import CodeFormatter, write_sdk_to_disk
from tests.constants import EXAMPLES_DIR, IS_PYDANTIC_V1, ShopSDKFiles


@pytest.fixture
def top_level_package() -> str:
    if IS_PYDANTIC_V1:
        return "shop_pydantic_v1.client"
    else:
        return "shop.client"


@pytest.fixture
def sdk_generator(shop_model: dm.DataModel, top_level_package: str):
    return SDKGenerator(top_level_package, "ShopClient", shop_model)


@pytest.fixture
def multi_api_generator(shop_model, top_level_package) -> MultiAPIGenerator:
    return MultiAPIGenerator(top_level_package, "ShopClient", shop_model.views)


@pytest.fixture
def command_api_generator(multi_api_generator: MultiAPIGenerator, command_config_view: dm.View) -> APIGenerator:
    api_generator = multi_api_generator[command_config_view]
    assert api_generator is not None, "Could not find API generator for command config view"
    return api_generator


def test_create_view_data_classes_case(
    multi_api_generator: MultiAPIGenerator, case_view: dm.View, top_level_package: str
):
    # Arrange
    expected = ShopSDKFiles.cases_data.read_text()
    api_generator = multi_api_generator[case_view]
    assert api_generator is not None, "Could not find API generator for case view"

    # Act
    actual = api_generator.generate_data_class_file()

    # Assert
    assert expected == actual


def test_generate_data_class_file_command_configs(command_api_generator: APIGenerator):
    # Arrange
    expected = ShopSDKFiles.command_configs_data.read_text()

    # Act
    actual = command_api_generator.generate_data_class_file()

    # Assert
    assert actual == expected


def test_create_view_api_classes_command_configs(
    command_api_generator: APIGenerator, top_level_package: str, code_formatter: CodeFormatter
):
    # Arrange
    expected = ShopSDKFiles.command_configs_api.read_text()

    # Act
    actual = command_api_generator.generate_api_file(top_level_package)
    actual = code_formatter.format_code(actual)

    assert actual == expected


def test_create_api_classes(
    multi_api_generator: MultiAPIGenerator,
):
    # Arrange
    expected = ShopSDKFiles.data_init.read_text()

    # Act
    actual = multi_api_generator.generate_data_classes_init_file()

    # Assert
    assert actual == expected


def test_generate_sdk(sdk_generator: SDKGenerator, movie_model: dm.DataModel, tmp_path: Path):
    # Act
    files_by_path = sdk_generator.generate_sdk()
    write_sdk_to_disk(files_by_path, tmp_path, overwrite=True, format_code=True)

    # Assert
    for file_path in tmp_path.glob("**/*.py"):
        relative_path = file_path.relative_to(tmp_path)
        expected = (EXAMPLES_DIR / relative_path).read_text()
        assert file_path.read_text() == expected
