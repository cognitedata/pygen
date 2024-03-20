from __future__ import annotations

import difflib

import pytest
from cognite.client import data_modeling as dm

from cognite.pygen._core.generators import APIGenerator, MultiAPIGenerator, SDKGenerator
from cognite.pygen._generator import CodeFormatter
from cognite.pygen.config import PygenConfig
from tests.constants import EQUIPMENT_UNIT_SDK, IS_PYDANTIC_V2, EquipmentSDKFiles


@pytest.fixture
def sdk_generator(equipment_unit_model: dm.DataModel[dm.View], pygen_config: PygenConfig) -> SDKGenerator:
    return SDKGenerator(
        EQUIPMENT_UNIT_SDK.top_level_package, EQUIPMENT_UNIT_SDK.client_name, equipment_unit_model, config=pygen_config
    )


@pytest.fixture
def multi_api_generator(sdk_generator: SDKGenerator) -> MultiAPIGenerator:
    return sdk_generator._multi_api_generator


@pytest.fixture
def unit_procedure_api_generator(multi_api_generator: MultiAPIGenerator, unit_procedure_view: dm.View) -> APIGenerator:
    api_generator = multi_api_generator[unit_procedure_view.as_id()]
    assert api_generator is not None, "Could not find API generator for unit procedure view"
    return api_generator


@pytest.fixture
def equipment_module_api_generator(
    multi_api_generator: MultiAPIGenerator, equipment_module_view: dm.View
) -> APIGenerator:
    api_generator = multi_api_generator[equipment_module_view.as_id()]
    assert api_generator is not None, "Could not find API generator for equipment module view"
    return api_generator


@pytest.fixture
def start_end_time_api_generator(multi_api_generator: MultiAPIGenerator, start_end_time_view: dm.View) -> APIGenerator:
    api_generator = multi_api_generator[start_end_time_view.as_id()]
    assert api_generator is not None, "Could not find API generator for start end time view"
    return api_generator


def test_generate_data_class_file_unit_procedure(
    unit_procedure_api_generator: APIGenerator, pygen_config: PygenConfig, code_formatter: CodeFormatter
):
    # Arrange
    expected = EquipmentSDKFiles.unit_procedure_data.read_text()

    # Act
    actual = unit_procedure_api_generator.generate_data_class_file(IS_PYDANTIC_V2)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_data_class_start_end_time(
    start_end_time_api_generator: APIGenerator, pygen_config: PygenConfig, code_formatter: CodeFormatter
):
    # Arrange
    expected = EquipmentSDKFiles.start_end_time_data.read_text()

    # Act
    actual = start_end_time_api_generator.generate_data_class_file(IS_PYDANTIC_V2)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_data_class_equipment_module(
    equipment_module_api_generator: APIGenerator, pygen_config: PygenConfig, code_formatter: CodeFormatter
):
    # Arrange
    expected = EquipmentSDKFiles.equipment_module_data.read_text()

    # Act
    actual = equipment_module_api_generator.generate_data_class_file(IS_PYDANTIC_V2)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_create_view_api_classes_equipment_module(
    equipment_module_api_generator: APIGenerator, code_formatter: CodeFormatter
):
    # Arrange
    expected = EquipmentSDKFiles.equipment_api.read_text()

    # Act
    actual = equipment_module_api_generator.generate_api_file(
        EQUIPMENT_UNIT_SDK.top_level_package, EQUIPMENT_UNIT_SDK.client_name
    )
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_equipment_module_sensor_value_api(
    equipment_module_api_generator: APIGenerator, code_formatter: CodeFormatter
):
    # Arrange
    expected = EquipmentSDKFiles.equipment_module_sensor_value_api.read_text()

    # Act
    _, actual = next(
        equipment_module_api_generator.generate_timeseries_api_files(
            EQUIPMENT_UNIT_SDK.top_level_package, EQUIPMENT_UNIT_SDK.client_name
        )
    )
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_create_view_api_classes_unit_procedure(
    unit_procedure_api_generator: APIGenerator, code_formatter: CodeFormatter
):
    # Arrange
    expected = EquipmentSDKFiles.unit_procedure_api.read_text()

    # Act
    actual = unit_procedure_api_generator.generate_api_file(
        EQUIPMENT_UNIT_SDK.top_level_package, EQUIPMENT_UNIT_SDK.client_name
    )
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_create_view_api_classes_unit_procedure_query(
    unit_procedure_api_generator: APIGenerator, code_formatter: CodeFormatter
):
    # Arrange
    expected = EquipmentSDKFiles.unit_procedure_query.read_text()

    # Act
    actual = unit_procedure_api_generator.generate_api_query_file(
        EQUIPMENT_UNIT_SDK.top_level_package, EQUIPMENT_UNIT_SDK.client_name
    )
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_create_view_api_classes_unit_procedure_work_units(
    unit_procedure_api_generator: APIGenerator, code_formatter: CodeFormatter
):
    # Arrange
    expected = EquipmentSDKFiles.unit_procedure_work_units.read_text()

    # Act

    actual_by_file_name = {
        filename: content
        for filename, content in unit_procedure_api_generator.generate_edge_api_files(
            EQUIPMENT_UNIT_SDK.top_level_package, EQUIPMENT_UNIT_SDK.client_name
        )
    }
    actual = actual_by_file_name[EquipmentSDKFiles.unit_procedure_work_units.stem]
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_data_class_init_file(multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    expected = EquipmentSDKFiles.data_init.read_text()

    # Act
    actual = multi_api_generator.generate_data_classes_init_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_create_api_client(sdk_generator: SDKGenerator, code_formatter: CodeFormatter):
    # Arrange
    expected = EquipmentSDKFiles.client.read_text()

    # Act
    actual = sdk_generator._generate_api_client_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected, "\n".join(difflib.unified_diff(expected.splitlines(), actual.splitlines()))
