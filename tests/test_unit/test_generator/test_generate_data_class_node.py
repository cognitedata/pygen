from cognite.client import data_modeling as dm

from cognite.pygen._core.generators import MultiAPIGenerator
from cognite.pygen._generator import CodeFormatter
from tests.constants import CogniteCoreFiles, OmniFiles, OmniSubFiles, WindTurbineFiles
from tests.omni_constants import OMNI_SPACE


def test_generate_primitive_nullable(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "PrimitiveNullable", "1")
    ]
    expected = OmniFiles.primitive_nullable_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_primitive_required(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "PrimitiveRequired", "1")
    ]
    expected = OmniFiles.primitive_required_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_primitive_nullable_list(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "PrimitiveNullableListed", "1")
    ]
    expected = OmniFiles.primitive_nullable_list_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_primitive_required_list(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "PrimitiveRequiredListed", "1")
    ]
    expected = OmniFiles.primitive_required_list_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_primitive_with_defaults(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "PrimitiveWithDefaults", "1")
    ]
    expected = OmniFiles.primitive_with_defaults_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_cdf_external_references(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "CDFExternalReferences", "1")
    ]
    expected = OmniFiles.cdf_external_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_cdf_external_references_list(
    omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter
):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "CDFExternalReferencesListed", "1")
    ]
    expected = OmniFiles.cdf_external_list_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_implementation1(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "Implementation1", "1")
    ]
    expected = OmniFiles.implementation_1_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_implementation1_non_writable(
    omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter
):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "Implementation1NonWriteable", "1")
    ]
    expected = OmniFiles.implementation_1_non_writeable_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_connection_item_a(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemA", "1")
    ]
    expected = OmniFiles.connection_item_a_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_connection_item_c(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemC", "1")
    ]
    expected = OmniFiles.connection_item_c_node_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_connection_item_d(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemD", "1")
    ]
    expected = OmniFiles.connection_item_d_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_connection_item_e(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemE", "1")
    ]
    expected = OmniFiles.connection_item_e_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_connection_item_f(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemF", "1")
    ]
    expected = OmniFiles.connection_item_f_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_connection_item_a_no_default_space(
    omnisub_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter
):
    # Arrange
    api_generator = omnisub_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemA", "1")
    ]
    expected = OmniSubFiles.connection_item_a_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_cognite_asset(core_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = core_multi_api_generator.api_by_type_by_view_id["node"][dm.ViewId("cdf_cdm", "CogniteAsset", "v1")]
    expected = CogniteCoreFiles.data_cognite_asset.read_text()

    # Act
    actual = api_generator.generate_data_class_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_sensor_time_series(
    turbine_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter
) -> None:
    # Arrange
    api_generator = turbine_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId("sp_pygen_power", "SensorTimeSeries", "1")
    ]
    expected = WindTurbineFiles.data_sensor_time_series.read_text()

    # Act
    actual = api_generator.generate_data_class_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_metmast(turbine_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter) -> None:
    # Arrange
    api_generator = turbine_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId("sp_pygen_power", "Metmast", "1")
    ]
    expected = WindTurbineFiles.data_metmast.read_text()

    # Act
    actual = api_generator.generate_data_class_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected
