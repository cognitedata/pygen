import difflib

from cognite.client import data_modeling as dm

from cognite.pygen._core.generators import MultiAPIGenerator
from cognite.pygen._generator import CodeFormatter
from tests.constants import OmniFiles, OmniSubFiles
from tests.omni_constants import OMNI_SPACE


def test_generate_primitive_nullable(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "PrimitiveNullable", "1")
    ]
    expected = OmniFiles.primitive_nullable_api.read_text()

    # Act
    actual = api_generator.generate_api_file(omni_multi_api_generator.client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected, "\n".join(difflib.unified_diff(expected.splitlines(), actual.splitlines()))


def test_generate_primitive_required(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "PrimitiveRequired", "1")
    ]
    expected = OmniFiles.primitive_required_api.read_text()

    # Act
    actual = api_generator.generate_api_file(omni_multi_api_generator.client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_primitive_nullable_list(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "PrimitiveNullableListed", "1")
    ]
    expected = OmniFiles.primitive_nullable_list_api.read_text()

    # Act
    actual = api_generator.generate_api_file(omni_multi_api_generator.client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_primitive_required_list(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "PrimitiveRequiredListed", "1")
    ]
    expected = OmniFiles.primitive_required_list_api.read_text()

    # Act
    actual = api_generator.generate_api_file(omni_multi_api_generator.client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_primitive_with_defaults(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "PrimitiveWithDefaults", "1")
    ]
    expected = OmniFiles.primitive_with_defaults_api.read_text()

    # Act
    actual = api_generator.generate_api_file(omni_multi_api_generator.client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_cdf_external_references(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "CDFExternalReferences", "1")
    ]
    expected = OmniFiles.cdf_external_api.read_text()

    # Act
    actual = api_generator.generate_api_file(omni_multi_api_generator.client_name)
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
    expected = OmniFiles.cdf_external_list_api.read_text()

    # Act
    actual = api_generator.generate_api_file(omni_multi_api_generator.client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_implementation1(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "Implementation1", "1")
    ]
    expected = OmniFiles.implementation_1_api.read_text()

    # Act
    actual = api_generator.generate_api_file(omni_multi_api_generator.client_name)
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
    expected = OmniFiles.implementation_1_non_writeable_api.read_text()

    # Act
    actual = api_generator.generate_api_file(omni_multi_api_generator.client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_sub_interface(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][dm.ViewId(OMNI_SPACE, "SubInterface", "1")]
    expected = OmniFiles.sub_interface.read_text()

    # Act
    actual = api_generator.generate_api_file(omni_multi_api_generator.client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_connection_item_a(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemA", "1")
    ]
    expected = OmniFiles.connection_item_a_api.read_text()

    # Act
    actual = api_generator.generate_api_file(omni_multi_api_generator.client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_connection_item_b(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemB", "1")
    ]
    expected = OmniFiles.connection_item_b_api.read_text()

    # Act
    actual = api_generator.generate_api_file(omni_multi_api_generator.client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_connection_item_c(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemC", "1")
    ]
    expected = OmniFiles.connection_item_c_api.read_text()

    # Act
    actual = api_generator.generate_api_file(omni_multi_api_generator.client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_connection_item_e(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemE", "1")
    ]
    expected = OmniFiles.connection_item_e_api.read_text()

    # Act
    actual = api_generator.generate_api_file(omni_multi_api_generator.client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_connection_item_f(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemF", "1")
    ]
    expected = OmniFiles.connection_item_f_api.read_text()

    # Act
    actual = api_generator.generate_api_file(omni_multi_api_generator.client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_connection_item_g(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemG", "1")
    ]
    expected = OmniFiles.connection_item_g_api.read_text()

    # Act
    actual = api_generator.generate_api_file(omni_multi_api_generator.client_name)
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
    expected = OmniSubFiles.connection_item_a_api.read_text()

    # Act
    actual = api_generator.generate_api_file(omnisub_multi_api_generator.client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected
