from cognite.client import data_modeling as dm

from cognite.pygen._core.generators import MultiAPIGenerator
from cognite.pygen._generator import CodeFormatter
from tests.constants import OmniFiles
from tests.omni_constants import OMNI_SPACE


def test_generate_connection_item_a(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemA", "1")
    ]
    expected = OmniFiles.connection_item_a_query.read_text()

    # Act
    actual = api_generator.generate_api_query_file(omni_multi_api_generator.client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_connection_item_b(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemB", "1")
    ]
    expected = OmniFiles.connection_item_b_query.read_text()

    # Act
    actual = api_generator.generate_api_query_file(omni_multi_api_generator.client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_connection_item_c(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemC", "1")
    ]
    expected = OmniFiles.connection_item_c_query.read_text()

    # Act
    actual = api_generator.generate_api_query_file(omni_multi_api_generator.client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_connection_item_d(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemD", "1")
    ]
    expected = OmniFiles.connection_item_d_query.read_text()

    # Act
    actual = api_generator.generate_api_query_file(omni_multi_api_generator.client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_connection_item_f(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemF", "1")
    ]
    expected = OmniFiles.connection_item_f_query.read_text()

    # Act
    actual = api_generator.generate_api_query_file(omni_multi_api_generator.client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_connection_item_g(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemG", "1")
    ]
    expected = OmniFiles.connection_item_g_query.read_text()

    # Act
    actual = api_generator.generate_api_query_file(omni_multi_api_generator.client_name)
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected
