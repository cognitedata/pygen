from cognite.client import data_modeling as dm

from cognite.pygen._core.generators import MultiAPIGenerator
from cognite.pygen._generator import CodeFormatter
from tests.constants import OmniFiles, OmniSubFiles
from tests.omni_constants import OMNI_SPACE


def test_generate_connection_edge_a(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["edge"][
        dm.ViewId(OMNI_SPACE, "ConnectionEdgeA", "1")
    ]
    expected = OmniFiles.connection_edge_a.read_text()

    # Act
    actual = api_generator.generate_data_class_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_connection_item_c_edge(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["edge"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemC", "1")
    ]
    expected = OmniFiles.connection_item_c_edge_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_generate_connection_item_c_edge_no_default_space(
    omnisub_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter
):
    # Arrange
    api_generator = omnisub_multi_api_generator.api_by_type_by_view_id["edge"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemC", "1")
    ]
    expected = OmniSubFiles.connection_item_c_edge_data.read_text()

    # Act
    actual = api_generator.generate_data_class_file()
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected
