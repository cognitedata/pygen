from cognite.client import data_modeling as dm

from cognite.pygen._core.generators import MultiAPIGenerator
from cognite.pygen._generator import CodeFormatter
from tests.constants import OmniFiles, OmniSubFiles
from tests.omni_constants import OMNI_SPACE


def test_generate_connection_item_a(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemA", "1")
    ]
    filepath_by_name = {f.stem: f for f in OmniFiles.connection_item_a_edge_apis}

    # Act
    for name, actual in api_generator.generate_edge_api_files(omni_multi_api_generator.client_name):
        actual = code_formatter.format_code(actual)

        # Assert
        assert actual == filepath_by_name[name].read_text()


def test_generate_connection_item_b(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemB", "1")
    ]
    filepath_by_name = {f.stem: f for f in OmniFiles.connection_item_b_edge_apis}

    # Act
    for name, actual in api_generator.generate_edge_api_files(omni_multi_api_generator.client_name):
        actual = code_formatter.format_code(actual)

        # Assert
        assert actual == filepath_by_name[name].read_text()


def test_generate_connection_item_c(omni_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    api_generator = omni_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemC", "1")
    ]
    filepath_by_name = {f.stem: f for f in OmniFiles.connection_item_c_edge_apis}

    # Act
    for name, actual in api_generator.generate_edge_api_files(omni_multi_api_generator.client_name):
        actual = code_formatter.format_code(actual)

        # Assert
        assert actual == filepath_by_name[name].read_text()


def test_generate_connection_item_a_no_default_space(
    omnisub_multi_api_generator: MultiAPIGenerator, code_formatter: CodeFormatter
):
    # Arrange
    api_generator = omnisub_multi_api_generator.api_by_type_by_view_id["node"][
        dm.ViewId(OMNI_SPACE, "ConnectionItemA", "1")
    ]
    filepath_by_name = {f.stem: f for f in OmniSubFiles.connection_item_a_edge_apis}

    # Act
    for name, actual in api_generator.generate_edge_api_files(omnisub_multi_api_generator.client_name):
        actual = code_formatter.format_code(actual)

        # Assert
        assert actual == filepath_by_name[name].read_text()
