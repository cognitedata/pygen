import random

from cognite.client import data_modeling as dm

from cognite.pygen._core.generators import MultiAPIGenerator
from cognite.pygen._generator import CodeFormatter
from tests.constants import OMNI_TYPED, OmniTypedFiles
from tests.omni_constants import OMNI_SPACE


def test_generate_typed_classes(omni_multi_api_generator_composition: MultiAPIGenerator, code_formatter: CodeFormatter):
    # Arrange
    view_external_ids = OMNI_TYPED.typed_classes
    expected = OmniTypedFiles.typed.read_text()

    # Act
    actual = omni_multi_api_generator_composition.generate_typed_classes_file(
        include={dm.ViewId(OMNI_SPACE, external_id, "1") for external_id in view_external_ids}
    )
    actual = code_formatter.format_code(actual)

    # Assert
    assert actual == expected


def test_topological_sort(omni_multi_api_generator: MultiAPIGenerator):
    sorted_data_classes = tuple([d.read_name for d in omni_multi_api_generator.data_classes_topological_order])
    original_nodes = omni_multi_api_generator.api_by_type_by_view_id["node"]
    original_edges = omni_multi_api_generator.api_by_type_by_view_id["edge"]
    node_views = list(original_nodes.keys())
    edge_views = list(original_edges.keys())
    random.shuffle(node_views)
    random.shuffle(edge_views)
    try:
        omni_multi_api_generator.api_by_type_by_view_id["node"] = {v: original_nodes[v] for v in node_views}
        omni_multi_api_generator.api_by_type_by_view_id["edge"] = {v: original_edges[v] for v in edge_views}

        new_sorted_data_classes = tuple([d.read_name for d in omni_multi_api_generator.data_classes_topological_order])

        assert sorted_data_classes == new_sorted_data_classes
    finally:
        omni_multi_api_generator.api_by_type_by_view_id["node"] = original_nodes
        omni_multi_api_generator.api_by_type_by_view_id["edge"] = original_edges
