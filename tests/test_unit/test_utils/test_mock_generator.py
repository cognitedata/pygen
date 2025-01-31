import pytest
from cognite.client import data_modeling as dm

from cognite.pygen.utils.mock_generator import MockGenerator, ViewMockConfig
from tests.omni_constants import OmniClasses, OmniView


@pytest.mark.parametrize(
    "view_external_id",
    [
        OmniView.primitive_nullable,
        OmniView.primitive_nullable_listed,
        OmniView.primitive_required,
        OmniView.primitive_required_listed,
        OmniView.cdf_external_references,
        OmniView.cdf_external_references_listed,
        OmniView.primitive_with_defaults,
        OmniView.empty,
    ],
)
def test_generate_mock_data_single_view(omni_data_classes: dict[str, OmniClasses], view_external_id: str) -> None:
    single_view = omni_data_classes[view_external_id].view

    generator = MockGenerator([single_view], "sandbox", seed=42)

    data = generator.generate_mock_data()

    assert len(data) == 1
    view_data = data[0]
    assert len(view_data.node) == 5
    assert len(view_data.edge) == 0

    data2 = generator.generate_mock_data()
    view_data2 = data2[0]
    assert view_data.node.dump() == view_data2.node.dump(), "Same seed should produce same data"


def test_generate_mock_data_edge_view(omni_data_classes: dict[str, OmniClasses]) -> None:
    start_node = omni_data_classes[OmniView.connection_item_f].view
    other_view = omni_data_classes[OmniView.connection_item_d].view
    edge_view = omni_data_classes[OmniView.connection_edge_a].view
    end_node = omni_data_classes[OmniView.connection_item_g].view
    alt_end_node = omni_data_classes[OmniView.connection_item_e].view

    generator = MockGenerator([start_node, end_node, edge_view, alt_end_node, other_view], "sandbox", seed=42)

    data = generator.generate_mock_data()

    assert len(data) == 4
    edges_with_data = [edge for edge in data.edges if edge.sources]
    assert edges_with_data, "At least one edge should have data"


@pytest.mark.parametrize(
    "view_external_ids",
    [
        pytest.param(
            [OmniView.connection_item_a, OmniView.connection_item_b, OmniView.connection_item_c], id="Connections"
        ),
        pytest.param(
            [
                OmniView.main_interface,
                OmniView.sub_interface,
                OmniView.implementation1,
                OmniView.implementation2,
                OmniView.implementation1_non_writeable,
                OmniView.dependent_on_non_writable,
            ],
            id="Inheritance",
        ),
    ],
)
def test_generate_mock_data_multiple_views(
    omni_data_classes: dict[str, OmniClasses], view_external_ids: list[str]
) -> None:
    node_count = 5
    max_edge_per_type = 3

    view_by_id = {omni_data_classes[name].view.as_id(): omni_data_classes[name].view for name in view_external_ids}

    generator = MockGenerator(list(view_by_id.values()), "sandbox", seed=42)

    data = generator.generate_mock_data(node_count=node_count, max_edge_per_type=max_edge_per_type)

    assert len(data) == len(view_by_id)
    for view_data in data:
        assert len(view_data.node) == node_count
        view = view_by_id[view_data.view_id]
        edge_type_count = sum(1 for prop in view.properties.values() if isinstance(prop, dm.ConnectionDefinition))
        if edge_type_count == 0:
            assert len(view_data.edge) == 0
        else:
            assert 0 < len(view_data.edge) <= max_edge_per_type * edge_type_count * len(view_data.node)


def test_generate_mock_data_skip_interfaces(omni_data_classes: dict[str, OmniClasses]) -> None:
    view_external_ids = [
        OmniView.main_interface,
        OmniView.sub_interface,
        OmniView.implementation1,
        OmniView.implementation2,
        OmniView.implementation1_non_writeable,
        OmniView.dependent_on_non_writable,
    ]
    interface_count = 2

    view_by_id = {
        omni_data_classes[external_id].view.as_id(): omni_data_classes[external_id].view
        for external_id in view_external_ids
    }

    generator = MockGenerator(list(view_by_id.values()), "sandbox", seed=42, skip_interfaces=True)

    data = generator.generate_mock_data(node_count=5, max_edge_per_type=3, null_values=0.25)

    assert len(data) == len(view_by_id) - interface_count
    for view_data in data:
        assert len(view_data.node) == 5
        view = view_by_id[view_data.view_id]
        edge_type_count = sum(1 for prop in view.properties.values() if isinstance(prop, dm.ConnectionDefinition))
        if edge_type_count == 0:
            assert len(view_data.edge) == 0
        else:
            max_edge_count = 3 * edge_type_count * len(view_data.node)
            assert 0 < len(view_data.edge)
            assert len(view_data.edge) <= max_edge_count


def test_generate_mock_data_customized(omni_data_classes: dict[str, OmniClasses]) -> None:
    views = [class_.view for class_ in omni_data_classes.values() if class_.view.used_for != "edge"]
    primitive_required = omni_data_classes[OmniView.primitive_required].view
    node_count = 3
    default_config = ViewMockConfig(
        node_count=node_count,
        property_types={dm.Float64: lambda count: [0.5] * count, dm.Text: lambda count: ["Hello"] * count},
    )
    view_configs = {
        primitive_required.as_id(): ViewMockConfig(
            node_count=node_count,
            properties={
                "int64": lambda count: [42] * count,
            },
            property_types={dm.Float64: lambda count: [0.5] * count, dm.Text: lambda count: ["there"] * count},
        )
    }
    generator = MockGenerator(views, "sandbox", view_configs=view_configs, default_config=default_config, seed=42)

    data = generator.generate_mock_data(node_count=node_count, max_edge_per_type=3)

    assert len(data.nodes) == len(views) * node_count
    primitive_required_data = next(d for d in data if d.view_id == primitive_required.as_id())

    other_values = [
        source.properties["float64"]
        for node in primitive_required_data.node
        for source in node.sources
        if source.properties["float64"] != 0.5
    ]
    assert len(other_values) == 0, f"Unexpected values: {other_values}"
    other_values = [
        source.properties["text"]
        for node in primitive_required_data.node
        for source in node.sources
        if source.properties["text"] != "there"
    ]
    assert len(other_values) == 0, f"Unexpected values: {other_values}"


def test__to_leaf_children_by_parent(omni_data_classes: dict[str, OmniClasses]) -> None:
    views = [
        omni_data_classes[name].view
        for name in [
            OmniView.main_interface,
            OmniView.sub_interface,
            OmniView.implementation1,
            OmniView.implementation2,
        ]
    ]
    expected = {
        OmniView.main_interface: {OmniView.implementation1, OmniView.implementation2},
        OmniView.sub_interface: {OmniView.implementation1, OmniView.implementation2},
    }

    leaf_children_by_parent = MockGenerator._to_leaf_children_by_parent(views)
    actual = {k.external_id: {vv.external_id for vv in v} for k, v in leaf_children_by_parent.items()}

    assert actual == expected


def test_generate_mock_data_seed_per_view(omni_data_classes: dict[str, OmniClasses]) -> None:
    seed = 42
    generator1 = MockGenerator([omni_data_classes[OmniView.primitive_required].view], "sandbox", seed=seed)
    generator2 = MockGenerator(
        [omni_data_classes[ext_id].view for ext_id in (OmniView.primitive_nullable, OmniView.primitive_required)],
        "sandbox",
        seed=seed,
    )
    data1 = generator1.generate_mock_data(node_count=1, null_values=0.0)
    data2 = generator2.generate_mock_data(node_count=1, null_values=0.0)

    assert data1[0].node[0].sources[0].properties == data2[1].node[0].sources[0].properties
    assert data2[1].node[0].sources[0].properties != data2[0].node[0].sources[0].properties


@pytest.mark.skip("Temporarily disabled due to diff Windows/Linux behavior")
def test_generate_faker_mock_data_seed_per_view(omni_data_classes: dict[str, OmniClasses]) -> None:
    seed = 42
    generator1 = MockGenerator(
        [omni_data_classes[OmniView.primitive_required].view], "sandbox", seed=seed, default_config="faker"
    )
    generator2 = MockGenerator(
        [omni_data_classes[ext_id].view for ext_id in (OmniView.primitive_nullable, OmniView.primitive_required)],
        "sandbox",
        seed=seed,
        default_config="faker",
    )
    data1 = generator1.generate_mock_data(node_count=1, null_values=0.0)
    data2 = generator2.generate_mock_data(node_count=1, null_values=0.0)

    assert data1[0].node[0].sources[0].properties == data2[1].node[0].sources[0].properties
    assert data2[1].node[0].sources[0].properties != data2[0].node[0].sources[0].properties
