import pytest
from cognite.client import data_modeling as dm

from cognite.pygen.utils.mock_generator import MockGenerator
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

    generator = MockGenerator([single_view])

    data = generator.generate_mock_data()

    assert len(data) == 1
    view_data = data[0]
    assert len(view_data.node) == 5
    assert len(view_data.edge) == 0


def test_generate_mock_data_multiple_views(omni_data_classes: dict[str, OmniClasses]) -> None:
    views = [
        omni_data_classes[name].view
        for name in [OmniView.connection_item_a, OmniView.connection_item_b, OmniView.connection_item_c]
    ]

    generator = MockGenerator(views)

    data = generator.generate_mock_data()

    assert len(data) == 3
    for view, view_data in zip(views, data):
        assert len(view_data.node) == 5
        edge_type_count = sum(1 for prop in view.properties.values() if isinstance(prop, dm.ConnectionDefinition))
        assert 0 < len(view_data.edge) <= 3 * edge_type_count * len(view_data.node)
