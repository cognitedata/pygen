from cognite.client import data_modeling as dm

from cognite.pygen.utils.mock_generator import MockGenerator
from tests.omni_constants import OmniClasses, OmniView


def test_generate_mock_data_single_view(omni_data_classes: dict[str, OmniClasses]) -> None:
    primitive_nullable = omni_data_classes[OmniView.primitive_nullable].view

    generator = MockGenerator([primitive_nullable])

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
    for view, view_data in zip(views, views):
        assert len(view_data.node) == 5
        edge_type_count = sum(1 for prop in view.properties.values() if isinstance(prop, dm.ConnectionDefinition))
        assert len(view_data.edge) == 3 * edge_type_count
