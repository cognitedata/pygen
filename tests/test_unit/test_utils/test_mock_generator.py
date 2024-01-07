from cognite.pygen.utils.mock_generator import MockGenerator
from tests.omni_constants import OmniClasses


def test_generate_mock_data_single_view(omni_data_classes: dict[str, OmniClasses]) -> None:
    primitive_nullable = omni_data_classes["PrimitiveNullable"].view

    generator = MockGenerator([primitive_nullable])

    data = generator.generate_mock_data()

    assert len(data) == 1
    view_data = data[0]
    assert len(view_data.node) == 5
    assert len(view_data.edge) == 0
