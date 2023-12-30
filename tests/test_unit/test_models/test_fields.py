from cognite.client import data_modeling as dm

from cognite.pygen._core.generators import MultiAPIGenerator


def test_has_date_field(omni_multi_api_generator: MultiAPIGenerator) -> None:
    # Arrange
    api_generator = omni_multi_api_generator.api_by_view_id[dm.ViewId("pygen-models", "PrimitiveRequired", "1")]

    # Assert
    assert api_generator.data_class.has_primitive_field_of_type(dm.Date)
