from cognite.client import data_modeling as dm

from cognite.pygen._core.generators import APIGenerator


def test_is_date_field(bid_view: dm.View) -> None:
    # Arrange
    gen = APIGenerator(bid_view, "doesnt matter")

    # Assert
    assert gen.fields.has_date
