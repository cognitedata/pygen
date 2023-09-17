from cognite.client import data_modeling as dm

from cognite.pygen._core.data_classes import DataClass
from cognite.pygen._core.generators import APIGenerator
from cognite.pygen.config import PygenConfig


def test_is_date_field(bid_view: dm.View, market_view: dm.View, pygen_config: PygenConfig) -> None:
    # Arrange
    market_data_class = DataClass.from_view(market_view, pygen_config)

    # Act
    gen = APIGenerator(bid_view, pygen_config)
    gen.data_class.update_fields(bid_view.properties, {market_view.as_id(): market_data_class}, pygen_config)

    # Assert
    assert gen.data_class.has_time_field
