from __future__ import annotations

from datetime import date

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from tests.constants import IS_PYDANTIC_V1

if IS_PYDANTIC_V1:
    from markets_pydantic_v1.client import MarketClient
    from markets_pydantic_v1.client.data_classes import PygenBidApply
else:
    from markets.client import MarketClient
    from markets.client.data_classes import PygenBidApply


def test_apply_with_date(market_client: MarketClient, cognite_client: CogniteClient) -> None:
    # Arrange
    bid = PygenBidApply(
        external_id="pygenbid:intergation_test:test_apply_with_date",
        minimum_price=1.0,
        price_premium=10.0,
        date=date(1925, 8, 3),
    )

    # Act
    created: None | dm.InstancesApplyResult = None
    try:
        created = market_client.pygen_pool.pygen_bids.apply(bid, replace=True)

        # Assert
        assert len(created.nodes) == 1
    finally:
        if created is not None:
            market_client.pygen_pool.pygen_bids.delete(bid.external_id)


def test_apply_without_date(market_client: MarketClient) -> None:
    # Arrange
    bid = PygenBidApply(
        external_id="pygenbid:intergation_test:test_apply_without_date",
        minimum_price=1.0,
        name="test_apply_without_date",
    )

    # Act
    created: None | dm.InstancesApplyResult = None
    try:
        created = market_client.pygen_pool.pygen_bids.apply(bid, replace=True)

        # Assert
        assert len(created.nodes) == 1
    finally:
        if created is not None:
            market_client.pygen_pool.pygen_bids.delete(bid.external_id)
