from __future__ import annotations

from cognite.client import CogniteClient

from tests.constants import IS_PYDANTIC_V1

if IS_PYDANTIC_V1:
    from markets_pydantic_v1.client import MarketClient
    from markets_pydantic_v1.client.data_classes import PygenPool
else:
    from markets.client import MarketClient
    from markets.client.data_classes import PygenPool


def test_list_empty_to_pandas(market_client: MarketClient, cognite_client: CogniteClient) -> None:
    # Act
    market_df = market_client.pygen_pool.pygen_pool.list().to_pandas()

    # Assert
    assert market_df.empty
    if IS_PYDANTIC_V1:
        assert sorted(market_df.columns) == sorted(PygenPool.__fields__)
    else:
        assert sorted(market_df.columns) == sorted(PygenPool.model_fields)
