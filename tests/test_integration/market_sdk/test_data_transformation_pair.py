from __future__ import annotations

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from markets.client import MarketClient
from markets.client.data_classes import DateTransformationApply, DateTransformationPairApply


def test_apply(market_client: MarketClient, cognite_client: CogniteClient) -> None:
    # Arrange
    new_date_transformation_pair = DateTransformationPairApply(
        external_id="integration_test:ApplyDateTransformationPair",
        start=[
            DateTransformationApply(
                external_id="integration_test:ApplyDateTransformationPair:Start",
                method="add",
                arguments={"days": 1},
            )
        ],
        end=[
            DateTransformationApply(
                external_id="integration_test:ApplyDateTransformationPair:End",
                method="add",
                arguments={"days": 2},
            ),
        ],
    )

    # Act
    created: dm.InstancesApplyResult | None = None
    try:
        created = market_client.pygen_pool.date_transformation_pairs.apply(new_date_transformation_pair)

        # Assert
        assert len(created.nodes) == 3
        assert len(created.edges) == 2
    finally:
        if created is not None:
            cognite_client.data_modeling.instances.delete(
                created.nodes,
                created.edges,
            )
