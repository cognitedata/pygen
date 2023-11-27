from __future__ import annotations

from cognite.client import CogniteClient

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from markets.client import MarketClient
    from markets.client.data_classes import DateTransformationApply, DateTransformationPairApply, ResourcesApplyResult
else:
    from markets_pydantic_v1.client import MarketClient
    from markets_pydantic_v1.client.data_classes import (
        DateTransformationApply,
        DateTransformationPairApply,
        ResourcesApplyResult,
    )


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
    created: ResourcesApplyResult | None = None
    try:
        # Act
        created = market_client.pygen_pool.date_transformation_pair.apply(new_date_transformation_pair)

        # Assert
        assert len(created.nodes) == 2
        assert len(created.edges) == 2

        # Act
        retrieved = market_client.pygen_pool.date_transformation_pair.retrieve(new_date_transformation_pair.external_id)

        # Assert
        assert retrieved.external_id == new_date_transformation_pair.external_id
        assert retrieved.start[0] == new_date_transformation_pair.start[0].external_id
        assert retrieved.end[0] == new_date_transformation_pair.end[0].external_id
    finally:
        if created is not None:
            cognite_client.data_modeling.instances.delete(
                created.nodes.as_ids(),
                created.edges.as_ids(),
            )
