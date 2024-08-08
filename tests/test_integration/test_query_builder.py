from __future__ import annotations

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import filters

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from omni import OmniClient
    from omni import data_classes as dc
    from omni._api._core import QueryBuilder, QueryStep
else:
    from omni_pydantic_v1 import OmniClient
    from omni_pydantic_v1 import data_classes as dc
    from omni_pydantic_v1._api._core import QueryBuilder, QueryStep


class TestQueryBuilder:
    def test_list_with_reverse_direct_relations(
        self, cognite_client: CogniteClient, omni_views: dict[str, dm.View]
    ) -> None:
        # Arrange
        item_e = omni_views["ConnectionItemE"].as_id()
        item_d = omni_views["ConnectionItemD"].as_id()
        builder = QueryBuilder(dc.ConnectionItemEList)
        builder.append(
            QueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=filters.HasData(views=[item_e]),
                ),
                dc.ConnectionItemE,
                max_retrieve_limit=10,
            )
        )
        from_ = builder.get_from()
        builder.append(
            QueryStep(
                builder.create_name(from_),
                dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=dm.PropertyId(
                        item_d,
                        "directSingle",
                    ),
                ),
                dc.ConnectionItemD,
            )
        )

        # Act
        result = builder.execute(cognite_client)

        # Assert
        assert isinstance(result, dc.ConnectionItemEList)
        assert builder[0].total_retrieved == 1

    def test_list_with_direct_relations(
        self, cognite_client: CogniteClient, omni_views: dict[str, dm.View], omni_client: OmniClient
    ) -> None:
        # Arrange
        item_a = omni_views["ConnectionItemA"].as_id()
        builder = QueryBuilder(dc.ConnectionItemAList)

        builder.append(
            QueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=filters.HasData(views=[item_a]),
                ),
                dc.ConnectionItemA,
            )
        )
        from_a = builder.get_from()
        builder.append(
            QueryStep(
                builder.create_name(from_a),
                dm.query.NodeResultSetExpression(
                    from_=from_a,
                    through=dm.PropertyId(
                        item_a,
                        "otherDirect",
                    ),
                ),
                dc.ConnectionItemCNode,
            )
        )
        edge_name = builder.create_name(from_a)
        builder.append(
            QueryStep(
                edge_name,
                dm.query.EdgeResultSetExpression(
                    from_=from_a,
                    chain_to="destination",
                    direction="outwards",
                ),
            )
        )
        builder.append(
            QueryStep(
                builder.create_name(edge_name),
                dm.query.NodeResultSetExpression(
                    from_=edge_name,
                ),
                dc.ConnectionItemB,
            )
        )

        # Act
        result = builder.execute(cognite_client)

        # Assert
        assert isinstance(result, dc.ConnectionItemAList)
        assert builder[0].total_retrieved == len(result) > 0

        actual_direct_set = sum(1 for item in result if item.other_direct)
        assert builder[1].total_retrieved == actual_direct_set

        actual_edge_set = sum(len(item.outwards or []) for item in result)
        assert builder[2].total_retrieved == actual_edge_set

        unique_destination_set = set(
            destination.as_id()
            for item in result
            for destination in item.outwards
            if isinstance(destination, dc.ConnectionItemB)
        )
        assert builder[3].total_retrieved == len(unique_destination_set)
