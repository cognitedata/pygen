import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import filters
from omni import data_classes as dc
from omni.data_classes._core.query import QueryBuilder, QueryStep, QueryUnpacker, ViewPropertyId


class TestQueryBuilder:
    def test_query_with_reverse_direct_relations(
        self, cognite_client: CogniteClient, omni_views: dict[str, dm.View]
    ) -> None:
        # Arrange
        item_e = omni_views["ConnectionItemE"].as_id()
        item_d = omni_views["ConnectionItemD"].as_id()

        builder = QueryBuilder()
        builder.append(
            QueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=filters.HasData(views=[item_e]),
                ),
                view_id=item_e,
                max_retrieve_limit=-1,
            )
        )
        from_ = builder.get_from()

        builder.append(
            QueryStep(
                builder.create_name(from_),
                dm.query.NodeResultSetExpression(
                    from_=from_,
                    filter=filters.HasData(views=[item_d]),
                    through=item_d.as_property_ref("directSingle"),
                    direction="inwards",
                ),
                view_id=item_d,
                connection_property=ViewPropertyId(item_e, "directReverseSingle"),
            )
        )
        builder.append(
            QueryStep(
                builder.create_name(from_),
                dm.query.NodeResultSetExpression(
                    from_=from_,
                    filter=filters.HasData(views=[item_d]),
                    through=item_d.as_property_ref("directMulti"),
                    direction="inwards",
                ),
                view_id=item_d,
                connection_type="reverse-list",
                connection_property=ViewPropertyId(item_e, "directReverseMulti"),
            )
        )
        # Act
        builder.execute_query(cognite_client)
        unpacked = QueryUnpacker(builder).unpack()
        result = dc.ConnectionItemEList([dc.ConnectionItemE.model_validate(item) for item in unpacked])

        # Assert
        assert isinstance(result, dc.ConnectionItemEList)
        assert builder[0].total_retrieved == len(result) > 0

        actual_direct_set = sum(1 for item in result if item.direct_reverse_single)
        assert builder[1].total_retrieved == actual_direct_set > 0

        actual_direct_multi_set = sum(len(item.direct_reverse_multi or []) for item in result)
        assert 0 < builder[2].total_retrieved <= actual_direct_multi_set

    def test_query_with_edge_and_direct_relation(
        self, cognite_client: CogniteClient, omni_views: dict[str, dm.View]
    ) -> None:
        # Arrange
        item_a = omni_views["ConnectionItemA"].as_id()

        builder = QueryBuilder()

        builder.append(
            QueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=filters.HasData(views=[item_a]),
                ),
                view_id=dc.ConnectionItemA._view_id,
                max_retrieve_limit=5,
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
                view_id=dc.ConnectionItemCNode._view_id,
                connection_property=ViewPropertyId(item_a, "otherDirect"),
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
                connection_property=ViewPropertyId(item_a, "outwards"),
            )
        )
        builder.append(
            QueryStep(
                builder.create_name(edge_name),
                dm.query.NodeResultSetExpression(
                    from_=edge_name,
                ),
                view_id=dc.ConnectionItemB._view_id,
                connection_property=ViewPropertyId(item_a, "end_node"),
            )
        )

        # Act
        builder.execute_query(cognite_client)
        unpacked = QueryUnpacker(builder).unpack()
        result = dc.ConnectionItemAList([dc.ConnectionItemA.model_validate(item) for item in unpacked])

        # Assert
        assert isinstance(result, dc.ConnectionItemAList)
        assert builder[0].total_retrieved > 0
        assert builder[0].total_retrieved == len(result)

        actual_direct_set = sum(1 for item in result if item.other_direct)
        # The other direct can be the same for multiple items
        assert 0 < builder[1].total_retrieved <= actual_direct_set

        actual_edge_set = sum(len(item.outwards or []) for item in result)
        assert builder[2].total_retrieved == actual_edge_set > 0

        unique_destination_set = set(
            destination.as_id()
            for item in result
            for destination in item.outwards or []
            if isinstance(destination, dc.ConnectionItemB)
        )
        assert builder[3].total_retrieved == len(unique_destination_set) > 0

    @pytest.mark.skip(reason="Missing test data")
    def test_query_across_edge_with_properties(
        self, cognite_client: CogniteClient, omni_views: dict[str, dm.View], omni_client
    ) -> None:
        item_f = omni_views["ConnectionItemF"].as_id()
        builder = QueryBuilder()
        builder.append(
            QueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=filters.HasData(views=[item_f]),
                ),
                view_id=item_f,
                max_retrieve_limit=1,
            )
        )
        from_f = builder.get_from()
        edge_view = omni_views["ConnectionEdgeA"].as_id()
        builder.append(
            QueryStep(
                builder.create_name(from_f),
                dm.query.EdgeResultSetExpression(
                    from_=from_f,
                    chain_to="destination",
                    direction="outwards",
                    filter=filters.HasData(views=[edge_view]),
                ),
                view_id=edge_view,
                connection_property=ViewPropertyId(item_f, "outwardsMulti"),
            )
        )
        item_g = omni_views["ConnectionItemG"].as_id()
        from_edge = builder.get_from()
        builder.append(
            QueryStep(
                builder.create_name(from_edge),
                dm.query.NodeResultSetExpression(
                    from_=from_edge,
                    filter=filters.HasData(views=[item_g]),
                ),
                view_id=dc.ConnectionItemG._view_id,
                connection_property=ViewPropertyId(edge_view, "end_node"),
            )
        )

        builder.execute_query(cognite_client)
        unpacked = QueryUnpacker(builder).unpack()
        result = dc.ConnectionItemFList([dc.ConnectionItemF.model_validate(item) for item in unpacked])

        assert isinstance(result, dc.ConnectionItemFList)
        assert builder[0].total_retrieved == len(result) > 0

        actual_edge_set = sum(len(item.outwards_multi or []) for item in result)
        assert builder[1].total_retrieved == actual_edge_set > 0

        actual_node_set = sum(
            1 for item in result for edge in item.outwards_multi or [] if isinstance(edge.end_node, dc.ConnectionItemG)
        )
        assert builder[2].total_retrieved == actual_node_set > 0
