from cognite.client.data_classes.aggregations import AggregatedNumberedValue
from cognite.client.data_classes.data_modeling import Node, NodeListWithCursor
from cognite.client.data_classes.data_modeling.instances import Properties
from cognite.client.data_classes.data_modeling.query import NodeOrEdgeResultSetExpression, Query, QueryResult
from cognite.client.testing import monkeypatch_cognite_client
from omni import OmniClient
from omni import data_classes as odc


class TestIterateMethod:
    def test_api_call_chunk_size_limit(self) -> None:
        def query_call(query: Query) -> QueryResult:
            assert "0" in query.with_
            zero = query.with_["0"]
            assert isinstance(zero, NodeOrEdgeResultSetExpression)
            assert zero.limit == 1
            return self.single_item_a_result

        with monkeypatch_cognite_client() as client:
            client.data_modeling.instances.aggregate.return_value = AggregatedNumberedValue("externalId", 4)
            client.data_modeling.instances.query.side_effect = query_call
            pygen = OmniClient(client)

        for _ in pygen.connection_item_a.iterate(chunk_size=1, limit=2):
            ...

        assert client.data_modeling.instances.query.call_count == 2

    single_item_a_result = QueryResult(
        {
            "0": NodeListWithCursor(
                [
                    Node(
                        space="my_instances",
                        external_id="my_instance",
                        version=42,
                        last_updated_time=1,
                        created_time=0,
                        deleted_time=None,
                        type=None,
                        properties=Properties(
                            {
                                odc.ConnectionItemA._view_id: {
                                    "name": "Item A",
                                }
                            }
                        ),
                    )
                ],
                cursor="abc",
            )
        }
    )
