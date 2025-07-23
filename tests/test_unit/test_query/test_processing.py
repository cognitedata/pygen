from collections.abc import Mapping
from typing import Literal

import pytest
from cognite.client.data_classes.data_modeling import (
    DirectRelationReference,
    Node,
    NodeListWithCursor,
    ViewId,
    filters,
    query,
)
from cognite.client.data_classes.data_modeling.instances import Edge, EdgeListWithCursor, Properties

from cognite.pygen._query.processing import QueryUnpacker
from cognite.pygen._query.step import QueryResultStep, ViewPropertyId

DEFAULT_INSTANCE_ARGS: Mapping = dict(
    version=1,
    last_updated_time=0,
    created_time=0,
    deleted_time=None,
)


@pytest.fixture(scope="module")
def node_with_edge_results() -> list[QueryResultStep]:
    view_id = ViewId("sp_pygen_models", "ConnectionItemA", "1")
    results = [
        QueryResultStep(
            results=NodeListWithCursor(
                [
                    Node(
                        space="test_space",
                        external_id="jennifer",
                        properties=Properties({view_id: {"name": "Jennifer"}}),
                        type=None,
                        **DEFAULT_INSTANCE_ARGS,
                    )
                ],
                cursor=None,
            ),
            name="0",
            expression=query.NodeResultSetExpression(filter=filters.HasData(views=[view_id])),
            view_id=view_id,
        ),
        QueryResultStep(
            results=EdgeListWithCursor(
                [
                    Edge(
                        space="test_space",
                        external_id="edge_external_id",
                        start_node=DirectRelationReference("test_space", "jennifer"),
                        end_node=DirectRelationReference("test_space", "brenda"),
                        type=DirectRelationReference("sp_pygen_models", "outwards"),
                        properties=None,
                        **DEFAULT_INSTANCE_ARGS,
                    )
                ],
                cursor=None,
            ),
            name="0_1",
            expression=query.EdgeResultSetExpression(from_="0", chain_to="source", direction="outwards"),
            selected_properties=None,
            connection_property=ViewPropertyId(view_id, "outwards"),
        ),
    ]
    return results


@pytest.fixture(scope="module")
def node_with_edge_and_node_results(node_with_edge_results: list[QueryResultStep]) -> list[QueryResultStep]:
    view_id = ViewId("sp_pygen_models", "ConnectionItemB", "1")
    results = node_with_edge_results.copy()
    results.append(
        QueryResultStep(
            results=NodeListWithCursor(
                [
                    Node(
                        space="test_space",
                        external_id="brenda",
                        properties=Properties({view_id: {"name": "Brenda"}}),
                        type=None,
                        **DEFAULT_INSTANCE_ARGS,
                    )
                ],
                cursor=None,
            ),
            name="0_1_1",
            expression=query.NodeResultSetExpression(filter=filters.HasData(views=[view_id]), from_="0_1"),
            view_id=view_id,
            connection_property=ViewPropertyId(view_id, "endNode"),
        ),
    )
    return results


class TestQueryUnpacker:
    def test_empty_response(self) -> None:
        unpacker = QueryUnpacker(steps=[])

        result = unpacker.unpack()

        assert result == [], "Expected empty list for empty response"

    @pytest.mark.parametrize(
        "edges, expected_outwards",
        [
            pytest.param("skip", [{"space": "test_space", "externalId": "brenda"}], id="Skip edges"),
            pytest.param(
                "identifier", [{"space": "test_space", "externalId": "edge_external_id"}], id="Only edge identifier"
            ),
            pytest.param(
                "include",
                [
                    {
                        "data_record": {"createdTime": 0, "lastUpdatedTime": 0, "version": 1},
                        "edge_type": {"externalId": "outwards", "space": "sp_pygen_models"},
                        "endNode": {"externalId": "brenda", "space": "test_space"},
                        "externalId": "edge_external_id",
                        "space": "test_space",
                        "startNode": {"externalId": "jennifer", "space": "test_space"},
                    }
                ],
                id="Include edges",
            ),
        ],
    )
    def test_unpack_with_edge_leaf_step(
        self,
        edges: Literal["skip", "identifier", "include"],
        expected_outwards: list[dict],
        node_with_edge_results: list[QueryResultStep],
    ) -> None:
        unpacker = QueryUnpacker(steps=node_with_edge_results, edges=edges)

        unpacked = unpacker.unpack()

        assert len(unpacked) == 1
        item = unpacked[0]
        # Bug which will be fixed in PR #494
        item.pop("instanceType", None)
        for outwards in item.get("outwards", []):
            outwards.pop("instanceType", None)

        assert isinstance(item, dict)
        assert item == {
            "space": "test_space",
            "externalId": "jennifer",
            "name": "Jennifer",
            "data_record": {"createdTime": 0, "lastUpdatedTime": 0, "version": 1},
            "outwards": expected_outwards,
        }

    @pytest.mark.parametrize(
        "edges, edge_connections, expected_outwards",
        [
            pytest.param(
                "skip",
                "object",
                [
                    {
                        "space": "test_space",
                        "externalId": "brenda",
                        "data_record": {"createdTime": 0, "lastUpdatedTime": 0, "version": 1},
                        "name": "Brenda",
                    }
                ],
                id="Skip edges",
            ),
            pytest.param(
                "identifier",
                "object",
                [
                    {
                        "space": "test_space",
                        "externalId": "edge_external_id",
                        "endNode": {
                            "space": "test_space",
                            "externalId": "brenda",
                            "data_record": {"createdTime": 0, "lastUpdatedTime": 0, "version": 1},
                            "name": "Brenda",
                        },
                    }
                ],
                id="Only edge identifier",
            ),
            pytest.param(
                "include",
                "list",
                [
                    {
                        "data_record": {"createdTime": 0, "lastUpdatedTime": 0, "version": 1},
                        "edge_type": {"externalId": "outwards", "space": "sp_pygen_models"},
                        "endNode": [
                            {
                                "data_record": {"createdTime": 0, "lastUpdatedTime": 0, "version": 1},
                                "externalId": "brenda",
                                "name": "Brenda",
                                "space": "test_space",
                            }
                        ],
                        "externalId": "edge_external_id",
                        "space": "test_space",
                        "startNode": {"externalId": "jennifer", "space": "test_space"},
                    }
                ],
                id="Include edges",
            ),
        ],
    )
    def test_unpack_with_node_leaf_step(
        self,
        edges: Literal["skip", "identifier", "include"],
        edge_connections: Literal["object", "list"],
        expected_outwards: list[dict],
        node_with_edge_and_node_results: list[QueryResultStep],
    ) -> None:
        unpacker = QueryUnpacker(steps=node_with_edge_and_node_results, edges=edges, edge_connections=edge_connections)

        unpacked = unpacker.unpack()

        assert len(unpacked) == 1
        item = unpacked[0]
        # Bug which will be fixed in PR #494
        item.pop("instanceType", None)
        for outwards in item.get("outwards", []):
            outwards.pop("instanceType", None)
            if "endNode" in outwards:
                end_node = outwards["endNode"]
                if isinstance(end_node, dict):
                    end_node.pop("instanceType", None)
                elif isinstance(end_node, list):
                    for node in end_node:
                        node.pop("instanceType", None)

        assert isinstance(item, dict)
        assert item == {
            "space": "test_space",
            "externalId": "jennifer",
            "name": "Jennifer",
            "data_record": {"createdTime": 0, "lastUpdatedTime": 0, "version": 1},
            "outwards": expected_outwards,
        }
