import pytest
from cognite.client.data_classes.data_modeling import ViewId
from cognite.client.data_classes.data_modeling.instances import Edge, Node, Properties

from cognite.pygen._query.processing import QueryUnpacker


class TestQueryUnpacker:
    @pytest.mark.parametrize(
        "instance, expected",
        [
            pytest.param(
                Node(
                    space="test_space",
                    external_id="test_id",
                    version=1,
                    last_updated_time=1,
                    created_time=1,
                    properties=Properties(
                        {
                            ViewId("schema_space", "MyViewId", "v1"): {
                                "name": "Test Node",
                                "description": "This is a test node",
                            }
                        }
                    ),
                    deleted_time=None,
                    type=None,
                ),
                {
                    "space": "test_space",
                    "externalId": "test_id",
                    "version": 1,
                    "lastUpdatedTime": 1,
                    "createdTime": 1,
                    "name": "Test Node",
                    "description": "This is a test node",
                },
                id="Node with properties",
            ),
            pytest.param(
                Edge(
                    space="test_space",
                    external_id="test_id",
                    version=1,
                    last_updated_time=1,
                    created_time=1,
                    properties=Properties(
                        {
                            ViewId("schema_space", "MyViewId", "v1"): {
                                "name": "Test Edge",
                                "description": "This is a test edge",
                            }
                        }
                    ),
                    deleted_time=None,
                    type=("schema_space", "myType"),
                    start_node=("test_space", "node1"),
                    end_node=("test_space", "node2"),
                ),
                {
                    "space": "test_space",
                    "externalId": "test_id",
                    "version": 1,
                    "lastUpdatedTime": 1,
                    "createdTime": 1,
                    "name": "Test Edge",
                    "description": "This is a test edge",
                    "type": {"space": "schema_space", "externalId": "myType"},
                    "startNode": {"space": "test_space", "externalId": "node1"},
                    "endNode": {"space": "test_space", "externalId": "node2"},
                },
                id="Edge with properties",
            ),
        ],
    )
    def test_flatten_dump(self, instance: Node | Edge, expected: dict[str, object]) -> None:
        flatten = QueryUnpacker.flatten_dump(instance, None, None)

        assert flatten == expected
