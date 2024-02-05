from __future__ import annotations

import datetime

import pytest
from cognite.client import CogniteClient

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from omni import OmniClient
    from omni import data_classes as dc
else:
    from omni_pydantic_v1 import OmniClient
    from omni_pydantic_v1 import data_classes as dc


def test_node_without_properties(omni_client: OmniClient, cognite_client: CogniteClient) -> None:
    # Arrange
    test_name = "integration_test:NodeWithoutProperties"
    new_connection_c = dc.ConnectionItemCApply(
        external_id=f"{test_name}:ConnectionPair",
        connection_item_a=[
            dc.ConnectionItemAApply(
                external_id=f"{test_name}:ConnectionPair:A",
                name="ConnectionPair:A",
            )
        ],
        connection_item_b=[
            dc.ConnectionItemBApply(
                external_id=f"{test_name}:ConnectionPair:B",
                name="ConnectionPair:B",
            )
        ],
    )
    created: dc.ResourcesApplyResult | None = None
    try:
        # Act
        created = omni_client.apply(new_connection_c)

        # Assert
        assert len(created.nodes) == 3
        assert len(created.edges) == 2

        # Act
        retrieved = omni_client.connection_item_c.retrieve(new_connection_c.external_id)

        # Assert
        assert retrieved.external_id == new_connection_c.external_id

        pytest.skip("Edge case not supported yet")
        # The issue is that there are two edges of the same type. The way we could distinguish between them
        # is to use a hasData filter on the end node.
        assert retrieved.connection_item_a[0] == new_connection_c.connection_item_a[0].external_id
        assert retrieved.connection_item_b[0] == new_connection_c.connection_item_b[0].external_id
    finally:
        if created is not None:
            cognite_client.data_modeling.instances.delete(
                created.nodes.as_ids(),
                created.edges.as_ids(),
            )


def test_apply_multiple_requests(omni_client: OmniClient, cognite_client: CogniteClient) -> None:
    # Arrange
    test_name = "integration_test:ApplyMultipleRequests"
    new_item_a = dc.ConnectionItemAApply(
        external_id=f"{test_name}:Connection:A",
        name="Connection:A",
        other_direct=dc.ConnectionItemCApply(
            external_id=f"{test_name}:Connection:C",
            connection_item_a=[f"{test_name}:Connection:A"],
            connection_item_b=[],
        ),
        outwards=[
            dc.ConnectionItemBApply(
                external_id=f"{test_name}:Connection:B1",
                name="Connection:B1",
            ),
            dc.ConnectionItemBApply(
                external_id=f"{test_name}:Connection:B2",
                name="Connection:B2",
            ),
        ],
    )
    resources = new_item_a.to_instances_apply()

    limit = omni_client.connection_item_a._client.data_modeling.instances._CREATE_LIMIT
    try:
        omni_client.connection_item_a._client.data_modeling.instances._CREATE_LIMIT = 1

        # Act
        created = omni_client.apply(new_item_a)

        # Assert
        assert len(created.nodes) == 4
        assert len(created.edges) == 3
    finally:
        omni_client.connection_item_a._client.data_modeling.instances._CREATE_LIMIT = limit

        cognite_client.data_modeling.instances.delete(resources.nodes.as_ids(), resources.edges.as_ids())


def test_apply_recursive(omni_client: OmniClient, cognite_client: CogniteClient) -> None:
    # Arrange
    test_name = "integration_test:ApplyRecursive"
    new_connection_a = dc.ConnectionItemAApply(
        external_id=f"{test_name}:Connection:A",
        name="Connection:A",
        other_direct=dc.ConnectionItemCApply(
            external_id=f"{test_name}:Connection:C",
            connection_item_a=[],
            connection_item_b=[],
        ),
        self_direct=dc.ConnectionItemAApply(
            external_id=f"{test_name}:Connection:OtherA",
            name="Connection:OtherA",
        ),
        outwards=[
            dc.ConnectionItemBApply(
                external_id=f"{test_name}:Connection:B1",
                name="Connection:B1",
                self_edge=[
                    dc.ConnectionItemBApply(
                        external_id=f"{test_name}:Connection:B3",
                        name="Connection:B3",
                    ),
                ],
            ),
            dc.ConnectionItemBApply(
                external_id=f"{test_name}:Connection:B2",
                name="Connection:B2",
            ),
        ],
    )

    resources = new_connection_a.to_instances_apply()
    node_ids = resources.nodes.as_ids()
    edge_ids = resources.edges.as_ids()

    try:
        # Act
        created = omni_client.apply(new_connection_a)

        # Assert
        assert len(created.nodes) == 6
        assert len(created.edges) == 3

        # Act
        retrieved = omni_client.connection_item_a.retrieve(new_connection_a.external_id)

        # Assert
        assert retrieved.external_id == new_connection_a.external_id
        assert retrieved.name == new_connection_a.name
        assert retrieved.other_direct == new_connection_a.other_direct.external_id
        assert retrieved.self_direct == new_connection_a.self_direct.external_id
        assert len(retrieved.outwards) == 2
    finally:
        cognite_client.data_modeling.instances.delete(nodes=node_ids, edges=edge_ids)


@pytest.fixture(scope="module")
def primitive_nullable_node(omni_client: OmniClient, cognite_client: CogniteClient) -> dc.PrimitiveNullableApply:
    node = dc.PrimitiveNullableApply(
        external_id="integration_test:PrimitiveNullable",
        text="string",
        int_32=1,
        int_64=2,
        float_32=1.1,
        float_64=-1.0,
        boolean=True,
        timestamp=datetime.datetime.fromisoformat("2021-01-01T00:00:00+00:00"),
        date=datetime.date.fromisoformat("2021-01-01"),
        json_={"a": 1, "b": 2},
    )
    try:
        omni_client.primitive_nullable.apply(node)
        yield node
    finally:
        cognite_client.data_modeling.instances.delete(nodes=node.as_tuple_id())


def test_update_to_null(
    omni_client: OmniClient, cognite_client: CogniteClient, primitive_nullable_node: dc.PrimitiveNullableApply
) -> None:
    update = primitive_nullable_node.model_copy()
    update.text = None
    update.int_32 = None
    update.int_64 = None
    update.float_32 = None
    update.float_64 = None
    update.boolean = None
    update.timestamp = None
    update.date = None
    update.json_ = None

    omni_client.apply(update, write_none=True)

    retrieved = omni_client.primitive_nullable.retrieve(primitive_nullable_node.external_id)
    assert retrieved.text is None
    assert retrieved.int_32 is None
    assert retrieved.int_64 is None
    assert retrieved.float_32 is None
    assert retrieved.float_64 is None
    assert retrieved.boolean is None
    assert retrieved.timestamp is None
    assert retrieved.date is None
    assert retrieved.json_ is None


def test_set_empty_string(
    omni_client: OmniClient, cognite_client: CogniteClient, primitive_nullable_node: dc.PrimitiveNullableApply
) -> None:
    update = primitive_nullable_node.model_copy()
    update.text = ""
    omni_client.apply(update, write_none=True)
    retrieved = omni_client.primitive_nullable.retrieve(primitive_nullable_node.external_id)
    assert retrieved is not None, f"Node {primitive_nullable_node.external_id} not found"
    assert retrieved.text == ""


def test_apply_multiple_list(
    omni_client: OmniClient, cognite_client: CogniteClient, primitive_nullable_node: dc.PrimitiveNullableApply
) -> None:
    # Arrange
    test_name = "integration_test:ApplyMultipleList"
    new_items = [
        dc.PrimitiveNullableApply(
            external_id=f"{test_name}:PrimitiveNullable:{i}",
            text="string",
            int_32=i,
            int_64=i,
            float_32=i,
            float_64=i,
            boolean=True,
            timestamp=datetime.datetime.fromisoformat("2021-01-01T00:00:00+00:00"),
            date=datetime.date.fromisoformat("2021-01-01"),
            json_={"a": 1, "b": 2},
        )
        for i in range(10)
    ]

    try:
        # Act
        created = omni_client.apply(new_items)

        # Assert
        assert len(created.nodes) == len(new_items)
    finally:
        cognite_client.data_modeling.instances.delete(nodes=[n.as_tuple_id() for n in new_items])
