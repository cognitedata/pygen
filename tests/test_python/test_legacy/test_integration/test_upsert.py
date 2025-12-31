import datetime
from collections.abc import Iterable

import pytest
from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadataWrite, SequenceColumnWrite, SequenceWrite, TimeSeriesWrite
from omni import OmniClient
from omni import data_classes as dc


# The retrieve node frequently fails, likely due to eventual consistency.
@pytest.mark.flaky(reruns=3, reruns_delay=10, only_rerun=["AssertionError"])
def test_node_without_properties(omni_client: OmniClient, cognite_client: CogniteClient) -> None:
    # Arrange
    test_name = "integration_test:NodeWithoutProperties"
    new_connection_c = dc.ConnectionItemCNodeWrite(
        external_id=f"{test_name}:ConnectionPair",
        connection_item_a=[
            dc.ConnectionItemAWrite(
                external_id=f"{test_name}:ConnectionPair:A",
                name="ConnectionPair:A",
            )
        ],
        connection_item_b=[
            dc.ConnectionItemBWrite(
                external_id=f"{test_name}:ConnectionPair:B",
                name="ConnectionPair:B",
            )
        ],
    )
    created: dc.ResourcesWriteResult | None = None
    try:
        # Act
        created = omni_client.upsert(new_connection_c)

        # Assert
        assert len(created.nodes) == 3
        assert len(created.edges) == 2

        # Act
        retrieved = omni_client.connection_item_c_node.retrieve(new_connection_c.external_id)

        # Assert
        assert retrieved is not None
        assert retrieved.external_id == new_connection_c.external_id
    finally:
        if created is not None:
            cognite_client.data_modeling.instances.delete(
                created.nodes.as_ids(),
                created.edges.as_ids(),
            )


def test_upsert_multiple_requests(omni_client: OmniClient, cognite_client: CogniteClient) -> None:
    # Arrange
    test_name = "integration_test:ApplyMultipleRequests"
    new_item_a = dc.ConnectionItemAWrite(
        external_id=f"{test_name}:Connection:A",
        name="Connection:A",
        other_direct=dc.ConnectionItemCNodeWrite(
            external_id=f"{test_name}:Connection:C",
            connection_item_a=[f"{test_name}:Connection:A"],
            connection_item_b=[],
        ),
        outwards=[
            dc.ConnectionItemBWrite(
                external_id=f"{test_name}:Connection:B1",
                name="Connection:B1",
            ),
            dc.ConnectionItemBWrite(
                external_id=f"{test_name}:Connection:B2",
                name="Connection:B2",
            ),
        ],
    )
    resources = new_item_a.to_instances_write()

    limit = omni_client.connection_item_a._client.data_modeling.instances._CREATE_LIMIT
    try:
        omni_client.connection_item_a._client.data_modeling.instances._CREATE_LIMIT = 1

        # Act
        created = omni_client.upsert(new_item_a)

        # Assert
        assert len(created.nodes) == 4
        assert len(created.edges) == 3
    finally:
        omni_client.connection_item_a._client.data_modeling.instances._CREATE_LIMIT = limit

        cognite_client.data_modeling.instances.delete(resources.nodes.as_ids(), resources.edges.as_ids())


def test_upsert_recursive(omni_client: OmniClient, cognite_client: CogniteClient) -> None:
    # Arrange
    test_name = "integration_test:ApplyRecursive"
    new_connection_a = dc.ConnectionItemAWrite(
        external_id=f"{test_name}:Connection:A",
        name="Connection:A",
        other_direct=dc.ConnectionItemCNodeWrite(
            external_id=f"{test_name}:Connection:C",
            connection_item_a=[],
            connection_item_b=[],
        ),
        self_direct=dc.ConnectionItemAWrite(
            external_id=f"{test_name}:Connection:OtherA",
            name="Connection:OtherA",
        ),
        outwards=[
            dc.ConnectionItemBWrite(
                external_id=f"{test_name}:Connection:B1",
                name="Connection:B1",
                self_edge=[
                    dc.ConnectionItemBWrite(
                        external_id=f"{test_name}:Connection:B3",
                        name="Connection:B3",
                    ),
                ],
            ),
            dc.ConnectionItemBWrite(
                external_id=f"{test_name}:Connection:B2",
                name="Connection:B2",
            ),
        ],
    )

    resources = new_connection_a.to_instances_write()
    node_ids = resources.nodes.as_ids()
    edge_ids = resources.edges.as_ids()

    try:
        # Act
        created = omni_client.upsert(new_connection_a)

        # Assert
        assert len(created.nodes) == 6
        assert len(created.edges) == 3

        # Act
        retrieved = omni_client.connection_item_a.retrieve(new_connection_a.external_id)

        # Assert
        assert retrieved is not None
        assert retrieved.external_id == new_connection_a.external_id
        assert retrieved.name == new_connection_a.name
        assert isinstance(new_connection_a.other_direct, dc.ConnectionItemCNodeWrite)
        assert retrieved.other_direct == new_connection_a.other_direct.external_id
        assert isinstance(new_connection_a.self_direct, dc.ConnectionItemAWrite)
        assert retrieved.self_direct == new_connection_a.self_direct.external_id
    finally:
        cognite_client.data_modeling.instances.delete(nodes=node_ids, edges=edge_ids)


@pytest.fixture(scope="module")
def primitive_nullable_node(
    omni_client: OmniClient, cognite_client: CogniteClient
) -> Iterable[dc.PrimitiveNullableWrite]:
    node = dc.PrimitiveNullableWrite(
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
        omni_client.upsert(node)
        yield node
    finally:
        cognite_client.data_modeling.instances.delete(nodes=node.as_tuple_id())


def test_set_empty_string(
    omni_client: OmniClient, cognite_client: CogniteClient, primitive_nullable_node: dc.PrimitiveNullableWrite
) -> None:
    update = primitive_nullable_node.model_copy()

    update.text = ""
    omni_client.upsert(update)
    retrieved = omni_client.primitive_nullable.retrieve(primitive_nullable_node.external_id)
    assert retrieved is not None, f"Node {primitive_nullable_node.external_id} not found"
    assert retrieved.text == ""


def test_upsert_multiple_list(
    omni_client: OmniClient, cognite_client: CogniteClient, primitive_nullable_node: dc.PrimitiveNullableWrite
) -> None:
    # Arrange
    test_name = "integration_test:ApplyMultipleList"
    new_items = [
        dc.PrimitiveNullableWrite(
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
        created = omni_client.upsert(new_items)

        # Assert
        assert len(created.nodes) == len(new_items)
    finally:
        cognite_client.data_modeling.instances.delete(nodes=[n.as_tuple_id() for n in new_items])


def test_upsert_recursive_with_single_edge(omni_client: OmniClient, cognite_client: CogniteClient) -> None:
    # Arrange
    test_name = "integration_test:ApplyRecursiveWithSingleEdge"
    new_items = dc.ConnectionItemDWrite(
        external_id=f"{test_name}:Connection:D",
        name="Connection:D",
        outwards_single=dc.ConnectionItemEWrite(
            external_id=f"{test_name}:Connection:E",
            name="Connection:E",
        ),
    )
    resources = dc.ResourcesWriteResult()
    try:
        # Act
        resources = omni_client.upsert(new_items)

        # Assert
        assert len(resources.nodes) == 2
        assert len(resources.edges) == 1
    finally:
        if resources.nodes:
            cognite_client.data_modeling.instances.delete(resources.nodes.as_ids())
        if resources.edges:
            cognite_client.data_modeling.instances.delete(edges=resources.edges.as_ids())


def test_upsert_with_cdf_external(omni_client: OmniClient, cognite_client: CogniteClient) -> None:
    # Arrange
    test_name = "integration_test:upsert_with_cdf_external"
    new_item = dc.CDFExternalReferencesWrite(
        external_id=f"{test_name}:Item1",
        file=FileMetadataWrite(
            external_id=f"{test_name}:File1",
            name="File1",
        ),
        sequence=SequenceWrite(
            external_id=f"{test_name}:Sequence1",
            name="Sequence1",
            columns=[
                SequenceColumnWrite(
                    external_id=f"{test_name}:Column1",
                    name="Column1",
                    value_type="String",
                )
            ],
        ),
        timeseries=TimeSeriesWrite(
            external_id=f"{test_name}:TimeSeries1",
            name="TimeSeries1",
            metadata={"key": "value"},
        ),
    )
    resources = dc.ResourcesWriteResult()
    try:
        # Act
        resources = omni_client.upsert(new_item)

        # Assert
        assert len(resources.nodes) == 1
        assert len(resources.time_series) == 1
        assert len(resources.files) == 1
        assert len(resources.sequences) == 1
    finally:
        if resources.nodes:
            cognite_client.data_modeling.instances.delete(resources.nodes.as_ids())
        if resources.time_series:
            cognite_client.time_series.delete(external_id=resources.time_series.as_external_ids())
        if resources.files:
            cognite_client.files.delete(id=resources.files.as_ids())
        if resources.sequences:
            cognite_client.sequences.delete(external_id=resources.sequences.as_external_ids())


def test_upsert_with_cdf_external_listed(omni_client: OmniClient, cognite_client: CogniteClient) -> None:
    # Arrange
    test_name = "integration_test:upsert_with_cdf_external_listed"
    new_item = dc.CDFExternalReferencesListedWrite(
        external_id=f"{test_name}:Item1",
        files=[
            FileMetadataWrite(
                external_id=f"{test_name}:File1",
                name="File1",
            )
        ],
        sequences=[
            SequenceWrite(
                external_id=f"{test_name}:Sequence1",
                name="Sequence1",
                columns=[
                    SequenceColumnWrite(
                        external_id=f"{test_name}:Column1",
                        name="Column1",
                        value_type="String",
                    )
                ],
            )
        ],
        timeseries=[
            TimeSeriesWrite(
                external_id=f"{test_name}:TimeSeries1",
                name="TimeSeries1",
                metadata={"key": "value"},
            )
        ],
    )

    resources = dc.ResourcesWriteResult()
    try:
        # Act
        resources = omni_client.upsert(new_item)

        # Assert
        assert len(resources.nodes) == 1
        assert len(resources.time_series) == 1
        assert len(resources.files) == 1
        assert len(resources.sequences) == 1
    finally:
        if resources.nodes:
            cognite_client.data_modeling.instances.delete(resources.nodes.as_ids())
        if resources.time_series:
            cognite_client.time_series.delete(external_id=resources.time_series.as_external_ids())
        if resources.files:
            cognite_client.files.delete(id=resources.files.as_ids())
        if resources.sequences:
            cognite_client.sequences.delete(external_id=resources.sequences.as_external_ids())
