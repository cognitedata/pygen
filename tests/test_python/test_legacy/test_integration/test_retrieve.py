import pytest
from omni import OmniClient
from omni import data_classes as dc


def test_retrieve_missing(omni_client: OmniClient) -> None:
    # Arrange
    external_id = "non-existing"

    # Act
    item = omni_client.primitive_required.retrieve(external_id=external_id)

    # Assert
    assert item is None


def test_retrieve_and_as_write(omni_client: OmniClient) -> None:
    external_id = omni_client.primitive_nullable.list(limit=1)[0].external_id

    item = omni_client.primitive_nullable.retrieve(external_id=external_id)

    assert item is not None

    def raise_(*args, **kwargs):
        raise ValueError("This should not be called")

    dc.DomainModelWrite.external_id_factory = raise_
    write = item.as_write()
    dc.DomainModelWrite.external_id_factory = None
    assert write.external_id == item.external_id


def test_retrieve_as_child_class(omni_client: OmniClient) -> None:
    items1 = omni_client.implementation_1.list(limit=1)
    assert len(items1) > 0
    item1 = items1[0]
    items2 = omni_client.implementation_2.list(limit=1)
    assert len(items2) > 0
    item2 = items2[0]

    items = omni_client.sub_interface.retrieve(
        [item1.external_id, item2.external_id],
        as_child_class=["Implementation1", "Implementation2"],
    )

    assert len(items) == 2

    assert isinstance(items[0], dc.Implementation1)
    assert isinstance(items[1], dc.Implementation2)


@pytest.fixture(scope="session")
def item_a_list(omni_client: OmniClient) -> dc.ConnectionItemAList:
    return omni_client.connection_item_a.list(limit=5)


def test_retrieve_single_with_connections(omni_client: OmniClient, item_a_list: dc.ConnectionItemAList) -> None:
    item_a = item_a_list[0]
    item = omni_client.connection_item_a.retrieve(external_id=item_a.external_id, retrieve_connections="full")

    assert item is not None
    connections = [c for c in [item.outwards, item.self_direct, item.other_direct] if c is not None]
    assert connections


def test_retrieve_multiple_with_connections(omni_client: OmniClient, item_a_list: dc.ConnectionItemAList) -> None:
    external_ids = [item_a.external_id for item_a in item_a_list]
    items = omni_client.connection_item_a.retrieve(external_id=external_ids, retrieve_connections="full")

    assert len(items) == len(item_a_list)
    for item in items:
        connections = [c for c in [item.outwards, item.self_direct, item.other_direct] if c is not None]
        assert connections


def test_retrieve_with_connections_same(omni_client: OmniClient, item_a_list: dc.ConnectionItemAList) -> None:
    item_a = item_a_list[0]
    retrieve_identifier = omni_client.connection_item_a.retrieve(
        external_id=item_a.external_id, retrieve_connections="identifier"
    )
    retrieve_full = omni_client.connection_item_a.retrieve(external_id=item_a.external_id, retrieve_connections="full")

    assert retrieve_identifier is not None
    assert retrieve_full is not None
    outward_identifiers = sorted(
        [outward for outward in retrieve_identifier.outwards or [] if isinstance(outward, str)]
    )
    outward_full = sorted(
        [outward.external_id for outward in retrieve_full.outwards or [] if isinstance(outward, dc.ConnectionItemB)]
    )
    assert outward_identifiers == outward_full
