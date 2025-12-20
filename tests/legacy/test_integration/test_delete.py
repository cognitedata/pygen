from __future__ import annotations

from cognite.client import CogniteClient
from omni import OmniClient
from omni import data_classes as dc


def test_delete_recursive(omni_client: OmniClient, cognite_client: CogniteClient) -> None:
    # Arrange
    item = dc.ConnectionItemAWrite(
        external_id="temp:test_delete_recursive:itemA",
        name="Item A",
        outwards=[
            dc.ConnectionItemBWrite(
                external_id="temp:test_delete_recursive:itemB",
                name="Item B",
            )
        ],
    )
    resources = item.to_instances_write()

    try:
        created = omni_client.upsert(item)
        assert len(created.nodes) == 2
        assert len(created.edges) == 1

        # Act
        deleted = omni_client.delete(item)

        # Arrange
        assert len(deleted.nodes) == 2
        assert len(deleted.edges) == 1

        retrieved = cognite_client.data_modeling.instances.retrieve(resources.nodes.as_ids(), resources.edges.as_ids())
        assert not retrieved.nodes
        assert not retrieved.edges
    finally:
        # Cleanup
        cognite_client.data_modeling.instances.delete(resources.nodes.as_ids(), resources.edges.as_ids())
