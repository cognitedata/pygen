from __future__ import annotations

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
        created = omni_client.connection_item_c.apply(new_connection_c)

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
