from __future__ import annotations

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from omni import OmniClient
    from omni import data_classes as dc
else:
    from omni_pydantic_v1 import OmniClient
    from omni_pydantic_v1 import data_classes as dc


def test_query_with_direct_relation(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_a(limit=5).query(retrieve_other_direct=True, retrieve_self_direct=True)

    assert len(items) > 0
    for item in items:
        assert isinstance(item, dc.ConnectionItemA)
        assert (
            isinstance(item.other_direct, dc.ConnectionItemC) or item.other_direct is None
        ), f"Direct relation should either be set or not exist, got {item.other_direct}"
        assert (
            isinstance(item.self_direct, dc.ConnectionItemA) or item.self_direct is None
        ), f"Direct relation should either be set or not exist, got {item.self_direct}"
