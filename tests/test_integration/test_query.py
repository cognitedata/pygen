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


def test_query_circular(omni_client: OmniClient) -> None:
    b_items = omni_client.connection_item_b(limit=5).inwards(limit=-1).outwards(limit=-1).query()

    assert len(b_items) > 0
    for b_item in b_items:
        assert isinstance(b_item, dc.ConnectionItemB)
        assert len(b_item.inwards or []) > 0
        for a_item in b_item.inwards:
            assert isinstance(a_item, dc.ConnectionItemA)
            assert len(a_item.outwards or []) > 0
            for b_item_circular in a_item.outwards:
                assert isinstance(b_item_circular, dc.ConnectionItemB)