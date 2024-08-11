from __future__ import annotations

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from omni import OmniClient
    from omni import data_classes as dc
else:
    from omni_pydantic_v1 import OmniClient
    from omni_pydantic_v1 import data_classes as dc


def test_query_across_direct_relation(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_a.query().other_direct.execute(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemAList)
    incorrect_other = [
        item.other_direct
        for item in items
        if not (isinstance(item.other_direct, dc.ConnectionItemCNode) or item.other_direct is None)
    ]
    assert len(incorrect_other) == 0, f"Other direct relation should be set, got {incorrect_other}"


def test_query_list_method(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_a.query().other_direct.list(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemCNodeList)
