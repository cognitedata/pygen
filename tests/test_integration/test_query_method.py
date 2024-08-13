from __future__ import annotations

import pytest

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


def test_query_across_edge_without_properties(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_a.query().outwards.execute(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemAList)
    item_bs = [edge for item in items for edge in item.outwards or []]
    assert len(item_bs) > 0


@pytest.mark.skip("Missing test data")
def test_query_across_edge_properties(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_f.query().outwards_multi.end_node.execute(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemFList)
    item_bs = [edge for item in items for edge in item.outwards_multi or []]
    assert len(item_bs) > 0


def test_query_circular_raises_value_error(omni_client: OmniClient) -> None:
    with pytest.raises(ValueError) as e:
        omni_client.connection_item_a.query().outwards.inwards.outwards.execute(limit=5)

    assert "Circular" in str(e.value)
