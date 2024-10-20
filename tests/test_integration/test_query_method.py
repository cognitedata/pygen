import pytest
from omni import OmniClient
from omni import data_classes as dc
from windmill import WindmillClient


def test_query_across_direct_relation(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_a.query().other_direct.list_full(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemAList)
    incorrect_other = [
        item.other_direct
        for item in items
        if not (isinstance(item.other_direct, dc.ConnectionItemCNode) or item.other_direct is None)
    ]
    assert len(incorrect_other) == 0, f"Other direct relation should be set, got {incorrect_other}"


def test_query_list_method(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_a.query().other_direct.list_connection_item_c_node(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemCNodeList)


def test_query_list_method_with_filter(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_a.query().outwards.name.prefix("A").list_connection_item_b(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemBList)
    for item in items:
        assert item.name.startswith("A")


def test_query_list_method_with_filter_query(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_a.query().outwards.name.prefix("A").list_full(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemAList)
    found_one = False
    for item in items:
        for subitem in item.outwards or []:
            if isinstance(subitem, dc.ConnectionItemB):
                assert subitem.name.startswith("A")
                found_one = True
    assert found_one, "No items found with name starting with 'A'"


def test_query_across_edge_without_properties(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_a.query().outwards.list_full(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemAList)
    item_bs = [edge for item in items for edge in item.outwards or []]
    assert len(item_bs) > 0


@pytest.mark.skip("Missing test data")
def test_query_across_edge_properties(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_f.query().outwards_multi.end_node.list_full(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemFList)
    item_bs = [edge for item in items for edge in item.outwards_multi or []]
    assert len(item_bs) > 0


def test_query_circular_raises_value_error(omni_client: OmniClient) -> None:
    with pytest.raises(ValueError) as e:
        omni_client.connection_item_a.query().outwards.inwards.outwards.list_full(limit=5)

    assert "Circular" in str(e.value)


def test_query_list_across_edge_limit(wind_client: WindmillClient) -> None:
    items = wind_client.windmill.query().name.equals("hornsea_1_mill_1").blades.list_blade(limit=5)

    assert len(items) > 0
