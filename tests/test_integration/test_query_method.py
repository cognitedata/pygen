import pytest
from omni import OmniClient
from omni import data_classes as dc
from windmill import WindmillClient


def test_query_across_direct_relation(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_a.query.other_direct.list_full(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemAList)
    incorrect_other = [
        item.other_direct
        for item in items
        if not (isinstance(item.other_direct, dc.ConnectionItemCNode) or item.other_direct is None)
    ]
    assert len(incorrect_other) == 0, f"Other direct relation should be set, got {incorrect_other}"


def test_query_list_method(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_a.query.other_direct.list_connection_item_c_node(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemCNodeList)


def test_query_list_method_with_filter(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_a.query.outwards.name.prefix("A").list_connection_item_b(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemBList)
    for item in items:
        assert item.name.startswith("A")


def test_query_list_method_with_filter_query(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_a.query.outwards.name.prefix("A").list_full(limit=5)
    items_reversed = omni_client.connection_item_b.query.name.prefix("A").inwards.list_connection_item_a(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemAList)
    assert len(items_reversed) > 0
    assert isinstance(items_reversed, dc.ConnectionItemAList)
    assert set(items.as_external_ids()) == set(items_reversed.as_external_ids())

    invalid_outwards = {
        item.external_id: subitems
        for item in items
        if (
            subitems := [
                subitem
                for subitem in item.outwards or []
                if not isinstance(subitem, dc.ConnectionItemB) or not subitem.name.startswith("A")
            ]
        )
    }
    assert not invalid_outwards, "Some outwards are not ConnectionItemB or do not start with 'A'"


def test_query_across_edge_without_properties(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_a.query.outwards.list_full(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemAList)
    item_bs = [edge for item in items for edge in item.outwards or []]
    assert len(item_bs) > 0


@pytest.mark.skip("Missing test data")
def test_query_across_edge_properties(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_f.query.outwards_multi.end_node.list_full(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemFList)
    item_bs = [edge for item in items for edge in item.outwards_multi or []]
    assert len(item_bs) > 0


def test_query_circular_raises_value_error(omni_client: OmniClient) -> None:
    with pytest.raises(ValueError) as e:
        omni_client.connection_item_a.query.outwards.inwards.outwards.list_full(limit=5)

    assert "Circular" in str(e.value)


def test_query_list_across_edge_limit(wind_client: WindmillClient) -> None:
    items = wind_client.windmill.query.name.equals("hornsea_1_mill_1").blades.list_blade(limit=5)

    assert len(items) > 0


def test_query_across_reverse_direct_relation_to_list_full(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_e.query.direct_reverse_multi.list_full(limit=5)

    assert len(items) > 0
