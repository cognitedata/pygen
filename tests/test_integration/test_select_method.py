import pytest
from cognite_core import CogniteCoreClient
from omni import OmniClient
from omni import data_classes as dc
from wind_turbine import WindTurbineClient

from tests.constants import CORE_SDK


def test_select_across_direct_relation(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_a.select().other_direct.list_full(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemAList)
    incorrect_other = [
        item.other_direct
        for item in items
        if not (isinstance(item.other_direct, dc.ConnectionItemCNode) or item.other_direct is None)
    ]
    assert len(incorrect_other) == 0, f"Other direct relation should be set, got {incorrect_other}"
    assert items.dump()


def test_select_filter_on_direct_relation(omni_client: OmniClient) -> None:
    items = (
        omni_client.connection_item_a.select().other_direct_filter.equals("ConnectionItemC:Sarah").list_full(limit=5)
    )

    assert len(items) != 0
    in_correct_other = [item.other_direct for item in items if item.other_direct != "ConnectionItemC:Sarah"]
    assert len(in_correct_other) == 0, f"Other direct relation should be set, got {in_correct_other}"


def test_select_list_end_cls_method(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_a.select().other_direct.list_connection_item_c_node(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemCNodeList)
    assert items.dump()


def test_select_list_method_with_filter(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_a.select().outwards.name.prefix("A").list_connection_item_b(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemBList)
    for item in items:
        assert item.name is not None
        assert item.name.startswith("A")
    assert items.dump()


def test_select_list_method_with_filter_query(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_a.select().outwards.name.prefix("A").list_full(limit=5)
    items_reversed = omni_client.connection_item_b.select().name.prefix("A").inwards.list_connection_item_a(limit=5)

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
                if not isinstance(subitem, dc.ConnectionItemB) or not (subitem.name and subitem.name.startswith("A"))
            ]
        )
    }
    assert not invalid_outwards, "Some outwards are not ConnectionItemB or do not start with 'A'"
    assert items.dump()


def test_select_list_full_inwards_edge(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_b.select().inwards.name.prefix("M").list_full(limit=5)
    items_reversed = omni_client.connection_item_a.select().name.prefix("M").outwards.list_connection_item_b(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemBList)
    assert len(items_reversed) > 0
    assert isinstance(items_reversed, dc.ConnectionItemBList)
    assert set(items.as_external_ids()) == set(items_reversed.as_external_ids())
    assert items.dump()


def test_select_list_full_across_direct_relation(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_d.select().direct_single.name.prefix("E").list_full(limit=5)
    items_reversed = (
        omni_client.connection_item_e.select().name.prefix("E").direct_reverse_single.list_connection_item_d(limit=5)
    )

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemDList)
    assert len(items_reversed) > 0
    assert isinstance(items_reversed, dc.ConnectionItemDList)
    assert set(items.as_external_ids()) == set(items_reversed.as_external_ids())
    assert items.dump()


def test_select_list_full_across_reverse_direct_relation(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_e.select().direct_reverse_single.name.prefix("M").list_full(limit=5)
    items_reversed = (
        omni_client.connection_item_d.select().name.prefix("M").direct_single.list_connection_item_e(limit=5)
    )

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemEList)
    assert len(items_reversed) > 0
    assert isinstance(items_reversed, dc.ConnectionItemEList)
    assert set(items.as_external_ids()) == set(items_reversed.as_external_ids())
    assert items.dump()


def test_select_across_edge_without_properties(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_a.select().outwards.list_full(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemAList)
    item_bs = [edge for item in items for edge in item.outwards or []]
    assert len(item_bs) > 0
    assert items.dump()


@pytest.mark.skip("Missing test data")
def test_select_across_edge_properties(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_f.select().outwards_multi.end_node.list_full(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemFList)
    item_bs = [edge for item in items for edge in item.outwards_multi or []]
    assert len(item_bs) > 0
    assert items.dump()


def test_select_circular_raises_value_error(omni_client: OmniClient) -> None:
    with pytest.raises(ValueError) as e:
        omni_client.connection_item_a.select().outwards.inwards.outwards.list_full(limit=5)

    assert "Circular" in str(e.value)


def test_select_above_max_depth_raises_value_error(omni_client: OmniClient) -> None:
    with pytest.raises(ValueError) as e:
        omni_client.connection_item_a.select().outwards.inwards.other_direct.list_connection_item_c_node(limit=5)

    assert "Max select depth reached" in str(e.value)


def test_select_past_reverse_list_value_error(omni_client: OmniClient) -> None:
    with pytest.raises(ValueError) as e:
        omni_client.connection_item_e.select().direct_reverse_multi.direct_single.list_full(limit=5)

    assert "Cannot traverse past reverse direct relation of list." in str(e.value)


def test_select_across_reversed_list(omni_client: OmniClient) -> None:
    result = omni_client.connection_item_e.select().direct_reverse_multi.list_full(limit=5)
    assert result


def test_select_end_on_reverse_direct_relation_to_list(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_e.select().direct_reverse_multi.list_connection_item_d(limit=5)

    assert len(items) > 0
    assert isinstance(items, dc.ConnectionItemDList)
    assert items.dump()


def test_select_list_across_edge_with_limit(turbine_client: WindTurbineClient) -> None:
    items = turbine_client.wind_turbine.select().name.equals("hornsea_1_mill_1").metmast.end_node.list_full(limit=5)

    assert len(items) > 0
    assert items.dump()


def test_select_across_reverse_direct_relation_to_list_full(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_e.select().direct_reverse_multi.list_full(limit=5)

    assert len(items) > 0
    assert items.dump()


def test_select_return_other_side_reverse_list(core_client: CogniteCoreClient) -> None:
    assert CORE_SDK.instance_space is not None
    result = (
        core_client.cognite_asset.select()
        .name.equals("230900")
        .space.equals(CORE_SDK.instance_space)
        .children.list_cognite_asset(limit=2)
    )

    assert len(result) == 2


def test_select_on_direct_relation_then_traverse(core_client: CogniteCoreClient) -> None:
    assert CORE_SDK.instance_space is not None
    result = (
        core_client.cognite_asset.select()
        .space.equals(CORE_SDK.instance_space)
        .parent.name.equals("230900")
        .list_full(limit=2)
    )
    assert len(result) == 2


@pytest.fixture(scope="session")
def primitive_required_list(omni_client: OmniClient) -> dc.PrimitiveRequiredList:
    return omni_client.primitive_required.list(limit=-1)


def test_select_latest_omni(omni_client: OmniClient, primitive_required_list) -> None:
    latest = max(primitive_required_list, key=lambda x: x.timestamp)

    result = omni_client.primitive_required.select().timestamp.latest().list_full()
    assert len(result) == 1
    assert result[0].external_id == latest.external_id


def test_select_earliest_omni(omni_client: OmniClient, primitive_required_list) -> None:
    earliest = min(primitive_required_list, key=lambda x: x.timestamp)

    result = omni_client.primitive_required.select().timestamp.earliest().list_primitive_required()
    assert len(result) == 1
    assert result[0].external_id == earliest.external_id


def test_select_sort_ascending(omni_client: OmniClient, primitive_required_list: dc.PrimitiveRequiredList) -> None:
    sorted_list = sorted(primitive_required_list, key=lambda x: x.timestamp)
    result = omni_client.primitive_required.select().timestamp.sort_ascending().list_full()

    assert [item.external_id for item in result] == [item.external_id for item in sorted_list]


def test_select_sort_descending(omni_client: OmniClient, primitive_required_list: dc.PrimitiveRequiredList) -> None:
    sorted_list = sorted(primitive_required_list, key=lambda x: x.timestamp, reverse=True)
    result = omni_client.primitive_required.select().timestamp.sort_descending().list_full()

    assert [item.external_id for item in result] == [item.external_id for item in sorted_list]


def test_select_latest_core(core_client: CogniteCoreClient) -> None:
    result = core_client.cognite_equipment.select().asset.path_last_updated_time.latest().list_cognite_asset()

    assert len(result) == 1


def test_select_helpful_feedback_on_type(omni_client: OmniClient) -> None:
    with pytest.raises(AttributeError) as e:
        omni_client.primitive_required.select().float64.range(123.0, 456.0).list_full()

    assert "Did you mean one of" in str(e.value)
