from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.pygen import _QueryExecutor


def test_query_reverse_direct_relation(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    item_e = omni_views["ConnectionItemE"]
    item_d = omni_views["ConnectionItemD"]
    executor = _QueryExecutor(cognite_client, views=[item_e, item_d])
    properties = [
        "name",
        {"directReverseMulti": ["name"]},
        "directNoSource",
        {"directReverseSingle": ["name", "directMulti"]},
    ]
    flatten_props = {"name", "directReverseMulti", "directNoSource", "directReverseSingle"}
    result = executor.execute_query(item_e.as_id(), "list", properties, limit=5)

    assert isinstance(result, dict)
    assert "listConnectionItemE" in result
    assert isinstance(result["listConnectionItemE"], list)
    assert len(result["listConnectionItemE"]) > 0
    ill_formed_items = [item for item in result["listConnectionItemE"] if not (set(item.keys()) <= flatten_props)]
    assert not ill_formed_items, f"Items with unexpected properties: {ill_formed_items}"
    ill_formed_subitems = [
        subitem
        for item in result["listConnectionItemE"]
        for subitem in item.get("directReverseMulti", [])
        if not (set(subitem.keys()) <= {"name"})
    ]
    assert not ill_formed_subitems, f"Subitems with unexpected properties: {ill_formed_subitems}"
    ill_formed_subitems_single = [
        subitem
        for item in result["listConnectionItemE"]
        for subitem in item.get("directReverseSingle", [])
        if not (set(subitem.keys()) <= {"name", "directMulti"})
    ]
    assert not ill_formed_subitems_single, f"Subitems with unexpected properties: {ill_formed_subitems_single}"


def test_query_list_primitive_properties(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    view = omni_views["PrimitiveNullable"]
    executor = _QueryExecutor(cognite_client, views=[view])
    properties = ["text", "boolean", "date"]
    result = executor.execute_query(view.as_id(), "list", properties, limit=5)

    assert isinstance(result, dict)
    assert "listPrimitiveNullable" in result
    assert isinstance(result["listPrimitiveNullable"], list)
    assert len(result["listPrimitiveNullable"]) > 0
    properties_set = set(properties)
    ill_formed_items = [item for item in result["listPrimitiveNullable"] if not (set(item.keys()) <= properties_set)]
    assert not ill_formed_items, f"Items with unexpected properties: {ill_formed_items}"
