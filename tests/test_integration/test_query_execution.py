from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.pygen import _QueryExecutor


def test_query_reverse_direct_relation_list(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    item_e = omni_views["ConnectionItemE"]
    item_d = omni_views["ConnectionItemD"]
    executor = _QueryExecutor(cognite_client, views=[item_e, item_d])

    result = executor.execute_query(item_e.as_id(), "list", ["name", "directReverseMulti"])

    assert result


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
