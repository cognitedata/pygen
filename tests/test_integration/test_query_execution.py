from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from omni import OmniClient

from cognite.pygen import _QueryExecutor


def test_query_reverse_direct_relation(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    item_e = omni_views["ConnectionItemE"]
    item_d = omni_views["ConnectionItemD"]
    executor = _QueryExecutor(cognite_client, views=[item_e, item_d])
    properties = [
        "externalId",
        "name",
        {"directReverseMulti": ["name", "externalId"]},
        {"directReverseSingle": ["name", "directMulti", "externalId"]},
    ]
    flatten_props = {"name", "directReverseMulti", "directReverseSingle", "externalId"}
    result = executor.list(item_e.as_id(), properties, limit=5)

    assert isinstance(result, list)
    assert len(result) > 0
    ill_formed_items = [item for item in result if not (set(item.keys()) <= flatten_props)]
    assert not ill_formed_items, f"Items with unexpected properties: {ill_formed_items}"
    ill_formed_subitems = [
        subitem
        for item in result
        for subitem in item.get("directReverseMulti", [])
        if not (set(subitem.keys()) <= {"name", "externalId"})
    ]
    assert not ill_formed_subitems, f"Subitems with unexpected properties: {ill_formed_subitems}"
    ill_formed_subitems_single = [
        subitem
        for item in result
        for subitem in item.get("directReverseSingle", [])
        if not (set(subitem.keys()) <= {"name", "directMulti", "externalId"})
    ]
    assert not ill_formed_subitems_single, f"Subitems with unexpected properties: {ill_formed_subitems_single}"


def test_query_direct_relation(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    item_e = omni_views["ConnectionItemE"]
    item_d = omni_views["ConnectionItemD"]
    executor = _QueryExecutor(cognite_client, views=[item_e, item_d])
    properties = [
        "externalId",
        "name",
        {"directMulti": ["name", "externalId"]},
        {"directSingle": ["name", "externalId"]},
    ]
    flatten_props = {"name", "directMulti", "directSingle", "externalId"}
    result = executor.list(item_d.as_id(), properties, limit=5)

    assert isinstance(result, list)
    assert len(result) > 0
    ill_formed_items = [item for item in result if not (set(item.keys()) <= flatten_props)]
    assert not ill_formed_items, f"Items with unexpected properties: {ill_formed_items}"
    has_subitems = any(item.get("directMulti") or item.get("directSingle") for item in result)
    assert has_subitems, "No subitems found"

    ill_formed_subitems = [
        subitem
        for item in result
        for subitem in item.get("directMulti", [])
        if not (set(subitem.keys()) <= {"name", "externalId"})
    ]
    assert not ill_formed_subitems, f"Subitems with unexpected properties: {ill_formed_subitems}"
    has_subitems_single = any(item.get("directSingle") for item in result)
    assert has_subitems_single, "No single subitems found"
    ill_formed_subitems_single = [
        subitem
        for item in result
        for subitem in item.get("directSingle", [])
        if not (set(subitem.keys()) <= {"name", "externalId"})
    ]
    assert not ill_formed_subitems_single, f"Subitems with unexpected properties: {ill_formed_subitems_single}"


def test_query_edge_outwards(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    item_a = omni_views["ConnectionItemA"]
    item_b = omni_views["ConnectionItemB"]
    executor = _QueryExecutor(cognite_client, views=[item_a, item_b])
    properties = [
        "externalId",
        "name",
        {"outwards": [{"node": ["name", "externalId"]}, "type"]},
    ]
    flatten_props = {"name", "outwards", "externalId"}
    result = executor.list(item_a.as_id(), properties, limit=5)
    assert isinstance(result, list)
    assert len(result) > 0
    ill_formed_items = [item for item in result if not (set(item.keys()) <= flatten_props)]
    assert not ill_formed_items, f"Items with unexpected properties: {ill_formed_items}"
    ill_formed_subitems = [
        subitem
        for item in result
        for edge in item.get("outwards", [])
        for subitem in edge.get("node", [])
        if not (set(subitem.keys()) <= {"name", "externalId"})
    ]
    assert not ill_formed_subitems, f"Subitems with unexpected properties: {ill_formed_subitems}"


def test_query_list_primitive_properties(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    view = omni_views["PrimitiveNullable"]
    executor = _QueryExecutor(cognite_client, views=[view])
    properties = ["text", "boolean", "date"]
    result = executor.list(view.as_id(), properties, limit=5)

    assert isinstance(result, list)
    assert len(result) > 0
    properties_set = set(properties)
    ill_formed_items = [item for item in result if not (set(item.keys()) <= properties_set)]
    assert not ill_formed_items, f"Items with unexpected properties: {ill_formed_items}"


def test_aggregate_count(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    view = omni_views["PrimitiveRequired"]
    executor = _QueryExecutor(cognite_client, views=[view])
    result = executor.aggregate(view.as_id(), aggregates=dm.aggregations.Count(property="externalId"))

    assert isinstance(result, dict)
    assert "count" in result


def test_aggregate_count_with_group_by(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    view = omni_views["PrimitiveRequired"]
    executor = _QueryExecutor(cognite_client, views=[view])
    result = executor.aggregate(
        view.as_id(), aggregates=dm.aggregations.Count(property="externalId"), group_by="boolean"
    )
    assert isinstance(result, list)
    assert len(result) > 0


def test_histogram(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    view = omni_views["PrimitiveRequired"]
    executor = _QueryExecutor(cognite_client, views=[view])
    result = executor.aggregate(view.as_id(), aggregates=dm.aggregations.Histogram(property="float32", interval=100.0))

    assert isinstance(result, dict)
    assert "histogram" in result


def test_search(cognite_client: CogniteClient, omni_client: OmniClient, omni_views: dict[str, dm.View]) -> None:
    items = omni_client.primitive_required.list(limit=1)
    assert len(items) > 0
    item = items[0]
    word = item.text.split(" ", maxsplit=1)[0]

    view = omni_views["PrimitiveRequired"]
    executor = _QueryExecutor(cognite_client, views=[view])
    result = executor.search(view.as_id(), query=word, limit=5)

    assert isinstance(result, list)
    assert len(result) > 0
