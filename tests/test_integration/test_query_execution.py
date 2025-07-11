from typing import Any

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import filters
from omni import OmniClient

from cognite.pygen._query.constants import SelectedProperties
from cognite.pygen._query.interface import QueryExecutor


def test_query_reverse_direct_relation(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    item_e = omni_views["ConnectionItemE"]
    item_d = omni_views["ConnectionItemD"]
    executor = QueryExecutor(cognite_client, views=[item_e, item_d], unpack_edges="include")
    properties: list[str | dict[str, Any]] = [
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
    executor = QueryExecutor(cognite_client, views=[item_e, item_d], unpack_edges="include")
    properties: list[str | dict[str, Any]] = [
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
    executor = QueryExecutor(cognite_client, views=[item_a, item_b], unpack_edges="include")
    properties: list[str | dict[str, Any]] = [
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


def test_query_edge_outwards_skip_edge(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    item_a = omni_views["ConnectionItemA"]
    item_b = omni_views["ConnectionItemB"]
    executor = QueryExecutor(cognite_client, views=[item_a, item_b], unpack_edges="skip")
    properties: list[str | dict[str, Any]] = [
        "externalId",
        "name",
        {"outwards": ["name", "externalId"]},
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
        for subitem in item.get("outwards", [])
        if not (set(subitem.keys()) <= {"name", "externalId"})
    ]
    assert not ill_formed_subitems, f"Subitems with unexpected properties: {ill_formed_subitems}"


def test_query_list_primitive_properties(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    view = omni_views["PrimitiveNullable"]
    executor = QueryExecutor(cognite_client, views=[view], unpack_edges="include")
    properties: list[str | dict[str, Any]] = ["text", "boolean", "date"]
    result = executor.list(view.as_id(), properties, limit=5)

    assert isinstance(result, list)
    assert len(result) > 0
    properties_set = set(properties)
    ill_formed_items = [item for item in result if not (set(item.keys()) <= properties_set)]
    assert not ill_formed_items, f"Items with unexpected properties: {ill_formed_items}"


def test_aggregate_count(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    view = omni_views["PrimitiveRequired"]
    executor = QueryExecutor(cognite_client, views=[view], unpack_edges="include")
    result = executor.aggregate(view.as_id(), aggregates=dm.aggregations.Count(property="externalId"))

    assert isinstance(result, dict)
    assert "count" in result


def test_aggregate_count_filter_no_results(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    view = omni_views["PrimitiveRequired"]
    executor = QueryExecutor(cognite_client, views=[view], unpack_edges="include")
    result = executor.aggregate(
        view.as_id(),
        aggregates=dm.aggregations.Avg(property="int64"),
        filter=filters.Equals(["node", "externalId"], "non_existing_id"),
    )

    assert isinstance(result, dict)
    assert "avg" in result


def test_aggregate_count_with_group_by(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    view = omni_views["PrimitiveRequired"]
    executor = QueryExecutor(cognite_client, views=[view], unpack_edges="include")
    result = executor.aggregate(
        view.as_id(), aggregates=dm.aggregations.Count(property="externalId"), group_by="boolean"
    )
    assert isinstance(result, list)
    assert len(result) > 0


def test_histogram(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    view = omni_views["PrimitiveRequired"]
    executor = QueryExecutor(cognite_client, views=[view], unpack_edges="include")
    result = executor.aggregate(view.as_id(), aggregates=dm.aggregations.Histogram(property="float32", interval=100.0))

    assert isinstance(result, dict)
    assert "histogram" in result


def test_search(cognite_client: CogniteClient, omni_client: OmniClient, omni_views: dict[str, dm.View]) -> None:
    items = omni_client.primitive_required.list(limit=1)
    assert len(items) > 0
    item = items[0]
    word = item.text.split(" ", maxsplit=1)[0]

    view = omni_views["PrimitiveRequired"]
    executor = QueryExecutor(cognite_client, views=[view], unpack_edges="include")
    selected_properties: list[str | dict[str, Any]] = ["text", "boolean", "externalId"]
    result = executor.search(view.as_id(), selected_properties, query=word, limit=5)

    assert isinstance(result, list)
    assert len(result) > 0
    properties_set = set(selected_properties)
    ill_formed_items = [item for item in result if not (set(item.keys()) <= properties_set)]
    assert not ill_formed_items, f"Items with unexpected properties: {ill_formed_items}"


def test_search_nested_properties(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    view = omni_views["ConnectionItemE"]
    executor = QueryExecutor(cognite_client, views=[view], unpack_edges="include")
    selected_properties: list[str | dict[str, Any]] = [
        "externalId",
        "name",
        {"directReverseMulti": ["name", "externalId"]},
    ]
    result = executor.search(view.as_id(), selected_properties, limit=5)

    assert isinstance(result, list)
    assert len(result) > 0
    flatten_props = {"name", "directReverseMulti", "externalId"}
    ill_formed_items = [item for item in result if not (set(item.keys()) <= flatten_props)]
    assert not ill_formed_items, f"Items with unexpected properties: {ill_formed_items}"
    assert any(item.get("directReverseMulti") for item in result), "No subitems found"
    ill_formed_subitems = [
        subitem
        for item in result
        for subitem in item.get("directReverseMulti", [])
        if not (set(subitem.keys()) <= {"name", "externalId"})
    ]
    assert not ill_formed_subitems, f"Subitems with unexpected properties: {ill_formed_subitems}"


def test_query_list_root_nodes(cognite_client: CogniteClient) -> None:
    executor = QueryExecutor(cognite_client, unpack_edges="include")
    view_id = dm.ViewId("cdf_cdm", "CogniteAsset", "v1")

    no_parent = filters.Equals(view_id.as_property_ref("parent"), None)  # type: ignore[arg-type]
    result = executor.list(view_id, ["externalId", "name", "parent"], filter=no_parent, limit=5)

    assert isinstance(result, list)
    assert len(result) > 0
    assert all(item.get("parent") is None for item in result)


def test_query_list_edges(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    executor = QueryExecutor(cognite_client, unpack_edges="include")
    view_id = omni_views["ConnectionEdgeA"].as_id()
    selected_properties: SelectedProperties = ["externalId", "name", "startNode"]
    result = executor.list(view_id, selected_properties, limit=5)

    assert isinstance(result, list)
    assert len(result) > 0
    assert all(all(prop in item for prop in selected_properties) for item in result)


def test_query_aggregate_edges(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    executor = QueryExecutor(cognite_client, unpack_edges="include")
    view_id = omni_views["ConnectionEdgeA"].as_id()

    result = executor.aggregate(view_id, aggregates=dm.aggregations.Count(property="externalId"))

    assert isinstance(result, dict)
    assert "count" in result
    assert isinstance(result["count"], dict)
    assert "externalId" in result["count"]
    count = result["count"]["externalId"]
    assert isinstance(count, int)
    assert count > 0, "Count should be greater than 0"


def test_query_aggregate_edges_group_by(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    executor = QueryExecutor(cognite_client, unpack_edges="include")
    view_id = omni_views["ConnectionEdgeA"].as_id()

    result = executor.aggregate(view_id, aggregates=dm.aggregations.Count(property="externalId"), group_by="name")

    assert isinstance(result, list)
    assert len(result) > 0
    for item in result:
        assert "count" in item
        assert isinstance(item["count"], dict)
        assert "externalId" in item["count"]
        count = item["count"]["externalId"]
        assert isinstance(count, int)
        assert count > 0, "Count should be greater than 0"


def test_query_search_edges(
    cognite_client: CogniteClient, omni_views: dict[str, dm.View], omni_client: OmniClient
) -> None:
    executor = QueryExecutor(cognite_client, unpack_edges="include")
    view_id = omni_views["ConnectionEdgeA"].as_id()
    item = omni_client.connection_item_f.outwards_multi_edge.list(limit=1)
    assert len(item) == 1
    name = item[0].name

    selected_properties: SelectedProperties = ["externalId", "name", "startNode"]
    result = executor.search(view_id, query=name, limit=5)

    assert isinstance(result, list)
    assert len(result) > 0
    assert all(all(prop in item for prop in selected_properties) for item in result)
