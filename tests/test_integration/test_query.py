from __future__ import annotations

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from omni import OmniClient
    from omni import data_classes as dc
else:
    from omni_pydantic_v1 import OmniClient
    from omni_pydantic_v1 import data_classes as dc


def test_query_with_direct_relation(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_a(limit=-1).query(retrieve_other_direct=True, retrieve_self_direct=True)

    assert len(items) > 0
    incorrect_other = [
        item.other_direct
        for item in items
        if not (isinstance(item.other_direct, dc.ConnectionItemC) or item.other_direct is None)
    ]
    assert len(incorrect_other) == 0, f"Other direct relation should be set, got {incorrect_other}"
    incorrect_self = [
        item.self_direct
        for item in items
        if not (isinstance(item.self_direct, dc.ConnectionItemA) or item.self_direct is None)
    ]
    assert len(incorrect_self) == 0, f"Self direct relation should be set, got {incorrect_self}"


def test_query_limit_with_direct_relations(omni_client: OmniClient) -> None:
    items = omni_client.connection_item_a(limit=2).query(retrieve_self_direct=True, retrieve_other_direct=True)

    assert len(items) == 2


def test_query_filer_on_subsequent_node(omni_client: OmniClient) -> None:
    items = (
        omni_client.connection_item_c(limit=-1).connection_item_a(external_id_prefix="ConnectionItemA:Joseph").query()
    )

    assert len(items) > 0
    has_retrieved_a = False
    for item in items:
        for a_item in item.connection_item_a or []:
            assert a_item.external_id.startswith("ConnectionItemA:Joseph")
            has_retrieved_a = True
    assert has_retrieved_a, "No ConnectionItemA items were retrieved"


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


def test_query_limit(omni_client: OmniClient) -> None:
    items = omni_client.implementation_2(limit=-1).query()

    assert len(items) >= 200, "There should be more than 5,000 items of this type in the dataset"
