from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.pygen import _QueryExecutor


def test_query_reverse_direct_relation_list(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    item_e = omni_views["ConnectionItemE"]
    item_d = omni_views["ConnectionItemD"]
    executor = _QueryExecutor(cognite_client, views=[item_e, item_d])

    result = executor.execute_query(item_e.as_id(), "list", ["name", "directReverseMulti"])

    assert result
