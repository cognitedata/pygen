import pytest
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.pygen import _execute_query


@pytest.mark.skip("In progress")
def test_query_reverse_direct_relation_list(cognite_client: CogniteClient, omni_views: dict[str, dm.View]) -> None:
    item_e = omni_views["ConnectionItemE"]
    result = _execute_query(cognite_client, item_e, "list", ["name", "directReverseMulti"])

    assert result
