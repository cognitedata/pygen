from __future__ import annotations

import datetime

import pytest
from cognite.client import CogniteClient

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from windmill import WindmillClient
    from windmill import data_classes as wdc
else:
    from windmill_pydantic_v1 import WindmillClient
    from windmill_pydantic_v1 import data_classes as wdc


def test_graphql_query(wind_client: WindmillClient) -> None:
    result = wind_client.query(
        """{
  listBlade(first: 1) {
    items {
      name
      __typename
    }
   }
}"""
    )
    assert len(result) == 1
    assert isinstance(result[0], wdc.BladeGraphQL)
    assert result[0].name

