from __future__ import annotations

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from omni import OmniClient
    from omni import data_classes as odc
    from windmill import WindmillClient
    from windmill import data_classes as wdc
else:
    from omni_multi_pydantic_v1 import OmniClient
    from omni_multi_pydantic_v1 import data_classes as odc
    from windmill_pydantic_v1 import WindmillClient
    from windmill_pydantic_v1 import data_classes as wdc


def test_graphql_query(wind_client: WindmillClient) -> None:
    result = wind_client.graphql_query(
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


def test_query_cdf_external(omni_client: OmniClient) -> None:
    query = """{
  listCDFExternalReferencesListed{
    items{
       __typename
      timeseries {
        externalId
        name
      }
      externalId
      files{
        externalId
        name
      }
      sequences{
        externalId
        name

      }
    }
  }
}
"""
    result = omni_client.graphql_query(query)

    assert len(result) > 0
    assert isinstance(result[0], odc.CDFExternalReferencesListedGraphQL)
