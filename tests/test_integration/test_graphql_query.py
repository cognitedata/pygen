from __future__ import annotations

from cognite.client.data_classes import TimeSeries

from tests.constants import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from omni import OmniClient
    from omni import data_classes as odc
    from windmill import WindmillClient
    from windmill import data_classes as wdc
else:
    from omni_pydantic_v1 import OmniClient
    from omni_pydantic_v1 import data_classes as odc
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
      createdTime
      lastUpdatedTime
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
    assert len(result[0].timeseries) > 0
    assert isinstance(result[0].timeseries[0], TimeSeries)
    item: odc.CDFExternalReferencesListedGraphQL
    for item in result:
        item.as_read()
        item.as_write()


def test_query_paging(omni_client: OmniClient) -> None:
    result = omni_client.graphql_query(
        """{
  listImplementation2{
    items{
      __typename
      externalId
    }
    pageInfo{
      hasNextPage
      startCursor
      endCursor
      hasPreviousPage
    }
  }
}"""
    )
    assert result.page_info is not None
    assert result.page_info.has_next_page is True
