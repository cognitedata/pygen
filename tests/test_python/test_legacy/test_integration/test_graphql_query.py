from __future__ import annotations

from omni import OmniClient
from omni import data_classes as odc
from wind_turbine import WindTurbineClient
from wind_turbine import data_classes as wdc


def test_graphql_query(turbine_client: WindTurbineClient) -> None:
    result = turbine_client.graphql_query(
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


def test_query_cdf_external_timeseries_and_sequence(omni_client: OmniClient) -> None:
    query = """{
  getCDFExternalReferencesById(instance:
    {space: "sp_omni_instances", externalId: "CDFExternalReferences:Colleen"}
  ){
    items{
       __typename
      space
      externalId
      createdTime
      lastUpdatedTime

      timeseries {
        externalId
        name
      }

      sequence{
        externalId
        name
        columns{
          externalId
          valueType
        }
      }
    }
  }
}
"""
    result = omni_client.graphql_query(query)

    assert len(result) > 0
    colleen = result[0]
    assert isinstance(colleen, odc.CDFExternalReferencesGraphQL)
    assert colleen.external_id == "CDFExternalReferences:Colleen"
    assert isinstance(colleen.timeseries, odc.TimeSeriesGraphQL)
    assert isinstance(colleen.sequence, odc.SequenceGraphQL)
    colleen.as_read()
    colleen.as_write()


def test_query_cdf_external_listed_timeseries_and_sequence(omni_client: OmniClient) -> None:
    query = """{
  getCDFExternalReferencesListedById(instance:
    {space: "sp_omni_instances", externalId: "CDFExternalReferencesListed:Steven"}
  ){
    items{
       __typename
      space
      externalId
      createdTime
      lastUpdatedTime

      timeseries {
        externalId
        name
      }

      sequences{
        externalId
        name
        columns{
          externalId
          valueType
        }
      }
    }
  }
}
"""
    result = omni_client.graphql_query(query)

    assert len(result) > 0
    steven = result[0]
    assert isinstance(steven, odc.CDFExternalReferencesListedGraphQL)
    assert steven.external_id == "CDFExternalReferencesListed:Steven"
    assert steven.timeseries is not None
    assert len(steven.timeseries) > 0
    assert isinstance(steven.timeseries[0], odc.TimeSeriesGraphQL)
    assert steven.sequences is not None
    assert len(steven.sequences) > 0
    assert isinstance(steven.sequences[0], odc.SequenceGraphQL)
    for item in result:
        assert isinstance(item, odc.CDFExternalReferencesListedGraphQL)
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


def test_query_reverse_direct_relation(omni_client: OmniClient) -> None:
    result = omni_client.graphql_query(
        """{
  listConnectionItemE{
    items{
      __typename
      name
      directReverseSingle{
        name
        externalId
      }
    }
  }
}"""
    )

    assert len(result) > 0
    first = result[0]
    assert isinstance(first, odc.ConnectionItemEGraphQL)
    direct_reverse_single = [
        item.direct_reverse_single
        for item in result
        if isinstance(item, odc.ConnectionItemEGraphQL) and item.direct_reverse_single
    ]

    assert len(direct_reverse_single) > 0, "No direct reverse single found"


def test_query_with_datapoints(turbine_client: WindTurbineClient) -> None:
    result = turbine_client.graphql_query(
        """{
  listMetmast(first: 1){
    items{
      __typename
      temperature{
        externalId
        name
        getDataPoints(granularity: "1d", aggregates: SUM, first: 100){
          items{
            timestamp
            sum
          }
        }
      }
    }
  }
}
"""
    )
    assert len(result) == 1
    item = result[0]
    assert isinstance(item, wdc.MetmastGraphQL)
    assert item.temperature is not None
    assert item.temperature.data is not None
    assert item.temperature.data.sum is not None
    assert len(item.temperature.data.sum) > 0
    assert item.temperature.data.timestamp is not None
    assert len(item.temperature.data.timestamp) > 0


def test_query_latest_datapoint(turbine_client: WindTurbineClient) -> None:
    result = turbine_client.graphql_query(
        """{
  listMetmast(first: 1){
    items{
      __typename
      temperature{
        externalId
        name
        getLatestDataPoint{
          items{
            timestamp
            value
          }
        }
      }
    }
  }
}
"""
    )
    assert len(result) == 1
    item = result[0]
    assert isinstance(item, wdc.MetmastGraphQL)
    assert item.temperature is not None
    assert item.temperature.latest is not None
    assert item.temperature.latest.timestamp is not None
    assert item.temperature.latest.value is not None
