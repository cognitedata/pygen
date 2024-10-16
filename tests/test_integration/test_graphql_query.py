from __future__ import annotations

from omni import OmniClient
from omni import data_classes as odc
from scenario_instance.client import ScenarioInstanceClient
from scenario_instance.client import data_classes as sidc
from windmill import WindmillClient
from windmill import data_classes as wdc


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


def test_query_cdf_external_timeseries_and_sequence(omni_client: OmniClient) -> None:
    query = """{
  getCDFExternalReferencesById(instance:
    {space: "omni-instances", externalId: "CDFExternalReferences:Jeffrey"}
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
    jeffery = result[0]
    assert isinstance(jeffery, odc.CDFExternalReferencesGraphQL)
    assert jeffery.external_id == "CDFExternalReferences:Jeffrey"
    assert isinstance(jeffery.timeseries, odc.TimeSeriesGraphQL)
    assert isinstance(jeffery.sequence, odc.SequenceGraphQL)
    jeffery.as_read()
    jeffery.as_write()


def test_query_cdf_external_listed_timeseries_and_sequence(omni_client: OmniClient) -> None:
    query = """{
  getCDFExternalReferencesListedById(instance:
    {space: "omni-instances", externalId: "CDFExternalReferencesListed:Brandon"}
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
    brandon = result[0]
    assert isinstance(brandon, odc.CDFExternalReferencesListedGraphQL)
    assert brandon.external_id == "CDFExternalReferencesListed:Brandon"
    assert len(brandon.timeseries or []) > 0
    assert isinstance(brandon.timeseries[0], odc.TimeSeriesGraphQL)
    assert len(brandon.sequences or []) > 0
    assert isinstance(brandon.sequences[0], odc.SequenceGraphQL)
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


def test_query_reverse_direct_relation(omni_client: OmniClient) -> None:
    result = omni_client.graphql_query(
        """{
  listConnectionItemE{
    items{
      __typename
      name
      directReverseMulti{
        items{
          externalId
          name
        }
      }
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
    assert first.direct_reverse_single is not None
    assert first.direct_reverse_multi is not None


def test_query_with_datapoints(scenario_instance_client: ScenarioInstanceClient) -> None:
    result = scenario_instance_client.graphql_query(
        """{
  listScenarioInstance(first: 1){
    items{
      __typename
      priceForecast{
        externalId
        name
        getDataPoints(granularity: "1d", aggregates: SUM){
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
    instance = result[0]
    assert isinstance(instance, sidc.ScenarioInstanceGraphQL)
    assert instance.price_forecast is not None
    assert instance.price_forecast.external_id is not None
    assert instance.price_forecast.name is not None
    assert instance.price_forecast.data is not None
    assert len(instance.price_forecast.data) > 0
    assert instance.price_forecast.data.sum[0] is not None
    assert instance.price_forecast.data.timestamp[0] is not None
