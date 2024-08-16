from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from tests.constants import IS_PYDANTIC_V1

if IS_PYDANTIC_V1:
    from scenario_instance_pydantic_v1.client import ScenarioInstanceClient
else:
    from scenario_instance.client import ScenarioInstanceClient


def test_scenario_instance_list_timeseries(client: ScenarioInstanceClient) -> None:
    # Act
    timeseries = client.scenario_instance.price_forecast.list(country="Norway", market="Day-ahead", limit=5)

    # Assert
    assert len(timeseries) == 5


def test_scenario_list_timeseries_empty_result(client: ScenarioInstanceClient) -> None:
    # Act
    timeseries = client.scenario_instance.price_forecast.list(country="NonExistentCountry", limit=5)

    # Assert
    assert len(timeseries) == 0


def test_scenario_instance_list_timeseries_no_filter(client: ScenarioInstanceClient) -> None:
    # Act
    timeseries = client.scenario_instance.price_forecast.list(limit=5)

    # Assert
    assert len(timeseries) == 5


def test_retrieve_latest(client: ScenarioInstanceClient) -> None:
    # Act
    data = client.scenario_instance.price_forecast(country="Norway", market="Day-ahead", limit=5).retrieve_latest()

    # Assert
    assert len(data) == 5


def test_retrieve_dataframe_in_tz(client: ScenarioInstanceClient) -> None:
    # Arrange
    start = datetime(2023, 9, 23, tzinfo=ZoneInfo("Europe/Oslo"))
    end = datetime(2023, 9, 24, tzinfo=ZoneInfo("Europe/Oslo"))

    # Act
    data = client.scenario_instance.price_forecast(
        price_area="NO1", scenario_prefix="scenario", limit=3
    ).retrieve_dataframe_in_tz(start=start, end=end, aggregates="average", granularity="1h")

    # Assert
    assert isinstance(data, pd.DataFrame)
    assert len(data.columns) == 3


def test_retrieve_dataframe(client: ScenarioInstanceClient) -> None:
    # Act
    data = client.scenario_instance.price_forecast(min_start=datetime(2023, 9, 23), limit=2).retrieve_dataframe(
        aggregates="max", granularity="1d"
    )

    # Assert
    assert isinstance(data, pd.DataFrame)
    assert len(data.columns) == 2


def test_retrieve_dataframe_column_names_aggregate_and_granularity(client: ScenarioInstanceClient) -> None:
    # Act
    data = client.scenario_instance.price_forecast(country="Norway", limit=1).retrieve_dataframe(
        aggregates=["min", "max"],
        granularity="1d",
        include_aggregate_name=True,
        include_granularity_name=True,
        column_names="country",
    )

    # Assert
    assert isinstance(data, pd.DataFrame)
    assert len(data.columns) == 2
    assert sorted(data.columns) == sorted(["Norway|min|1d", "Norway|max|1d"])


def test_retrieve_dataframe_column_names_aggregate(client: ScenarioInstanceClient) -> None:
    # Act
    data = client.scenario_instance.price_forecast(country="Norway", limit=1).retrieve_dataframe(
        aggregates=["min", "max"],
        granularity="1d",
        include_aggregate_name=True,
        include_granularity_name=False,
        column_names="country",
    )

    # Assert
    assert isinstance(data, pd.DataFrame)
    assert len(data.columns) == 2
    assert sorted(data.columns) == sorted(["Norway|min", "Norway|max"])


def test_retrieve_dataframe_column_names(client: ScenarioInstanceClient) -> None:
    # Act
    data = client.scenario_instance.price_forecast(country="Norway", limit=1).retrieve_dataframe(
        aggregates="min",
        granularity="1d",
        include_aggregate_name=False,
        include_granularity_name=False,
        column_names="country",
    )

    # Assert
    assert isinstance(data, pd.DataFrame)
    assert len(data.columns) == 1
    assert data.columns[0] == "Norway"


def test_retrieve_dataframe_multiple_column_names(client: ScenarioInstanceClient) -> None:
    # Act
    data = client.scenario_instance.price_forecast(country="Norway", price_area="NO1", limit=1).retrieve_dataframe(
        aggregates="min",
        granularity="1d",
        include_aggregate_name=False,
        include_granularity_name=False,
        column_names=["country", "priceArea"],
    )

    # Assert
    assert isinstance(data, pd.DataFrame)
    assert len(data.columns) == 1
    assert data.columns[0] == "Norway-NO1"


def test_retrieve_dataframe_multiple_column_names_missing_values(client: ScenarioInstanceClient) -> None:
    # Act
    data = client.scenario_instance.price_forecast(aggregation="mean", limit=1).retrieve_dataframe(
        aggregates="min",
        granularity="6h",
        include_aggregate_name=False,
        include_granularity_name=False,
        column_names=["scenario", "market"],
    )

    # Assert
    assert isinstance(data, pd.DataFrame)
    assert len(data.columns) == 1
    assert data.columns[0] == "MISSING-MISSING"


def test_retrieve_arrays(client: ScenarioInstanceClient) -> None:
    # Act
    data = client.scenario_instance.price_forecast(aggregation="mean", limit=1).retrieve_arrays(
        aggregates="max", granularity="1d"
    )

    # Assert
    assert len(data) == 1


def test_retrieve(client: ScenarioInstanceClient) -> None:
    # Act
    data = client.scenario_instance.price_forecast(country_prefix="N", limit=5).retrieve(
        start=datetime(2023, 9, 23), end=datetime(2023, 9, 24)
    )

    # Assert
    assert len(data) == 5


def test_paging(client: ScenarioInstanceClient) -> None:
    # Act
    # The query endpoint can return a maximum of 10_000 rows, so we set the limit to 10_001 to test paging
    timeseries = client.scenario_instance.price_forecast.list(limit=10_001)

    # Assert
    assert len(timeseries) == 10_001
