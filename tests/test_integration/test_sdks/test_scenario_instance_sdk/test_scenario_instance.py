from datetime import datetime
from zoneinfo import ZoneInfo

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
    data = client.scenario_instance.price_forecast(country="Norway", market="Day-Ahead", limit=5).retrieve_latest()

    # Assert
    assert len(data) == 5


def test_retrieve_dataframe_in_tz(client: ScenarioInstanceClient) -> None:
    # Arrange
    start = datetime(2023, 9, 23, tzinfo=ZoneInfo("Europe/Oslo"))
    end = datetime(2023, 9, 24, tzinfo=ZoneInfo("Europe/Oslo"))

    # Act
    data = client.scenario_instance.price_forecast(
        price_area="NO", scenario_prefix="scenario", limit=5
    ).retrieve_dataframe_in_tz(start=start, end=end, aggregates="mean", granularity="1h")

    # Assert
    assert len(data) == 5


def test_retrieve_dataframe(client: ScenarioInstanceClient) -> None:
    # Act
    data = client.scenario_instance.price_forecast(min_start=datetime(2023, 9, 23), limit=5).retrieve_dataframe(
        aggregates="max", granularity="1d"
    )

    # Assert
    assert len(data) == 5


def test_retrieve_arrays(client: ScenarioInstanceClient) -> None:
    # Act
    data = client.scenario_instance.price_forecast(aggregation="mean", limit=5).retrieve_arrays(
        aggregates="max", granularity="1d"
    )

    # Assert
    assert len(data) == 5


def test_retrieve(client: ScenarioInstanceClient) -> None:
    # Act
    data = client.scenario_instance.price_forecast(country_prefix="N", limit=5).retrieve()

    # Assert
    assert len(data) == 5


def test_plot(client: ScenarioInstanceClient) -> None:
    # Act
    client.scenario_instance.price_forecast(country_prefix="N", limit=5).plot()
