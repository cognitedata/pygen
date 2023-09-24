import json
import random
from datetime import datetime, timedelta
from itertools import product
from zoneinfo import ZoneInfo

import pandas as pd
from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeries

from cognite.pygen.utils.cdf import load_cognite_client_from_toml
from cognite.pygen.utils.helper import chdir
from tests.constants import REPO_ROOT

from scenario_instance.client.data_classes import ScenarioInstanceApply
from scenario_instance.client import ScenarioInstanceClient

DATE_FORMAT = "%Y-%m-%d"


def populate_data(
    client: CogniteClient,
    scenario_names: list[str],
    countries_by_price_area: dict[str, str],
    markets: list[str],
    instance_by_start_time: dict[pd.Timestamp, pd.DatetimeIndex],
    dataset_id: int,
    batch_size: int,
) -> None:
    instance_client = ScenarioInstanceClient(client)
    scenario_instances: list[ScenarioInstanceApply] = []
    timeseries: list[TimeSeries] = []
    datapoints: list[pd.Series] = []
    total = (
        len(countries_by_price_area)
        * len(instance_by_start_time)
        * (len(scenario_names) + 4)
        * len(next(iter(instance_by_start_time.values())))
        * len(markets)
    )
    count = 0
    for (price_area, country), start_time in product(countries_by_price_area.items(), instance_by_start_time):
        for market in markets:
            for instance in instance_by_start_time[start_time]:
                prefix_external_id = f"scenario_instance:{price_area}:{market}:{start_time.strftime(DATE_FORMAT)}:{instance.strftime(DATE_FORMAT)}:"
                scenario_data = []
                for scenario_name in scenario_names:
                    ins = ScenarioInstanceApply(
                        external_id=f"{prefix_external_id}{scenario_name}",
                        market=market,
                        country=country,
                        price_area=price_area,
                        scenario=scenario_name,
                        start=start_time,
                        instance=instance,
                        price_forecast=f"{prefix_external_id}{scenario_name}",
                    )
                    scenario_instances.append(ins)
                    ts = TimeSeries(
                        external_id=ins.price_forecast,
                        name=f"Start={ins.start.strftime(DATE_FORMAT)}, Scenario={scenario_name} Instance={ins.instance.strftime(DATE_FORMAT)}",
                        data_set_id=dataset_id,
                        is_string=False,
                        metadata=json.loads(ins.model_dump_json(exclude_none=True)),
                    )
                    start = ins.start.replace(tzinfo=None)
                    index = pd.date_range(start, end=start + timedelta(days=14), freq="1H", tz="Europe/Oslo")
                    data = pd.Series(
                        index=index, data=[random.random() * 100 for _ in range(len(index))], name=ins.price_forecast
                    )
                    datapoints.append(data)
                    timeseries.append(ts)
                    scenario_data.append(data)

                scenario_df = pd.concat(scenario_data, axis=1)
                for aggregation in ["mean", "median", 0.1, 0.9]:
                    if isinstance(aggregation, str):
                        agg = scenario_df.agg(aggregation, axis=1)
                        agg_name = aggregation
                    else:
                        agg = scenario_df.quantile(aggregation, axis=1)
                        agg_name = f"{100*aggregation}%"
                    ins = ScenarioInstanceApply(
                        external_id=f"{prefix_external_id}{agg_name}",
                        country=country,
                        price_area=price_area,
                        start=start_time,
                        instance=instance,
                        price_forecast=f"{prefix_external_id}{agg_name}",
                        aggregation=agg_name,
                    )
                    scenario_instances.append(ins)
                    ts = TimeSeries(
                        external_id=ins.price_forecast,
                        name=f"Start={ins.start.strftime(DATE_FORMAT)}, Aggregation={agg_name} Instance={ins.instance.strftime(DATE_FORMAT)}",
                        data_set_id=dataset_id,
                        is_string=False,
                        metadata=json.loads(ins.model_dump_json(exclude_none=True)),
                    )
                    agg.name = ins.price_forecast
                    timeseries.append(ts)
                    datapoints.append(agg)

                scenario_data.clear()
                if len(timeseries) > batch_size:
                    print(f"Inserting {len(timeseries)} timeseries, datapoints, and ScenarioInstance nodes.")
                    client.time_series.upsert(timeseries, mode="replace")
                    count += len(timeseries)
                    print(f"Inserted {len(timeseries)} timeseries, accumulated {count:,}/{total:,}")

                    df = pd.concat(datapoints, axis=1)
                    client.time_series.data.insert_dataframe(df, external_id_headers=True)
                    print(f"Inserted dataframe of {'x'.join(map(str, df.shape))}.")

                    instance_client.scenario_instance.apply(scenario_instances)
                    timeseries.clear()
                    datapoints.clear()
                    scenario_instances.clear()


def main():
    with chdir(REPO_ROOT):
        client = load_cognite_client_from_toml()
        dataset_id = client.data_sets.retrieve(external_id="uc:scenario:instance").id
        oslo_tz = "Europe/Oslo"
        oslo = ZoneInfo(oslo_tz)
        today = datetime.now(tz=oslo).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

        start_times = pd.date_range(today - timedelta(days=2), today + timedelta(days=5), tz=oslo_tz, freq="1D")
        instance_by_start_time: dict[pd.Timestamp, pd.DatetimeIndex] = {
            start_time: pd.date_range(
                start_time - timedelta(days=14),
                end=start_time,
                tz=oslo_tz,
                freq="1D",
            )
            for start_time in start_times
        }
        scenario_names = [f"scenario{no}" for no in range(1, 4)]

        markets = ["Day-ahead", "aFRR", "mFRR", "Intraday"]
        countries_by_code = dict(zip(["NO", "SE", "DK", "FI"], ["Norway", "Sweden", "Denmark", "Finland"]))
        price_areas = [f"NO{no}" for no in "12345"] + [f"SE{no}" for no in "1234"] + [f"DK{no}" for no in "12"] + ["FI"]
        countries_by_price_area = {price_area: countries_by_code[price_area[:2]] for price_area in price_areas}

        populate_data(
            client, scenario_names, countries_by_price_area, markets, instance_by_start_time, dataset_id, batch_size=500
        )


if __name__ == "__main__":
    main()
