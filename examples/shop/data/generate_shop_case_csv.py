import random
from pathlib import Path

import pandas as pd
from faker import Faker

THIS = Path(__file__).resolve().parent


def main(case_count: int = 10):
    from movie_domain.client import MovieClient

    client = MovieClient.from_toml(THIS.parent.parent / "config.toml")
    client = client.actors._client

    dataset_id = client.data_sets.retrieve(external_id="src:shop").id

    available_sequences = [s.external_id for s in client.sequences.list(data_set_ids=dataset_id)]
    available_cut_files = [
        f.external_id for f in client.files.list(data_set_ids=dataset_id, external_id_prefix="shop:cut_file:")
    ]
    available_command_files = [
        f.external_id for f in client.files.list(data_set_ids=dataset_id, external_id_prefix="shop:commands:")
    ]
    fake = Faker()
    case_names = [fake.unique.first_name() for _ in range(case_count)]
    case_table = []
    cut_table = []
    bid_history_table = []
    config_table = []
    for case_no, case_name in enumerate(case_names):
        external_id = f"shop:case:{case_no}:{case_name}"
        start_time = fake.date_time_this_year()
        duration_min = random.randint(5, 120)
        end_time = start_time + pd.Timedelta(minutes=duration_min)
        case_table.append(
            {
                "external_id": external_id,
                "name": case_name,
                "scenario": f"Scenario {random.randint(1, 52)}",
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "bid": random.choice(available_sequences),
                "arguments": fake.company(),
                "source": random.choice(available_command_files),
                "runStatus": random.choice(["Running", "Completed", "Failed"]),
            }
        )
        cut_table.extend(
            {
                "external_id": external_id,
                "cut_file": random.choice(available_cut_files),
            }
            for _ in range(random.randint(1, 4))
        )
        bid_history_table.extend(
            {
                "external_id": external_id,
                "bid_matrix": random.choice(available_sequences),
            }
            for _ in range(random.randint(3, 7))
        )

        config_table.extend({"external_id": external_id, "config": fake.color_name()} for _ in range(2, 8))

    case_df = pd.DataFrame(case_table, columns=list(case_table[0]))
    cut_df = pd.DataFrame(cut_table, columns=list(cut_table[0]))
    bid_history_df = pd.DataFrame(bid_history_table, columns=list(bid_history_table[0]))
    config_df = pd.DataFrame(config_table, columns=list(config_table[0]))

    database_name = "shop"
    client.raw.tables.delete(database_name, name=["case", "cut", "bid_history", "config"])
    client.raw.rows.insert_dataframe(database_name, "case", dataframe=case_df, ensure_parent=True)
    client.raw.rows.insert_dataframe(database_name, "cut", dataframe=cut_df, ensure_parent=True)
    client.raw.rows.insert_dataframe(database_name, "bid_history", dataframe=bid_history_df, ensure_parent=True)
    client.raw.rows.insert_dataframe(database_name, "config", dataframe=config_df, ensure_parent=True)
    print("Done")  # noqa: T201


if __name__ == "__main__":
    main()
