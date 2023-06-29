import random
from pathlib import Path

import pandas as pd
from cognite.client.data_classes import Sequence

THIS = Path(__file__).resolve().parent


def main(sequence_count: int = 10, file_count: int = 10):
    from movie_domain.client import MovieClient

    client = MovieClient.from_toml(THIS.parent.parent / "config.toml")
    client = client.actors._client
    dataset_id = client.data_sets.retrieve(external_id="src:shop").id

    for sequence_no in range(sequence_count):
        columns = list(map(str, [-500.0, 10.1, 10.2, 10.3, 4_000.0]))

        sequence = Sequence(
            name=f"Bid Matrix {sequence_no}",
            external_id=f"shop:bid_matrix:{sequence_no}",
            columns=[{"valueType": "DOUBLE", "externalId": c} for c in columns],
            data_set_id=dataset_id,
        )

        rows = []
        for _ in range(1, 25):
            row = sorted(random.randint(-500, 2000) / 10 for _ in range(len(columns)))
            rows.append(row)
        df = pd.DataFrame(rows, columns=columns, index=range(1, 25))

        client.sequences.create(sequence)
        client.sequences.data.insert_dataframe(df, external_id=sequence.external_id)
        print(f"Uploaded {sequence.name}")  # noqa: T201

    commands_file = THIS / "commands.yaml"
    cut_file = THIS / "water_value_cut_file.dat"

    client.files.delete(external_id=[f"shop:commands:{file_no}" for file_no in range(2)])
    client.files.delete(external_id=[f"shop:cut_file:{file_no}" for file_no in range(2)])

    for file_no in range(file_count):
        client.files.upload(
            commands_file,
            external_id=f"shop:commands:{file_no}",
            name=f"Commands {file_no}",
            mime_type="text/plain",
            data_set_id=dataset_id,
        )

        client.files.upload(
            cut_file,
            external_id=f"shop:cut_file:{file_no}",
            name=f"Cut File {file_no}",
            mime_type="text/plain",
            data_set_id=dataset_id,
        )
        print(f"Uploaded files: {file_no}")  # noqa: T201


if __name__ == "__main__":
    main()
