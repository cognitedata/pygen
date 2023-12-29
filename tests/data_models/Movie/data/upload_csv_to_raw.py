from pathlib import Path

import pandas as pd

from cognite.pygen.dm_clients.cdf.get_client import get_cognite_client

DATABASE_NAME = "movie"


def main():
    client = get_cognite_client()

    for file in Path("../../../../examples/movie_domain/data").glob("*.csv"):
        data = pd.read_csv(file)
        client.raw.rows.insert_dataframe(DATABASE_NAME, file.stem, data, ensure_parent=True)
        print(f"Uploaded {file.stem}")


if __name__ == "__main__":
    main()
