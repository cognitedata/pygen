from cognite.client import CogniteClient

from cognite import pygen
from movie_domain.client import MovieClient
from tests.constants import examples_dir


def main():
    client = MovieClient.from_toml("config.toml")
    c: CogniteClient = client.persons._client
    sdk = pygen.generate_multimodel_sdk(
        c,
        model_ids=[("market", "CogPool", "1"), ("market", "PygenPool", "1")],
        top_level_package="markets.client",
        client_name="MarketClient",
        output_dir=examples_dir,
        logger=print,
    )

    pygen.write_sdk_to_disk(sdk, examples_dir)


if __name__ == "__main__":
    main()
