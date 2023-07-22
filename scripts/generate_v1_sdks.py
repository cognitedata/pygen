from cognite.client import CogniteClient

from cognite import pygen
from movie_domain.client import MovieClient
from tests.constants import examples_dir_pydantic_v1


def main():
    client = MovieClient.from_toml("config.toml")
    c: CogniteClient = client.persons._client

    pygen.generate_sdk(
        c,
        ("IntegrationTestsImmutable", "Movie", "2"),
        "movie_domain_pydantic_v1.client",
        "MovieClient",
        examples_dir_pydantic_v1,
        print,
        pydantic_version="v1",
    )
    pygen.generate_sdk(
        c,
        ("IntegrationTestsImmutable", "SHOP_Model", "2"),
        "shop_pydantic_v1.client",
        "ShopClient",
        examples_dir_pydantic_v1,
        print,
        pydantic_version="v1",
    )
    pygen.generate_multimodel_sdk(
        c,
        [("market", "CogPool", "3"), ("market", "PygenPool", "3")],
        "markets_pydantic_v1.client",
        "MarketClient",
        examples_dir_pydantic_v1,
        print,
        pydantic_version="v1",
    )


if __name__ == "__main__":
    main()
