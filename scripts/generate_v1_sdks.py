from cognite import pygen
from cognite.pygen.utils.cdf import load_cognite_client_from_toml
from tests.constants import REPO_ROOT


def main():
    c = load_cognite_client_from_toml("config.toml")
    examples_dir_pydantic_v1 = REPO_ROOT / "examples-pydantic-v1"

    pygen.generate_sdk(
        c,
        ("IntegrationTestsImmutable", "Movie", "2"),
        "movie_domain_pydantic_v1.client",
        "MovieClient",
        examples_dir_pydantic_v1,
        print,
        pydantic_version="v1",
        overwrite=True,
    )
    pygen.generate_sdk(
        c,
        ("IntegrationTestsImmutable", "SHOP_Model", "2"),
        "shop_pydantic_v1.client",
        "ShopClient",
        examples_dir_pydantic_v1,
        print,
        pydantic_version="v1",
        overwrite=True,
    )
    pygen.generate_sdk(
        c,
        [("market", "CogPool", "3"), ("market", "PygenPool", "3")],
        "markets_pydantic_v1.client",
        "MarketClient",
        examples_dir_pydantic_v1,
        print,
        pydantic_version="v1",
        overwrite=True,
    )


if __name__ == "__main__":
    main()
