from cognite.client import CogniteClient

from cognite import pygen
from movie_domain.client import MovieClient
from tests.constants import examples_dir


def main():
    client = MovieClient.from_toml("config.toml")
    c: CogniteClient = client.persons._client

    generator = pygen.SDKGenerator("shop", "Shop")

    data_model = c.data_modeling.data_models.retrieve(
        ("IntegrationTestsImmutable", "SHOP_Model", "2"), inline_views=True
    )[0]
    sdk = generator.generate_sdk(data_model)

    pygen.write_sdk_to_disk(sdk, examples_dir)


if __name__ == "__main__":
    main()
