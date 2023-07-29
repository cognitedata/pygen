from yaml import safe_dump

from cognite import pygen
from cognite.pygen.utils.cdf import get_cognite_client_from_toml
from tests.constants import examples_dir, schemas_dir


def main():
    c = get_cognite_client_from_toml("config.toml")
    data_model = c.data_modeling.data_models.retrieve(
        ("IntegrationTestsImmutable", "SHOP_Model", "2"), inline_views=True
    )[0]

    generator = pygen.SDKGenerator("shop.client", "ShopClient", data_model)

    try:
        sdk = generator.generate_sdk()

        pygen.write_sdk_to_disk(sdk, examples_dir)
    except Exception:
        print("Failed to generate SDK")  # noqa
        raise
    else:
        print("Successfully generated SDK")  # noqa
        with (schemas_dir / "shop_data_model.yaml").open("w") as f:
            safe_dump(data_model.dump(camel_case=True), f)


if __name__ == "__main__":
    main()
