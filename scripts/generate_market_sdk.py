from pathlib import Path

from cognite.pygen import write_sdk_to_disk
from cognite.pygen._generator import SDKGenerator, _load_data_model
from cognite.pygen.utils.cdf import load_cognite_client_from_toml
from tests.constants import examples_dir


def main():
    c = load_cognite_client_from_toml("config.toml")
    data_models = _load_data_model(c, [("market", "CogPool", "3"), ("market", "PygenPool", "3")], print)

    top_level_package = "markets.client"
    sdk_generator = SDKGenerator(
        top_level_package=top_level_package, client_name="MarketClient", data_model=data_models, logger=print
    )

    sdk = sdk_generator.generate_sdk()
    # Remove the date_transformation_pair as this is expected to be set manually
    client_dir = Path(top_level_package.replace(".", "/"))
    sdk.pop(client_dir / "data_classes" / "_date_transformation_pairs.py")
    sdk.pop(client_dir / "_api" / "date_transformation_pairs.py")

    write_sdk_to_disk(sdk, examples_dir, overwrite=True, format_code=True)
    print("Market SDK Created!")


if __name__ == "__main__":
    main()
