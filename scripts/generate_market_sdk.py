from pathlib import Path

from cognite.pygen import write_sdk_to_disk
from cognite.pygen._generator import MultiModelSDKGenerator, _load_data_model
from cognite.pygen.utils.cdf import get_cognite_client_from_toml
from tests.constants import examples_dir


def main():
    c = get_cognite_client_from_toml("config.toml")
    data_models = _load_data_model(c, [("market", "CogPool", "3"), ("market", "PygenPool", "3")], print)

    top_level_package = "markets.client"
    sdk_generator = MultiModelSDKGenerator(top_level_package, "MarketClient", data_models, print)

    sdk = sdk_generator.generate_sdk()
    # Remove the date_transformation_pair as this is expected to be set manually
    client_dir = Path(top_level_package.replace(".", "/"))
    sdk.pop(client_dir / "data_classes" / "_date_transformation_pairs.py")
    sdk.pop(client_dir / "_api" / "date_transformation_pairs.py")

    write_sdk_to_disk(sdk, examples_dir)
    print("Market SDK Created!")


if __name__ == "__main__":
    main()
