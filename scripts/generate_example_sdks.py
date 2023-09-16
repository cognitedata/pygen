"""

"""
import argparse
from pathlib import Path
from cognite.pygen.utils.helper import get_pydantic_version
from dataclasses import dataclass
from cognite.client.data_classes.data_modeling import DataModelId
from cognite.pygen.utils.cdf import load_cognite_client_from_toml


def main():
    pydantic_version = get_pydantic_version()
    print(f"Detected pydantic version: {pydantic_version}")

    args = parser.parse_args()


# Plan:
# 1. Get pydantic version based on environment.
# 2. Iterate through all the examples and generate the SDKs.
# 3. Pop the manually controlled files.
# 4. Write the generated files to the correct location.

if __name__ == "__main__":
    main()
