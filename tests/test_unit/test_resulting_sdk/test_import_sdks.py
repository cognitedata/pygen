"""
These are tests that apply to all generated SDKs.
"""

import importlib
from collections.abc import Iterable

import pytest
from cognite.client import CogniteClient

from tests.constants import EXAMPLE_SDKS, ExampleSDK


def example_sdk_generated() -> Iterable[ExampleSDK]:
    for sdk in EXAMPLE_SDKS:
        if not sdk.generate_sdk:
            continue
        yield pytest.param(sdk, id=sdk.top_level_package)


@pytest.mark.parametrize("example_sdk", example_sdk_generated())
def test_import_client(example_sdk: ExampleSDK, mock_cognite_client: CogniteClient) -> None:
    # Act
    module = vars(importlib.import_module(example_sdk.top_level_package))

    # Assert
    assert example_sdk.client_name in module
    assert module[example_sdk.client_name](mock_cognite_client)


@pytest.mark.parametrize("example_sdk", example_sdk_generated())
def test_import_data_class(example_sdk: ExampleSDK, mock_cognite_client: CogniteClient) -> None:
    # Act
    module = vars(importlib.import_module(f"{example_sdk.top_level_package}.data_classes"))

    # Assert
    assert "__all__" in module
    assert not (missing := set(module["__all__"]) - set(module)), f"Missing {missing}"
