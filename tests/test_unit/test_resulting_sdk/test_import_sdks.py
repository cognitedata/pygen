"""
These are tests that apply to all generated SDKs.
"""

import importlib
from collections.abc import Iterable

import pytest
from cognite.client import CogniteClient

from tests.constants import EXAMPLE_SDKS, ExampleSDK


def example_sdk_generated(skip_typed: bool = False) -> Iterable[ExampleSDK]:
    for sdk in EXAMPLE_SDKS:
        if not sdk.generate_sdk:
            continue
        if skip_typed and sdk.is_typed:
            continue
        yield pytest.param(sdk, id=sdk.top_level_package)  # type: ignore[misc]


@pytest.mark.parametrize("example_sdk", example_sdk_generated())
def test_import_client(example_sdk: ExampleSDK, mock_cognite_client: CogniteClient) -> None:
    # Act
    module = vars(importlib.import_module(example_sdk.top_level_package))

    # Assert
    if not example_sdk.is_typed:
        assert example_sdk.client_name in module
        assert module[example_sdk.client_name](mock_cognite_client)


@pytest.mark.parametrize("example_sdk", example_sdk_generated(skip_typed=True))
def test_import_data_class(example_sdk: ExampleSDK, mock_cognite_client: CogniteClient) -> None:
    # Act
    module = vars(importlib.import_module(f"{example_sdk.top_level_package}.data_classes"))

    # Assert
    assert "__all__" in module
    missing = set(module["__all__"]) - set(module)
    assert not missing, f"Missing {missing}"
