from typing import Any

import pytest

from cognite.pygen._client import PygenClientConfig
from cognite.pygen._generation.python.example import ExampleClient, PrimitiveNullableWrite, PrimitiveNullable


@pytest.fixture(scope="session")
def example_client(pygen_client_config: PygenClientConfig) -> ExampleClient:
    return ExampleClient(config=pygen_client_config)

@pytest.fixture(scope="session")
def example_response() -> dict[str, Any]:
    return {
        "instanceType": "node",
        "space": "my_space",
        "externalId": "example_id",
        "createdTime": 1625247600000,
        "lastUpdatedTime": 1625247600000,
        "version": 1,
        "properties": {
            "sp_pygen_models": {
                "PrimitiveNullable/v1": {
                    "text": "example text",
                    "boolean": True,
                    "float32": 1.23,
                    "float64": 4.56,
                    "int32": 123,
                    "int64": 456,
                    "timestamp": 1625247600000,
                    "date": "2021-07-02",
                    "json": {"key": "value"},
                }
            }
        }
    }

class TestExampleDTOs:
    def test_serialization(self, example_response: dict[str, Any]) -> None:
        dto = PrimitiveNullable.model_validate(example_response)
        assert isinstance(dto.as_write(), PrimitiveNullableWrite)
        assert dto.dump(format="instance") == example_response

class TestExampleAPI:
    def test_create_primitive_nullable(
        self,
        example_client: ExampleClient,
        example_response: dict[str, Any],
    ) -> None:
        client = example_client
        client.primitive_nullable
