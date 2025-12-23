from typing import Any

import pytest

from cognite.pygen._client import PygenClientConfig
from cognite.pygen._generation.python.example import ExampleClient, PrimitiveNullable, PrimitiveNullableWrite
from cognite.pygen._generation.python.example._data_class import PrimitiveNullableList


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
        "deletedTime": None,
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
                    "timestamp": "2021-07-02T17:40:00.000+00:00",
                    "date": "2021-07-02",
                    "json": {"key": "value"},
                }
            }
        },
    }


class TestExampleDTOs:
    def test_serialization(self, example_response: dict[str, Any]) -> None:
        dto = PrimitiveNullable.model_validate(example_response)
        assert isinstance(dto.as_write(), PrimitiveNullableWrite)
        assert dto.dump(format="instance") == example_response

    def test_list_to_pandas(self, example_response: dict[str, Any]) -> None:
        dto = PrimitiveNullable.model_validate(example_response)
        dto_list = PrimitiveNullableList([dto])
        df = dto_list.to_pandas()
        assert not df.empty
        assert df.loc[0, "external_id"] == dto.external_id
        assert dto_list._repr_html_() != ""
        assert dto_list.dump(format="instance") == [example_response]


class TestExampleAPI:
    def test_create_primitive_nullable(
        self,
        example_client: ExampleClient,
        example_response: dict[str, Any],
    ) -> None:
        client = example_client
        assert isinstance(client, ExampleClient)
