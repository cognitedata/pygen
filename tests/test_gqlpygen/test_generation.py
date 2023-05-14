import pytest

from cinematography_domain.schema import cine_schema
from cognite.dm_clients.domain_modeling import DomainModel, Schema
from cognite.gqlpygen.generator import PythonSDK, to_client_sdk
from tests.constants import CINEMATOGRAPHY


def generate_graphql_from_pydantic_data():
    expected = (CINEMATOGRAPHY / "schema.graphql").read_text()
    yield pytest.param(cine_schema, expected, id="Cinematography example")


@pytest.mark.parametrize("schema, expected_graphql", list(generate_graphql_from_pydantic_data()))
def test_generate_graphql_from_pydantic(schema: Schema[DomainModel], expected_graphql: str):
    assert schema.as_str() == expected_graphql


def generate_client_sdk_from_graphql_data():
    input_schema = (CINEMATOGRAPHY / "schema.graphql").read_text()
    client = (CINEMATOGRAPHY / "client.py").read_text()
    schema = (CINEMATOGRAPHY / "schema.py").read_text()

    yield pytest.param(
        input_schema,
        "CineClient",
        "cine_schema",
        PythonSDK(client, schema),
        id="Cinematography example",
    )


@pytest.mark.parametrize(
    "graphql_schema, client_name, schema_name, expected", list(generate_client_sdk_from_graphql_data())
)
def test_generate_client_sdk_from_graphql(
    graphql_schema: str, client_name: str, schema_name: str, expected: dict[str, str]
):
    actual = to_client_sdk(graphql_schema, client_name, schema_name)

    assert actual == expected
