import pytest

from cognite.pygen.data_classes import DomainModel, Field
from cognite.pygen.parser import parse_graphql
from tests.constants import CINEMATOGRAPHY


def parse_graphql_test_data():
    input_schema = (CINEMATOGRAPHY / "schema.graphql").read_text()
    person = DomainModel(
        name="PersonSimple", fields=[Field(name="name", type="str", is_required=True, is_named_type=True)]
    )
    movie = DomainModel(
        name="MovieSimple",
        fields=[
            Field(name="title", type="str", is_required=True, is_named_type=True),
            Field(name="director", type="PersonSimple", is_named_type=True),
            Field(name="actors", type="PersonSimple", is_list=True, is_named_type=True),
            Field(name="producers", type="PersonSimple", is_list=True, is_named_type=True),
            Field(name="release", type="Timestamp", is_named_type=True),
            Field(name="meta", type="JSONObject", is_named_type=True),
            Field(name="genres", type="str", is_list=True, is_named_type=True, is_required=True),
        ],
    )
    yield pytest.param(input_schema, [person, movie], id="Person & Movie")


@pytest.mark.parametrize("schema, expected_models", list(parse_graphql_test_data()))
def test_parse_graphql(schema: str, expected_models: list[DomainModel]):
    actual_models = parse_graphql(schema)

    assert sorted(actual_models, key=lambda m: m.name) == sorted(expected_models, key=lambda m: m.name)
