from __future__ import annotations

import graphql

from cognite.dm_clients.misc import to_pascal
from cognite.gqlpygen.data_classes import DomainModel, DomainModels, Field

GRAPHQL_TO_PYTHON_TYPE_MAP = {
    "String": "str",
    "Float": "float",
    "Integer": "int",
    "Boolean": "bool",
}


def parse_graphql(schema_raw: str) -> DomainModels:
    schema = graphql.parse(schema_raw)

    domain_models = []
    for definition in schema.to_dict()["definitions"]:
        fields = [_parse_field(field) for field in definition["fields"]]
        domain_models.append(DomainModel(to_pascal(definition["name"]["value"]), fields))
    return DomainModels(domain_models)


def _parse_field(field: dict) -> Field:
    field_name = field["name"]["value"]
    is_required = False
    is_list = False
    field_type = field["type"]
    if field_type["kind"] == "non_null_type":
        is_required = True
        field_type = field_type["type"]
    if field_type["kind"] == "list_type":
        is_list = True
        field_type = field_type["type"]
    is_named_type = field_type["kind"] == "named_type"
    type_name = field_type["name"]["value"]

    return Field(
        name=field_name,
        type=GRAPHQL_TO_PYTHON_TYPE_MAP.get(type_name, type_name),
        is_list=is_list,
        is_required=is_required,
        is_named_type=is_named_type,
    )
