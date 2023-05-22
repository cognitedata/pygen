from __future__ import annotations

import graphql

from cognite.pygen.data_classes import DomainModel, DomainModels, Field
from cognite.pygen.dm_clients.misc import to_pascal

GRAPHQL_TO_PYTHON_TYPE_MAP = {
    "String": "str",
    "Int": "int",
    "Float": "float",
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
    while True:
        if field_type["kind"] == "non_null_type":
            is_required = True
            field_type = field_type["type"]
        elif field_type["kind"] == "list_type":
            is_list = True
            field_type = field_type["type"]
        else:
            break
    is_named_type = field_type["kind"] == "named_type"
    type_name = field_type["name"]["value"]

    return Field(
        name=field_name,
        type=GRAPHQL_TO_PYTHON_TYPE_MAP.get(type_name, type_name),
        is_list=is_list,
        is_required=is_required,
        is_named_type=is_named_type,
    )
