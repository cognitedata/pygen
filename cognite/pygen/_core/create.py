from __future__ import annotations

from collections.abc import Iterable
from typing import Literal

from cognite.client import data_modeling as dm
from jinja2 import Environment, PackageLoader, select_autoescape

from cognite.pygen.ulits.text import to_snake


def view_to_data_classes(view: dm.View) -> str:
    env = Environment(
        loader=PackageLoader("cognite.pygen._core", "templates"),
        autoescape=select_autoescape(),
    )

    type_data = env.get_template("type_data.py.jinja")
    return (
        type_data.render(
            view_name=view.name,
            view_space=view.space,
            read_fields="\n    ".join(properties_to_fields(view.properties.values(), "read")),
            write_fields="\n    ".join(properties_to_fields(view.properties.values(), "write")),
        )
        + "\n"
    )


def view_to_api(view: dm.View) -> str:
    ...


def properties_to_fields(
    properties: Iterable[dm.MappedPropertyDefinition | dm.ConnectionDefinition],
    field_type: Literal["read", "write"] = "read",
) -> list[str]:
    fields = []
    for property_ in properties:
        snake_name = to_snake(property_.name)
        is_nullable = (
            (field_type == "read") or isinstance(property_.type, dm.DirectRelationReference) or property_.nullable
        )
        type_ = _to_python_type(property_.type, is_nullable=is_nullable)

        default = property_.default_value if isinstance(property_.type, dm.PropertyType) else "[]"
        if snake_name != property_.name and field_type == "read":
            default = f'Field({default}, alias="{property_.name}")'

        rhs = f" = {default}" if is_nullable else ""
        fields.append(f"{snake_name}: {type_}{rhs}")
    return fields


def _to_python_type(type_: dm.DirectRelationReference | dm.PropertyType, is_nullable: bool) -> str:
    if isinstance(type_, dm.DirectRelationReference):
        return "list[str]"
    if isinstance(type_, (dm.Int32, dm.Int64)):
        out_type = "int"
    elif isinstance(type_, dm.Boolean):
        out_type = "bool"
    elif isinstance(type_, (dm.Float32, dm.Float64)):
        out_type = "float"
    elif isinstance(type_, dm.Date):
        out_type = "date"
    elif isinstance(type_, dm.Timestamp):
        out_type = "datetime"
    elif isinstance(type_, dm.Json):
        out_type = "dict"
    elif isinstance(type_, dm.Text):
        out_type = "str"
    else:
        raise ValueError(f"Unknown type {type_}")

    # Todo use this when SDK has exposed the type
    # if isinstance(type_, dm.ListablePropertyType) and type_.is_list:
    if hasattr(type_, "is_list") and type_.is_list:
        out_type = "list[str]"
    else:
        if is_nullable:
            out_type = f"Optional[{out_type}]"
    return out_type
