from __future__ import annotations

from collections.abc import Iterable

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
            read_fields="\n    ".join(properties_to_read_fields(view.properties.values())),
        )
        + "\n"
    )


def view_to_api(view: dm.View) -> str:
    ...


def properties_to_read_fields(properties: Iterable[dm.MappedPropertyDefinition | dm.ConnectionDefinition]) -> list[str]:
    fields = []
    for property_ in properties:
        snake_name = to_snake(property_.name)
        type_ = _to_python_type(property_.type)
        default = property_.default_value if isinstance(property_.type, dm.PropertyType) else "[]"
        if snake_name == property_.name:
            fields.append(f"{snake_name}: {type_} = {default}")
        else:
            fields.append(f'{snake_name}: {type_} = Field({default}, alias="{property_.name}")')
    return fields


def _to_python_type(type_: dm.DirectRelationReference | dm.PropertyType) -> str:
    if isinstance(type_, dm.DirectRelationReference):
        return "list[str]"
    if isinstance(type_, (dm.Int32, dm.Int64)):
        out_type = "int"
    elif isinstance(type_, (dm.Text)):
        out_type = "str"
    else:
        raise ValueError(f"Unknown type {type_}")

    # Todo use this when SDK has exposed the type
    # if isinstance(type_, dm.ListablePropertyType) and type_.is_list:
    if hasattr(type_, "is_list") and type_.is_list:
        out_type = "list[str]"
    else:
        out_type = f"Optional[{out_type}]"
    return out_type
