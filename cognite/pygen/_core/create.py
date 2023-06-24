from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
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
    fields = properties_to_fields(view.properties.values())
    sources = properties_to_sources(view.properties.values())

    return (
        type_data.render(
            view_name=view.name,
            view_space=view.space,
            read_fields="\n    ".join(f.as_type_hint("read") for f in fields),
            write_fields="\n    ".join(f.as_type_hint("write") for f in fields),
            sources=sources,
        )
        + "\n"
    )


def view_to_api(view: dm.View) -> str:
    ...


@dataclass
class Field:
    source_name: str
    type: str
    default: str
    is_nullable: bool

    @property
    def snake_name(self):
        return to_snake(self.source_name)

    @classmethod
    def from_property(cls, property_: dm.MappedPropertyDefinition | dm.ConnectionDefinition) -> Field:
        type_ = _to_python_type(property_.type)
        default = property_.default_value if isinstance(property_.type, dm.PropertyType) else "[]"
        is_nullable = isinstance(property_.type, dm.DirectRelationReference) or property_.nullable
        return cls(property_.name, type_, default, is_nullable)

    def as_type_hint(self, field_type: Literal["read", "write"]) -> str:
        is_nullable = self.is_nullable or (field_type == "read")
        default = self.default
        snake_name = self.snake_name
        if snake_name != self.source_name and field_type == "read":
            default = f'Field({default}, alias="{self.source_name}")'
        rhs = f" = {default}" if is_nullable else ""
        type_ = self.type
        if is_nullable and not type_.startswith("list"):
            type_ = f"Optional[{type_}]"
        return f"{snake_name}: {type_}{rhs}"


def properties_to_fields(
    properties: Iterable[dm.MappedPropertyDefinition | dm.ConnectionDefinition],
) -> list[Field]:
    return [Field.from_property(p) for p in properties]


def _to_python_type(type_: dm.DirectRelationReference | dm.PropertyType) -> str:
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
    return out_type


def properties_to_sources(properties: Iterable[dm.MappedPropertyDefinition | dm.ConnectionDefinition]) -> list[str]:
    properties_by_container_id: dict[dm.ContainerId, list[dm.MappedPropertyDefinition]] = defaultdict(list)
    for property_ in properties:
        if isinstance(property_, dm.MappedPropertyDefinition):
            properties_by_container_id[property_.container].append(property_)

    output = []
    for container_id, container_props in properties_by_container_id.items():
        prop_str = (
            ",\n                        ".join(f'"{p.name}": self.{to_snake(p.name)}' for p in container_props) + ","
        )
        output.append(
            """dm.NodeOrEdgeData(
                    source=dm.ContainerId("%s", "%s"),
                    properties={
                        %s
                    },
                ),"""
            % (container_id.space, container_id.external_id, prop_str)
        )
    return output
