from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

from cognite.client import data_modeling as dm
from jinja2 import Environment, PackageLoader, select_autoescape

from cognite.pygen._core import view_functions
from cognite.pygen.utils.text import as_plural, as_singular, to_snake

_env = Environment(
    loader=PackageLoader("cognite.pygen._core", "templates"),
    autoescape=select_autoescape(),
)


def view_to_data_classes(view: dm.View) -> str:
    type_data = _env.get_template("type_data.py.jinja")
    fields = properties_to_fields(view.properties.values())
    sources = properties_to_sources(view.properties.values())

    return (
        type_data.render(
            view_name=view.name,
            view_space=view.space,
            read_fields="\n    ".join(f.as_type_hint("read") for f in fields),
            write_fields="\n    ".join(f.as_type_hint("write") for f in fields),
            sources=f',\n{" "*16}'.join(sources),
        )
        + "\n"
    )


def view_to_api(view: dm.View, sdk_name: str) -> str:
    edges_apis = [
        property_to_edge_api(prop, view.name, view.space)
        for prop in view_functions.one_to_many_properties(view.properties.values())
    ]
    edges_helpers = [
        property_to_edge_helper(prop, view.name)
        for prop in view_functions.one_to_many_properties(view.properties.values())
    ]
    edge_snippets = [
        property_to_edge_snippets(prop, view.name)
        for prop in view_functions.one_to_many_properties(view.properties.values())
    ]

    type_api = _env.get_template("type_api.py.jinja")

    return (
        type_api.render(
            sdk_name=sdk_name,
            view_name=view.name,
            view_space=view.space,
            view_ext_id=view.external_id,
            view_version=view.version,
            view_snake_plural=as_plural(view.name),
            view_pascal_plural=as_plural(view.name).title(),
            edge_apis="\n\n".join(edges_apis),
            edge_helpers="\n".join(edges_helpers),
            edge_retrieve=(" " * 12).join(snippet.retrieve for snippet in edge_snippets),
            edge_list=(" " * 8).join(snippet.list for snippet in edge_snippets),
            init_edge_apis=(" " * 8).join(snippet.init for snippet in edge_snippets),
            set_retrieve_singular=(" " * 12).join(snippet.set_singular for snippet in edge_snippets),
            set_retrieve_plural=(" " * 12).join(snippet.set_plural for snippet in edge_snippets),
            set_list_plural=(" " * 8).join(snippet.set_plural for snippet in edge_snippets),
        )
        + "\n"
    )


def property_to_edge_api(prop: dm.ConnectionDefinition, view_name: str, view_space: str) -> str:
    edge_api = _env.get_template("edge_api.py.jinja")
    if isinstance(prop, dm.SingleHopConnectionDefinition):
        return edge_api.render(
            view_name=view_name,
            view_space=view_space,
            view_plural=as_plural(view_name),
            edge_name=as_singular(prop.name),
            edge_plural=prop.name,
            type_ext_id=prop.type.external_id,
        )
    raise NotImplementedError(f"Edge API for type={type(prop)} is not implemented")


def property_to_edge_helper(prop: dm.ConnectionDefinition, view_name: str) -> str:
    helper = _env.get_template("type_api_set_edge_helper.py.jinja")
    if isinstance(prop, dm.SingleHopConnectionDefinition):
        return helper.render(
            view_name=view_name,
            view_plural=as_plural(view_name),
        )
    raise NotImplementedError(f"Edge API for type={type(prop)} is not implemented")


@dataclass
class EdgeSnippets:
    init: str
    set_singular: str
    set_plural: str
    retrieve: str
    list: str


def property_to_edge_snippets(prop: dm.ConnectionDefinition, view_name: str) -> EdgeSnippets:
    if isinstance(prop, dm.SingleHopConnectionDefinition):
        return EdgeSnippets(
            f"self.{prop.name} = {view_name}{prop.name.title()}API(client)",
            f"{view_name.lower()}.{prop.name} = [edge.end_node.external_id for edge in edges]",
            f"self._set_{prop.name}({as_plural(view_name.lower())}, edges)",
            f"edges = self.{prop.name}.retrieve(external_id)",
            f"edges = self.{prop.name}.list(limit=-1)",
        )

    raise NotImplementedError(f"Edge API for type={type(prop)} is not implemented")


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
