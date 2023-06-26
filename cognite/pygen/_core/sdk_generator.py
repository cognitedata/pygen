from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from cognite.client import data_modeling as dm
from cognite.client._version import __version__ as cognite_sdk_version
from cognite.client.data_classes.data_modeling.views import ViewDirectRelation
from jinja2 import Environment, PackageLoader, select_autoescape
from pydantic.version import VERSION as PYDANTIC_VERSION

from cognite.pygen._core import view_functions
from cognite.pygen._version import __version__
from cognite.pygen.utils.text import to_pascal, to_snake


class SDKGenerator:
    def __init__(self, sdk_name: str, client_name_pascal: str):
        self.sdk_name = sdk_name
        self.client_name_pascal = client_name_pascal
        self._env = Environment(
            loader=PackageLoader("cognite.pygen._core", "templates"),
            autoescape=select_autoescape(),
        )
        self._dependencies_by_view_name = defaultdict(set)
        self._view_names = set()
        self._data_model_space = None
        self._data_model_external_id = None
        self._data_model_version = None
        self._static_dir = Path(__file__).parent / "static"

    def data_model_to_sdk(self, data_model: dm.DataModel) -> dict[Path, str]:
        self._data_model_space = data_model.space
        self._data_model_external_id = data_model.external_id
        self._data_model_version = data_model.version

        client_dir = Path(self.sdk_name) / "client"
        data_classes_dir = client_dir / "data_classes"
        api_dir = client_dir / "_api"
        sdk = {(api_dir / "__init__.py"): ""}
        for view in data_model.views:
            view_snake_plural = to_snake(view.name, pluralize=True)
            try:
                sdk[data_classes_dir / f"_{view_snake_plural}.py"] = self.view_to_data_classes(view)
                sdk[api_dir / f"{view_snake_plural}.py"] = self.view_to_api(view)
            except Exception as e:
                print(f"Failed to generate SDK for view {view.name}: {e}")  # noqa
                print(f"Skipping view {view.name}")  # noqa
                if view.name in self._view_names:
                    self._view_names.remove(view.name)
                if view.name in self._dependencies_by_view_name:
                    del self._dependencies_by_view_name[view.name]
        sdk[client_dir / "_api_client.py"] = self.create_api_client()
        sdk[client_dir / "__init__.py"] = self.create_client_init()
        sdk[data_classes_dir / "__init__.py"] = self.create_data_classes_init()
        sdk[api_dir / "_core.py"] = self.create_api_core()
        sdk[data_classes_dir / "_core.py"] = (self._static_dir / "_core_data_classes.py").read_text()
        return sdk

    def create_api_core(self) -> str:
        api_core = self._env.get_template("_core_api.py.jinja")
        return api_core.render(sdk_name=self.sdk_name)

    def create_client_init(self) -> str:
        client_init = self._env.get_template("_client_init.py.jinja")
        return client_init.render(client_name_pascal=self.client_name_pascal)

    def create_api_client(self) -> str:
        api_client = self._env.get_template("_api_client.py.jinja")

        api_imports = [client_subapi_import(view_name) for view_name in sorted(self._view_names)]
        api_instantiations = [subapi_instantiation(view_name) for view_name in sorted(self._view_names)]

        return (
            api_client.render(
                client_name_pascal=self.client_name_pascal,
                pygen_version=__version__,
                cognite_sdk_version=cognite_sdk_version,
                pydantic_version=PYDANTIC_VERSION,
                data_model_space=self._data_model_space,
                data_model_external_id=self._data_model_external_id,
                data_model_version=self._data_model_version,
                api_imports="\n".join(api_imports),
                api_instantiations="\n        ".join(api_instantiations),
            )
            + "\n"
        )

    def view_to_data_classes(self, view: dm.View) -> str:
        type_data = self._env.get_template("type_data.py.jinja")
        fields = properties_to_fields(view.properties.values())
        self._update_dependencies(fields, view.name)
        self._view_names.add(view.name)

        sources = properties_to_sources(view.properties.values())
        create_edges = self.properties_to_create_edge_methods(view.properties.values())
        add_edges = self.properties_to_add_edges(view.properties.values())

        circular_imports = dependencies_to_imports(self._dependencies_by_view_name.get(view.name, set()))

        return (
            type_data.render(
                view_name=view.name,
                view_space=view.space,
                read_fields="\n    ".join(f.as_type_hint("read") for f in fields),
                write_fields="\n    ".join(f.as_type_hint("write") for f in fields),
                sources=f',\n{" "*16}'.join(sources),
                circular_imports=circular_imports,
                create_edges="\n\n".join(create_edges),
                add_edges="\n\n".join(add_edges),
            )
            + "\n"
        )

    def _update_dependencies(self, fields: list[Field], view_name: str):
        for field in fields:
            if field.is_edge:
                self._dependencies_by_view_name[view_name].add(field.edge_end_node_external_id)

    def view_to_api(self, view: dm.View) -> str:
        edge_properties = list(view_functions.edge_properties(view.properties.values()))

        edges_apis = [self.property_to_edge_api(prop, view.name, view.space) for prop in edge_properties]
        edges_helpers = [self.property_to_edge_helper(prop, view.name) for prop in edge_properties]
        edge_snippets = [property_to_edge_snippets(prop, view.name) for prop in edge_properties]

        has_one_to_many = len(edge_properties) > 0

        type_api = self._env.get_template("type_api.py.jinja")

        view_plural_snake = to_snake(view.name, pluralize=True)
        return type_api.render(
            sdk_name=self.sdk_name,
            view_name=view.name,
            view_snake=to_snake(view.name),
            view_space=view.space,
            view_ext_id=view.external_id,
            view_version=view.version,
            view_plural_snake=view_plural_snake,
            view_plural_pascal=to_pascal(view_plural_snake),
            has_one_to_many=has_one_to_many,
            edge_apis="\n\n\n".join(edges_apis),
            edge_helpers="\n\n".join(edges_helpers),
            edge_retrieve=f"\n{' '*12}".join(snippet.retrieve for snippet in edge_snippets),
            edge_list=f"\n{' '*8}".join(snippet.list for snippet in edge_snippets),
            init_edge_apis=f"\n{' ' * 8}".join(snippet.init for snippet in edge_snippets),
            set_retrieve_singular=f"\n{' '*12}".join(snippet.set_singular for snippet in edge_snippets),
            set_retrieve_plural=f"\n{' '*12}".join(snippet.set_plural for snippet in edge_snippets),
            set_list_plural=f"\n{' '*8}".join(snippet.set_plural for snippet in edge_snippets),
        ) + ("\n" if has_one_to_many else "")

    def property_to_edge_api(
        self, prop: dm.ConnectionDefinition | dm.MappedPropertyDefinition, view_name: str, view_space: str
    ) -> str:
        edge_api = self._env.get_template("edge_api.py.jinja")
        shared_args = dict(
            view_name=view_name,
            view_space=view_space,
            view_snake=to_snake(view_name, singularize=True),
            view_snake_plural=to_snake(view_name, pluralize=True),
        )
        if isinstance(prop, dm.SingleHopConnectionDefinition):
            return edge_api.render(
                **shared_args,
                edge_api_name=to_pascal(prop.name, pluralize=True),
                type_ext_id=prop.type.external_id,
            )
        elif isinstance(prop, dm.MappedPropertyDefinition):
            return edge_api.render(
                **shared_args,
                edge_api_name=to_pascal(prop.name, singularize=True),
                type_ext_id=f"{prop.container.external_id}.{prop.name}",
            )
        raise NotImplementedError(f"Edge API for type={type(prop)} is not implemented")

    def property_to_edge_helper(
        self, prop: dm.ConnectionDefinition | dm.MappedPropertyDefinition, view_name: str
    ) -> str:
        if isinstance(prop, dm.SingleHopConnectionDefinition):
            helper = self._env.get_template("type_api_set_edges_helper.py.jinja")
            return helper.render(
                view_name=to_pascal(view_name),
                view_snake=to_snake(view_name),
                view_plural_snake=to_snake(view_name, pluralize=True),
                edge_name=prop.name,
                edge_plural_snake=to_snake(prop.name, pluralize=True),
                edge_snake=to_snake(prop.name, singularize=True),
            )
        elif isinstance(prop, dm.MappedPropertyDefinition):
            helper = self._env.get_template("type_api_set_edge_helper.py.jinja")
            return helper.render(
                edge_name=prop.name,
                edge_snake=to_snake(prop.name, singularize=True),
                view_name=view_name,
                view_snake=to_snake(view_name),
                view_snake_plural=to_snake(view_name, pluralize=True),
            )
        raise NotImplementedError(f"Edge API for type={type(prop)} is not implemented")

    def create_data_classes_init(self) -> str:
        import_lines = []
        class_lines = []
        for view_name in sorted(self._view_names):
            classes = [view_name, f"{view_name}Apply", f"{view_name}List"]
            import_lines.append(f"from ._{to_snake(view_name, pluralize=True)} import {', '.join(classes)}")
            class_lines.extend(classes)

        update_forward_refs_lines = []
        for view_name, dependencies in sorted(self._dependencies_by_view_name.items()):
            if not dependencies:
                continue
            dependencies = [f"{dependency}Apply={dependency}Apply" for dependency in sorted(dependencies)]
            update_forward_refs_lines.append(f"{view_name}Apply.update_forward_refs({', '.join(dependencies)})")

        imports = "\n".join(import_lines)
        forward_refs = "\n".join(update_forward_refs_lines)
        classes = '",\n    "'.join(class_lines)
        return f"""{imports}

{forward_refs}

__all__ = [
    "{classes}",
]
"""

    def properties_to_create_edge_methods(
        self, properties: Iterable[dm.MappedPropertyDefinition | dm.ConnectionDefinition]
    ) -> list[str]:
        create_edge = self._env.get_template("type_data_create_edge.py.jinja")
        create_methods = []
        for prop in properties:
            if isinstance(prop, dm.SingleHopConnectionDefinition):
                create_methods.append(
                    create_edge.render(
                        edge_snake=to_snake(prop.name, singularize=True),
                        # Todo Avoid assuming that nodes and edges are in the same space.
                        space=prop.source.space,
                        edge_pascal=to_pascal(prop.name, singularize=True),
                        type_ext_id=prop.type.external_id,
                    )
                )
            elif isinstance(prop, dm.MappedPropertyDefinition) and isinstance(prop.type, ViewDirectRelation):
                create_methods.append(
                    create_edge.render(
                        edge_snake=to_snake(prop.name, singularize=True),
                        # Todo Avoid assuming that nodes and edges are in the same space.
                        space=prop.type.source.space,
                        edge_pascal=to_pascal(prop.name, singularize=True),
                        type_ext_id=f"{prop.container.external_id}.{prop.name}",
                    )
                )
        return create_methods

    def properties_to_add_edges(
        self, properties: Iterable[dm.MappedPropertyDefinition | dm.ConnectionDefinition]
    ) -> list[str]:
        add_edges = self._env.get_template("type_data_add_edges.py.jinja")
        add_edge = self._env.get_template("type_data_add_edge.py.jinja")

        add_snippets = []
        for prop in properties:
            if isinstance(prop, dm.SingleHopConnectionDefinition):
                add_snippets.append(
                    add_edges.render(
                        edge_name=prop.name,
                        edge_snake=to_snake(prop.name, singularize=True),
                    )
                )
            elif isinstance(prop, dm.MappedPropertyDefinition) and isinstance(prop.type, ViewDirectRelation):
                add_snippets.append(
                    add_edge.render(
                        edge_snake=to_snake(prop.name, singularize=True),
                    )
                )

        return add_snippets


@dataclass
class EdgeSnippets:
    init: str
    set_singular: str
    set_plural: str
    retrieve: str
    list: str


def property_to_edge_snippets(
    prop: dm.ConnectionDefinition | dm.MappedPropertyDefinition, view_name: str
) -> EdgeSnippets:
    view_snake = to_snake(view_name)
    view_snake_plural = to_snake(view_name, pluralize=True)
    prop_snake = to_snake(prop.name, singularize=True)
    prop_pascal = to_pascal(prop.name, singularize=True)
    if isinstance(prop, dm.SingleHopConnectionDefinition):
        prop_pascal_plural = to_pascal(prop.name, pluralize=True)
        prop_plural_snake = to_snake(prop.name, pluralize=True)
        return EdgeSnippets(
            f"self.{prop_plural_snake} = {view_name}{prop_pascal_plural}API(client)",
            f"{view_snake}.{prop.name} = [edge.end_node.external_id for edge in {prop_snake}_edges]",
            f"self._set_{prop_plural_snake}({view_snake_plural}, {prop_snake}_edges)",
            f"{prop_snake}_edges = self.{prop_plural_snake}.retrieve(external_id)",
            f"{prop_snake}_edges = self.{prop_plural_snake}.list(limit=-1)",
        )
    elif isinstance(prop, dm.MappedPropertyDefinition):
        return EdgeSnippets(
            f"self.{prop_snake} = {view_name}{prop_pascal}API(client)",
            f"{view_snake}.{prop.name} = {prop_snake}_edges[0].end_node.external_id if {prop_snake}_edges else None",
            f"self._set_{prop_snake}({view_snake_plural}, {prop_snake}_edges)",
            f"{prop_snake}_edges = self.{prop_snake}.retrieve(external_id)",
            f"{prop_snake}_edges = self.{prop_snake}.list(limit=-1)",
        )

    raise NotImplementedError(f"Edge API for type={type(prop)} is not implemented")


@dataclass
class Field:
    source_name: str
    type: str
    default: str
    is_nullable: bool
    is_one_to_many: bool = False
    edge_end_node_external_id: str | None = None

    @property
    def snake_name(self):
        return to_snake(self.source_name)

    @property
    def is_edge(self) -> bool:
        return self.edge_end_node_external_id is not None

    @classmethod
    def from_property(cls, property_: dm.MappedPropertyDefinition | dm.ConnectionDefinition) -> Field:
        if isinstance(property_, dm.MappedPropertyDefinition) and not isinstance(property_.type, dm.DirectRelation):
            type_ = _to_python_type(property_.type)
            default = property_.default_value if isinstance(property_.type, dm.PropertyType) else "[]"
            is_nullable = isinstance(property_.type, dm.DirectRelationReference) or property_.nullable
            return cls(property_.name, type_, default, is_nullable)
        elif isinstance(property_, dm.SingleHopConnectionDefinition):
            # One to Many
            return cls(property_.name, "list[str]", "[]", True, True, property_.source.external_id)
        elif isinstance(property_, dm.MappedPropertyDefinition) and isinstance(property_.type, ViewDirectRelation):
            # One to One
            return cls(property_.name, "str", "None", True, False, property_.type.source.external_id)
        else:
            raise NotImplementedError(f"Property type={type(property_)} is not supported")

    def as_type_hint(self, field_type: Literal["read", "write"]) -> str:
        is_nullable = self.is_nullable or (field_type == "read")
        default = self.default
        snake_name = self.snake_name
        if snake_name != self.source_name and field_type == "read":
            default = f'Field({default}, alias="{self.source_name}")'

        rhs = f" = {default}" if is_nullable else ""

        type_ = self.type
        if self.is_edge and field_type == "write":
            type_ = f'Union[str, "{self.edge_end_node_external_id}Apply"]'
            if self.is_one_to_many:
                type_ = f"list[{type_}]"

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
    elif isinstance(type_, (dm.Text, dm.DirectRelation, dm.CDFExternalIdReference)):
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
        if isinstance(property_, dm.MappedPropertyDefinition) and not isinstance(property_.type, ViewDirectRelation):
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


def dependencies_to_imports(dependencies: set[str]) -> str:
    if not dependencies:
        return ""
    lines = ["if TYPE_CHECKING:"]
    for dependency in sorted(dependencies):
        snake_plural = to_snake(dependency, pluralize=True)
        lines.append(f"    from ._{snake_plural} import {dependency}Apply")
    lines.append("")
    return "\n".join(lines)


def client_subapi_import(view_name: str) -> str:
    return f"from ._api.{to_snake(view_name, pluralize=True)} import {to_pascal(view_name, pluralize=True)}API"


def subapi_instantiation(view_name: str) -> str:
    return f"self.{to_snake(view_name, pluralize=True)} = {to_pascal(view_name, pluralize=True)}API(client)"
