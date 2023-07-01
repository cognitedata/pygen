from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Literal

from cognite.client import data_modeling as dm
from cognite.client._version import __version__ as cognite_sdk_version
from cognite.client.data_classes.data_modeling.data_types import ListablePropertyType
from jinja2 import Environment, PackageLoader, select_autoescape
from pydantic.version import VERSION as PYDANTIC_VERSION

from cognite.pygen._core import view_functions
from cognite.pygen._version import __version__
from cognite.pygen.utils.text import to_pascal, to_snake


class SDKGenerator:
    def __init__(self, top_level_package: str, client_name: str):
        self.top_level_package = top_level_package
        self.client_name = client_name
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

        client_dir = Path(self.top_level_package.replace(".", "/"))
        data_classes_dir = client_dir / "data_classes"
        api_dir = client_dir / "_api"

        sdk = {(api_dir / "__init__.py"): ""}
        for view in data_model.views:
            file_name = to_snake(view.name, pluralize=True)
            try:
                data_class = APIGenerator(view)
                sdk[data_classes_dir / f"_{file_name}.py"] = data_class.generate_data_class_file()
                sdk[api_dir / f"{file_name}.py"] = self.view_to_api(view)
            except Exception as e:
                print(f"Failed to generate SDK for view {view.name}: {e}")  # noqa
                print(f"Skipping view {view.name}")  # noqa
                if view.name in self._view_names:
                    self._view_names.remove(view.name)
                if view.name in self._dependencies_by_view_name:
                    del self._dependencies_by_view_name[view.name]
            else:
                self._update_dependencies(data_class.fields, view.name)
                self._view_names.add(view.name)

        sdk[client_dir / "_api_client.py"] = self.create_api_client()
        sdk[client_dir / "__init__.py"] = self.create_client_init()
        sdk[data_classes_dir / "__init__.py"] = self.create_data_classes_init()
        sdk[api_dir / "_core.py"] = self.create_api_core()
        sdk[data_classes_dir / "_core.py"] = (self._static_dir / "_core_data_classes.py").read_text()
        return sdk

    def create_api_core(self) -> str:
        api_core = self._env.get_template("_core_api.py.jinja")
        return api_core.render(top_level_package=self.top_level_package)

    def create_client_init(self) -> str:
        client_init = self._env.get_template("_client_init.py.jinja")
        return client_init.render(client_name_pascal=self.client_name)

    def create_api_client(self) -> str:
        api_client = self._env.get_template("_api_client.py.jinja")

        api_imports = [client_subapi_import(view_name) for view_name in sorted(self._view_names)]
        api_instantiations = [subapi_instantiation(view_name) for view_name in sorted(self._view_names)]

        return (
            api_client.render(
                client_name_pascal=self.client_name,
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

    def _update_dependencies(self, fields: list[Field], view_name: str):
        for field in fields:
            if field.is_edge:
                self._dependencies_by_view_name[view_name].add(field.edge_end_node_external_id)

    def view_to_api(self, view: dm.View) -> str:
        edge_properties = list(view_functions.edge_one_to_many_properties(view.properties.values()))

        edges_apis = [self.property_to_edge_api(prop, view.name, view.space) for prop in edge_properties]
        edges_helpers = [self.property_to_edge_helper(prop, view.name) for prop in edge_properties]
        edge_snippets = [property_to_edge_snippets(prop, view.name) for prop in edge_properties]

        has_one_to_many = len(edge_properties) > 0

        type_api = self._env.get_template("type_api.py.jinja")

        view_plural_snake = to_snake(view.name, pluralize=True)
        return type_api.render(
            top_level_package=self.top_level_package,
            view_name=to_pascal(view.name, singularize=True),
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
        self, prop: dm.ConnectionDefinition | dm.MappedProperty, view_name: str, view_space: str
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
        elif isinstance(prop, dm.MappedProperty):
            return edge_api.render(
                **shared_args,
                edge_api_name=to_pascal(prop.name, singularize=True),
                type_ext_id=f"{prop.container.external_id}.{prop.name}",
            )
        raise NotImplementedError(f"Edge API for type={type(prop)} is not implemented")

    def property_to_edge_helper(self, prop: dm.ConnectionDefinition | dm.MappedProperty, view_name: str) -> str:
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
        elif isinstance(prop, dm.MappedProperty):
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
            pascal_name = to_pascal(view_name, singularize=True)
            classes = [pascal_name, f"{pascal_name}Apply", f"{pascal_name}List"]
            import_lines.append(f"from ._{to_snake(view_name, pluralize=True)} import {', '.join(classes)}")
            class_lines.extend(classes)

        update_forward_refs_lines = []
        for view_name, dependencies in sorted(self._dependencies_by_view_name.items()):
            if not dependencies:
                continue
            pascal_name = to_pascal(view_name, singularize=True)
            dependencies = [
                f"{to_pascal(dependency, singularize=True)}Apply={to_pascal(dependency, singularize=True)}Apply"
                for dependency in sorted(dependencies)
            ]
            update_forward_refs_lines.append(f"{pascal_name}Apply.update_forward_refs({', '.join(dependencies)})")

        imports = "\n".join(import_lines)
        forward_refs = "\n".join(update_forward_refs_lines)
        classes = '",\n    "'.join(class_lines)
        return f"""{imports}

{forward_refs}

__all__ = [
    "{classes}",
]
"""


@dataclass
class Field:
    name: str
    read_type: str
    is_list: bool
    is_nullable: bool
    default: str | None
    prop: dm.MappedProperty | dm.ConnectionDefinition
    write_type: str | None = None
    is_edge: bool = False
    variable: str | None = None
    dependency_class: str | None = None
    dependency_file: str | None = None
    edge_api_class_suffix: str | None = None

    @property
    def is_edges(self) -> bool:
        return self.is_edge and self.is_list

    @property
    def is_datetime(self) -> bool:
        return self.read_type == "datetime"

    @classmethod
    def from_property(cls, property_: dm.MappedProperty | dm.ConnectionDefinition) -> Field:
        if isinstance(property_, dm.MappedProperty) and not isinstance(property_.type, dm.DirectRelation):
            # Is primary field
            is_list = isinstance(property_.type, ListablePropertyType) and property_.type.is_list
            name = to_snake(property_.name, singularize=not is_list, pluralize=is_list)
            type_ = _to_python_type(property_.type)
            is_nullable = property_.nullable
            default = property_.default_value or ("[]" if is_list else ("None" if is_nullable else None))
            variable = to_snake(property_.name, singularize=True) if is_list else None
            return cls(name, type_, is_list, is_nullable, default, property_, write_type=type_, variable=variable)
        elif isinstance(property_, dm.MappedProperty):
            # Edge One to One
            name = to_snake(property_.name, singularize=True)
            dependency_class = to_pascal(property_.source.external_id, singularize=True)
            dependency_file = to_snake(property_.source.external_id, pluralize=True)
            write_type = f'Union[str, "{dependency_class}Apply"]'
            return cls(
                name,
                "str",
                is_list=False,
                is_nullable=property_.nullable,
                write_type=write_type,
                default="None",
                is_edge=True,
                prop=property_,
                dependency_class=dependency_class,
                dependency_file=dependency_file,
            )
        elif isinstance(property_, dm.SingleHopConnectionDefinition):
            # One to Many
            name = to_snake(property_.name, pluralize=True)
            dependency_class = to_pascal(property_.source.external_id, singularize=True)
            dependency_file = to_snake(property_.source.external_id, pluralize=True)
            write_type = f'Union[str, "{dependency_class}Apply"]'
            return cls(
                name,
                "str",
                is_list=True,
                is_nullable=True,
                write_type=write_type,
                default="[]",
                is_edge=True,
                variable=to_snake(property_.name, singularize=True),
                prop=property_,
                dependency_class=dependency_class,
                dependency_file=dependency_file,
                edge_api_class_suffix=to_pascal(property_.name, pluralize=True),
            )
        else:
            raise NotImplementedError(f"Property type={type(property_)} is not supported")

    def as_type_hint(self, field_type: Literal["read", "write"]) -> str:
        is_nullable = self.is_nullable or (field_type == "read")
        default = self.default
        if self.name != self.prop.name and field_type == "read":
            default = f'Field({default}, alias="{self.prop.name}")'

        rhs = f" = {default}" if is_nullable else ""

        type_ = self.read_type if field_type == "read" else self.write_type
        if self.is_list:
            type_ = f"list[{type_}]"

        if is_nullable and not type_.startswith("list"):
            type_ = f"Optional[{type_}]"

        return f"{type_}{rhs}"


@dataclass
class Fields:
    data: list[Field]

    def __iter__(self):
        return iter(self.data)

    @property
    def primary(self) -> list[Field]:
        return [field for field in self.data if not field.is_edge]

    @property
    def fields_by_container(self) -> dict[dm.ContainerId, list[Field]]:
        result: Dict[dm.ContainerId, List[Field]] = defaultdict(list)
        for field in self:
            if isinstance(field.prop, dm.MappedProperty):
                result[field.prop.container].append(field)
        return result

    @property
    def edges(self) -> list[Field]:
        return [field for field in self.data if field.is_edge]

    @property
    def edges_one_to_one(self) -> list[Field]:
        return [field for field in self.data if field.is_edge and not field.is_list]

    @property
    def edges_one_to_many(self) -> list[Field]:
        return [field for field in self.data if field.is_edge and field.is_list]

    @property
    def has_one_to_many_edges(self) -> bool:
        return any(field.is_edge and field.is_list for field in self.data)

    @property
    def has_one_to_one_edges(self) -> bool:
        return any(field.is_edge and not field.is_list for field in self.data)

    @property
    def import_pydantic_field(self) -> bool:
        return any("Field" in field.as_type_hint("read") for field in self.data)

    @property
    def import_dependencies(self) -> bool:
        return any(field.is_edge for field in self.data)

    @property
    def has_datetime(self) -> bool:
        return any(field.is_datetime for field in self.data)


@dataclass
class APIClass:
    data_class: str
    variable: str
    variable_list: str
    api_class: str

    @classmethod
    def from_view(cls, view: dm.View) -> APIClass:
        return cls(
            to_pascal(view.name, singularize=True),
            to_snake(view.name, singularize=True),
            to_snake(view.name, pluralize=True),
            to_pascal(view.name, pluralize=True),
        )


class APIGenerator:
    def __init__(self, view: dm.View):
        self._view = view
        self._env = Environment(
            loader=PackageLoader("cognite.pygen._core", "templates"),
            autoescape=select_autoescape(),
        )
        self.fields = Fields([Field.from_property(prop) for prop in view.properties.values()])
        self.class_ = APIClass.from_view(view)

    def generate_data_class_file(self) -> str:
        type_data = self._env.get_template("type_data.py.jinja")

        return (
            type_data.render(
                class_name=self.class_.data_class,
                fields=self.fields,
                space=self._view.space,
                view_name=self._view.name,
            )
            + "\n"
        )

    def generate_api_file(self, top_level_package: str) -> str:
        type_api = self._env.get_template("type_api.py.jinja")

        return type_api.render(
            top_level_package=top_level_package,
            class_=self.class_,
            fields=self.fields,
            view_space=self._view.space,
            view_external_id=self._view.external_id,
            view_version=self._view.version,
        )


@dataclass
class EdgeSnippets:
    init: str
    set_singular: str
    set_plural: str
    retrieve: str
    list: str


def property_to_edge_snippets(prop: dm.ConnectionDefinition | dm.MappedProperty, view_name: str) -> EdgeSnippets:
    view_snake = to_snake(view_name)
    view_snake_plural = to_snake(view_name, pluralize=True)
    prop_snake = to_snake(prop.name, singularize=True)
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

    raise NotImplementedError(f"Edge API for type={type(prop)} is not implemented")


def properties_to_fields(
    properties: Iterable[dm.MappedProperty | dm.ConnectionDefinition],
) -> list[Field]:
    return [Field.from_property(p) for p in properties]


def _to_python_type(type_: dm.DirectRelationReference | dm.PropertyType) -> str:
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
    elif isinstance(type_, (dm.Text, dm.DirectRelation, dm.CDFExternalIdReference, dm.DirectRelationReference)):
        out_type = "str"
    else:
        raise ValueError(f"Unknown type {type_}")

    return out_type


def client_subapi_import(view_name: str) -> str:
    return f"from ._api.{to_snake(view_name, pluralize=True)} import {to_pascal(view_name, pluralize=True)}API"


def subapi_instantiation(view_name: str) -> str:
    return f"self.{to_snake(view_name, pluralize=True)} = {to_pascal(view_name, pluralize=True)}API(client)"
