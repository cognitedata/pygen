from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import Any, Callable, Literal

from cognite.client import data_modeling as dm
from cognite.client._version import __version__ as cognite_sdk_version
from cognite.client.data_classes.data_modeling.data_types import ListablePropertyType
from jinja2 import Environment, PackageLoader, select_autoescape
from pydantic.version import VERSION as PYDANTIC_VERSION

from cognite.pygen._version import __version__
from cognite.pygen.utils.text import to_pascal, to_snake


class SDKGenerator:
    """
    SDK generator for a data model.

    Args:
        top_level_package: The name of the top level package for the SDK. Example "movie.client"
        client_name: The name of the client class. Example "MovieClient"
        data_model: The data model(s) to generate a SDK for.
        pydantic_version: The version of pydantic to use. "infer" will use the version of pydantic installed in
                          the environment.
        logger: A logger function to use for logging. If None, print will be done.
    """

    def __init__(
        self,
        top_level_package: str,
        client_name: str,
        data_model: dm.DataModel | Sequence[dm.DataModel],
        pydantic_version: Literal["v1", "v2", "infer"] = "infer",
        logger: Callable[[str], None] | None = None,
    ):
        self._data_model = data_model
        self.top_level_package = top_level_package
        self.client_name = client_name
        if isinstance(data_model, dm.DataModel):
            self._is_single_model = True
            self._apis = APIsGenerator(top_level_package, client_name, data_model.views, pydantic_version, logger)
            self._apis_classes = []
        elif isinstance(data_model, Sequence):
            self._is_single_model = False
            unique_views = get_unique_views(*[view for dm in data_model for view in dm.views])
            self._apis = APIsGenerator(top_level_package, client_name, unique_views, pydantic_version, logger)
            api_by_view_external_id = {api.view.external_id: api.class_ for api in self._apis.apis}
            self._apis_classes = sorted(
                (APIsClass.from_data_model(dm, api_by_view_external_id) for dm in data_model), key=lambda a: a.name
            )
        else:
            raise ValueError("data_model must be a DataModel or a sequence of DataModels")

    def generate_sdk(self) -> dict[Path, str]:
        """
        Generate the SDK.

        Returns:
            A Python SDK given as a dictionary of file paths and file contents, which can be written to disk.
        """
        client_dir = Path(self.top_level_package.replace(".", "/"))
        sdk = self._apis.generate_apis(client_dir)
        sdk[client_dir / "_api_client.py"] = self._generate_api_client_file()
        return sdk

    def _generate_api_client_file(self) -> str:
        api_client = self._apis.env.get_template("_api_client.py.jinja")

        return (
            api_client.render(
                client_name=self.client_name,
                pygen_version=__version__,
                cognite_sdk_version=cognite_sdk_version,
                pydantic_version=PYDANTIC_VERSION,
                classes=sorted((api.class_ for api in self._apis.apis), key=lambda c: c.data_class),
                is_single_model=self._is_single_model,
                top_level_package=self.top_level_package,
                api_classes=self._apis_classes,
                data_model=self._data_model,
            )
            + "\n"
        )


class APIsGenerator:
    def __init__(
        self,
        top_level_package: str,
        client_name: str,
        views: Sequence[dm.View],
        pydantic_version: Literal["v1", "v2", "infer"] = "infer",
        logger: Callable[[str], None] | None = None,
    ):
        self.env = Environment(
            loader=PackageLoader("cognite.pygen._core", "templates"),
            autoescape=select_autoescape(),
        )
        self.top_level_package = top_level_package
        self.client_name = client_name
        if pydantic_version == "infer":
            self.pydantic_version = "v2" if PYDANTIC_VERSION[0] == "2" else "v1"
        else:
            self.pydantic_version = pydantic_version
        self._logger = logger or print

        self.apis = []
        for view in views:
            try:
                api_generator = APIGenerator(view, self.top_level_package)
            except Exception as e:
                self._logger(f"Failed to generate SDK for view {view.name}: {e}")
            else:
                self.apis.append(api_generator)
        self._dependencies_by_class = find_dependencies(self.apis)
        self._static_dir = Path(__file__).parent / "static"

    def generate_apis(self, client_dir: Path) -> dict[Path, str]:
        data_classes_dir = client_dir / "data_classes"
        api_dir = client_dir / "_api"

        sdk = {(api_dir / "__init__.py"): ""}
        for api in self.apis:
            file_name = api.class_.file_name
            try:
                sdk[data_classes_dir / f"_{file_name}.py"] = api.generate_data_class_file()
                sdk[api_dir / f"{file_name}.py"] = api.generate_api_file(self.top_level_package)
            except Exception as e:
                self._logger(f"Failed to generate SDK for view {api.view.name}: {e}")
                self._logger(f"Skipping view {api.view.name}")
                self._dependencies_by_class.pop(api.class_, None)

        sdk[client_dir / "__init__.py"] = self.generate_client_init_file()
        sdk[data_classes_dir / "__init__.py"] = self.generate_data_classes_init_file()
        sdk[api_dir / "_core.py"] = self.generate_api_core_file()
        if self.pydantic_version == "v2":
            core_data_classes = "_core_data_classes.py"
        else:
            core_data_classes = "_core_data_classes_pydantic_v1.py"
        sdk[data_classes_dir / "_core.py"] = (self._static_dir / core_data_classes).read_text()
        return sdk

    def generate_api_core_file(self) -> str:
        api_core = self.env.get_template("_core_api.py.jinja")
        return api_core.render(top_level_package=self.top_level_package) + "\n"

    def generate_client_init_file(self) -> str:
        client_init = self.env.get_template("_client_init.py.jinja")
        return client_init.render(client_name=self.client_name, top_level_package=self.top_level_package) + "\n"

    def generate_data_classes_init_file(self) -> str:
        data_class_init = self.env.get_template("data_classes_init.py.jinja")
        return (
            data_class_init.render(
                classes=sorted((api.class_ for api in self.apis), key=lambda c: c.data_class),
                dependencies_by_class={
                    class_: sorted(dependencies, key=lambda c: c.data_class)
                    for class_, dependencies in sorted(
                        self._dependencies_by_class.items(), key=lambda x: x[0].data_class
                    )
                },
                top_level_package=self.top_level_package,
                import_file={
                    "v2": "data_classes_init_import.py.jinja",
                    "v1": "data_classes_init_import.py_pydanticv1.jinja",
                }[self.pydantic_version],
            )
            + "\n"
        )


@dataclass
class Field:
    name: str
    read_type: str
    is_list: bool
    is_nullable: bool
    default: str | None
    prop: dm.MappedProperty | dm.SingleHopConnectionDefinition
    write_type: str
    is_edge: bool = False
    variable: str | None = None
    dependency_class: str | None = None
    dependency_file: str | None = None
    edge_api_class_suffix: str | None = None
    edge_api_attribute: str | None = None

    @property
    def is_edges(self) -> bool:
        return self.is_edge and self.is_list

    @property
    def is_datetime(self) -> bool:
        return self.read_type == "datetime.datetime"

    @property
    def is_date(self) -> bool:
        return self.read_type == "datetime.date"

    @classmethod
    def from_property(cls, property_: dm.MappedProperty | dm.ConnectionDefinition) -> Field:
        if not isinstance(property_, (dm.MappedProperty, dm.SingleHopConnectionDefinition)):
            raise NotImplementedError(f"Property type={type(property_)!r} is not supported")
        if property_.name is None:
            raise ValueError("Property must have a name")
        property_name = property_.name
        if isinstance(property_, dm.MappedProperty) and not isinstance(property_.type, dm.DirectRelation):
            # Is primary field
            is_list = isinstance(property_.type, ListablePropertyType) and property_.type.is_list
            name = to_snake(property_name)
            type_ = _to_python_type(property_.type)
            is_nullable = property_.nullable
            default = property_.default_value or ("[]" if is_list else ("None" if is_nullable else None))
            variable = to_snake(property_name, singularize=True) if is_list else None
            return cls(
                name,
                type_,
                is_list,
                is_nullable,
                str(default) if default else None,
                property_,
                write_type=type_,
                variable=variable,
            )

        if property_.source is None:
            raise ValueError("Property must have a source")
        property_source = property_.source
        if isinstance(property_, dm.MappedProperty):
            # Edge One to One
            name = to_snake(property_name)
            dependency_class = to_pascal(property_source.external_id, singularize=True)
            dependency_file = to_snake(property_source.external_id, pluralize=True)
            write_type = f'Union["{dependency_class}Apply", str]'
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
            name = to_snake(property_name)
            dependency_class = to_pascal(property_source.external_id, singularize=True)
            dependency_file = to_snake(property_source.external_id, pluralize=True)
            write_type = f'Union["{dependency_class}Apply", str]'
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
                edge_api_attribute=to_snake(property_.name, pluralize=True),
            )
        else:
            raise NotImplementedError(f"Property type={type(property_)} is not supported")

    def as_type_hint(self, field_type: Literal["read", "write"]) -> str:
        is_nullable: bool = self.is_nullable or (field_type == "read")
        default = self.default
        if self.name != self.prop.name and field_type == "read":
            default = f'Field({default}, alias="{self.prop.name}")'
        elif field_type == "write" and self.is_edges:
            default = "Field(default_factory=list, repr=False)"
        elif field_type == "write" and self.is_edge:
            default = f"Field({default}, repr=False)"
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
        result: dict[dm.ContainerId, list[Field]] = defaultdict(list)
        for field in self:
            if isinstance(field.prop, dm.MappedProperty):
                result[field.prop.container].append(field)
        return result

    @property
    def edges(self) -> list[Field]:
        return [field for field in self.data if field.is_edge]

    @property
    def unique_dependencies(self) -> list[tuple[str, str]]:
        dependencies = set()
        for field in self.edges:
            if (field.dependency_file, field.dependency_class) not in dependencies:
                if field.dependency_file and field.dependency_class:
                    dependencies.add((field.dependency_file, field.dependency_class))
        return sorted(dependencies, key=lambda x: x[0])

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
    def has_properties(self) -> bool:
        return any(not field.is_edge for field in self.data)

    @property
    def import_pydantic_field(self) -> bool:
        return any("Field" in field.as_type_hint("read") for field in self.data) or any(
            field.is_edge for field in self.data
        )

    @property
    def import_dependencies(self) -> bool:
        return any(field.is_edge for field in self.data)

    @property
    def has_datetime(self) -> bool:
        return any(field.is_datetime for field in self.data)

    @property
    def has_date(self) -> bool:
        return any(field.is_date for field in self.data)

    @property
    def dependencies(self) -> set[str]:
        return {field.dependency_class for field in self.data if field.is_edge and field.dependency_class}

    @property
    def has_optional(self) -> bool:
        return any(
            "Optional" in field.as_type_hint(field_type)  # type: ignore[arg-type]
            for field, field_type in product(self.data, ["read", "write"])
        )


@dataclass(frozen=True)
class APIClass:
    data_class: str
    variable: str
    variable_list: str
    client_attribute: str
    api_class: str
    file_name: str
    _top_level_package: str

    @classmethod
    def from_view(cls, view_name: str, top_level_package: str) -> APIClass:
        return cls(
            data_class=to_pascal(view_name, singularize=True),
            variable=to_snake(view_name, singularize=True),
            variable_list=to_snake(view_name, pluralize=True),
            client_attribute=to_snake(view_name, pluralize=True),
            api_class=to_pascal(view_name, pluralize=True),
            file_name=to_snake(view_name, pluralize=True),
            _top_level_package=top_level_package,
        )

    @property
    def one_line_import(self) -> str:
        return (
            f"from {self._top_level_package}.data_classes._{self.file_name} "
            f"import {self.data_class}, {self.data_class}Apply, {self.data_class}List"
        )

    @property
    def multiline_import(self) -> str:
        return (
            f"from {self._top_level_package}.data_classes._{self.file_name} "
            f"import (\n    {self.data_class},\n    {self.data_class}Apply,\n    {self.data_class}List,\n)"
        )

    # Todo hack to get around black in unit tests
    @property
    def is_line_length_above_120(self) -> bool:
        return len(self.one_line_import) > 120


@dataclass(frozen=True)
class APIsClass:
    sub_apis: list[APIClass]
    variable: str
    name: str
    model: dm.DataModelId

    @classmethod
    def from_data_model(cls, data_model: dm.DataModel, api_class_by_view_external_id: dict[str, APIClass]) -> APIsClass:
        sub_apis = []
        for view in data_model.views:
            sub_apis.append(api_class_by_view_external_id[view.external_id])
        data_model_name = data_model.name or data_model.external_id

        return cls(
            sub_apis=sorted(sub_apis, key=lambda api: api.data_class),
            variable=to_snake(data_model_name, singularize=True),
            name=f"{to_pascal(data_model_name, singularize=True)}APIs",
            model=data_model.as_id(),
        )


class APIGenerator:
    def __init__(self, view: dm.View, top_level_package: str):
        self.view = view
        self.top_level_package = top_level_package
        self._env = Environment(
            loader=PackageLoader("cognite.pygen._core", "templates"),
            autoescape=select_autoescape(),
        )
        self.fields = Fields(
            sorted((Field.from_property(prop) for prop in view.properties.values()), key=lambda f: f.name)
        )
        self.class_ = APIClass.from_view(view.name or view.external_id, top_level_package)

    def generate_data_class_file(self) -> str:
        type_data = self._env.get_template("type_data.py.jinja")

        return (
            type_data.render(
                class_name=self.class_.data_class,
                fields=self.fields,
                view=self.view,
                top_level_package=self.top_level_package,
            )
            + "\n"
        )

    def generate_api_file(self, top_level_package: str) -> str:
        type_api = self._env.get_template("type_api.py.jinja")

        return type_api.render(
            top_level_package=top_level_package,
            class_=self.class_,
            fields=self.fields,
            view=self.view,
        )


def _to_python_type(type_: dm.DirectRelationReference | dm.PropertyType) -> str:
    if isinstance(type_, (dm.Int32, dm.Int64)):
        out_type = "int"
    elif isinstance(type_, dm.Boolean):
        out_type = "bool"
    elif isinstance(type_, (dm.Float32, dm.Float64)):
        out_type = "float"
    elif isinstance(type_, dm.Date):
        out_type = "datetime.date"
    elif isinstance(type_, dm.Timestamp):
        out_type = "datetime.datetime"
    elif isinstance(type_, dm.Json):
        out_type = "dict"
    elif isinstance(type_, (dm.Text, dm.DirectRelation, dm.CDFExternalIdReference, dm.DirectRelationReference)):
        out_type = "str"
    else:
        raise ValueError(f"Unknown type {type_}")

    return out_type


def find_dependencies(apis: list[APIGenerator]) -> dict[APIClass, set[APIClass]]:
    class_by_data_class_name = {api.class_.data_class: api.class_ for api in apis}
    return {
        api.class_: {class_by_data_class_name[d] for d in dependencies}
        for api in apis
        if (dependencies := api.fields.dependencies)
    }


def _unique_properties(
    prop: dm.MappedProperty | dm.SingleHopConnectionDefinition | dm.MappedProperty | dm.ConnectionDefinition,
) -> dict[str, Any]:
    if isinstance(prop, dm.MappedProperty):
        return {
            "container": prop.container.dump(),
            "container_property_identifier": prop.container_property_identifier,
            "default_value": prop.default_value,
            "name": prop.name,
            "nullable": prop.nullable,
            "type": prop.type.dump(),
        }
    elif isinstance(prop, dm.SingleHopConnectionDefinition):
        return {
            "direction": prop.direction,
            "name": prop.name,
            "type": prop.type.dump(),
        }
    else:
        raise ValueError(f"Unknown property type {prop}")


def _unique_views_properties(view: dm.View) -> dict[str, Any]:
    """
    Returns the properties from a view that uniquely defines it.

    This is necessary as there might be two views that have different versions, but all else is the same,
    thus they can be used to create the same data classes and apis in the generated SDK.

    Parameters
    ----------
    view (dm.View) : The View

    Returns
    -------
        A dictionary with the properties that uniquely defines the view.
    """
    return {
        "name": view.name,
        "externalId": view.external_id,
        "properties": {name: _unique_properties(prop) for name, prop in view.properties.items()},
    }


def get_unique_views(*views: dm.View) -> list[dm.View]:
    view_hashes = set()
    unique_views = []
    for view in views:
        view_hash = hashlib.shake_256(json.dumps(_unique_views_properties(view)).encode("utf-8")).hexdigest(16)
        if view_hash not in view_hashes:
            unique_views.append(view)
            view_hashes.add(view_hash)
    return unique_views
