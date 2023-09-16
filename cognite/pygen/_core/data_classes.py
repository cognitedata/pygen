from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field
from typing_extensions import Self

from cognite.client.data_classes import data_modeling as dm

from cognite.pygen.config import PygenConfig
from cognite.pygen.utils.text import create_name

_PRIMITIVE_TYPES = {"text", "boolean" "float32" "float64" "int32" "int64" "timestamp" "date" "json"}
_EXTERNAL_TYPES = {"timeseries" "file" "sequence"}


@dataclass(frozen=True)
class Field(ABC):
    """
    A field represents a pydantic field in the generated pydantic class.
    """

    name: str

    @classmethod
    def from_property(cls, prop: dm.MappedProperty | dm.ConnectionDefinition, config: PygenConfig) -> Field:
        if prop.type in _PRIMITIVE_TYPES and isinstance(prop, dm.MappedProperty):
            ...
        elif prop.type in _EXTERNAL_TYPES and isinstance(prop, dm.MappedProperty):
            ...
        elif prop.type == "direct" and isinstance(prop, dm.MappedProperty):
            return EdgeField()
        elif isinstance(prop, dm.SingleHopConnectionDefinition):
            return EdgesField()
        else:
            raise NotImplementedError(f"Property type={type(prop)!r} is not supported")


@dataclass(frozen=True)
class PrimitiveField(Field):
    """
    This represents a basic type such as str, int, float, bool, datetime.datetime, datetime.date.
    """

    ...


@dataclass(frozen=True)
class PrimitiveListField(Field):
    """
    This represents a list of basic types such as list[str], list[int], list[float], list[bool], list[datetime.datetime], list[datetime.date].
    """

    ...


@dataclass(frozen=True)
class EdgeField(Field):
    """
    This represents an edge field linking to another data class.
    """

    data_class: DataClass
    ...


@dataclass(frozen=True)
class EdgesField(Field):
    """
    This represents a list of edge fields linking to another data class.
    """

    ...


@dataclass(frozen=True)
class DataClass:
    """
    This represents a data class. It is created from a view.
    """

    read_class_name: str
    write_class_name: str
    read_list_class_name: str
    write_list_class_name: str
    variable_name: str
    file_name: str
    view: dm.ViewId
    fields: list[Field] = field(default_factory=list)

    @classmethod
    def from_view(cls, view: dm.View, config: PygenConfig) -> Self:
        raw_name = view.name or view.external_id
        class_name = create_name(raw_name, config.naming.data_class.name)
        variable_name = create_name(raw_name, config.naming.data_class.variable)
        file_name = f"_{create_name(raw_name, config.naming.data_class.file)}"
        return cls(
            read_class_name=class_name,
            write_class_name=f"{class_name}Apply",
            read_list_class_name=f"{class_name}List",
            write_list_class_name=f"{class_name}ApplyList",
            variable_name=variable_name,
            file_name=file_name,
            view=view.as_id(),
        )

    @property
    def import_(self) -> str:
        return (
            f"from .{self.file_name} "
            f"import {self.read_class_name}, {self.read_class_name}Apply, {self.read_class_name}List"
        )


@dataclass(frozen=True)
class APIClass:
    variable: str
    variable_list: str
    client_attribute: str
    api_class: str
    file_name: str
    data_class: DataClass

    @classmethod
    def from_view(cls, view: dm.View, data_class: DataClass, config: PygenConfig) -> APIClass:
        raw_name = view.name or view.external_id

        return cls(
            variable=create_name(raw_name, config.naming.api_class.variable),
            variable_list=create_name(raw_name, config.naming.api_class.variable_list),
            client_attribute=create_name(raw_name, config.naming.api_class.client_attribute),
            api_class=create_name(raw_name, config.naming.api_class.name),
            file_name=create_name(raw_name, config.naming.api_class.file_name),
            data_class=data_class,
        )


@dataclass(frozen=True)
class APIsClass:
    """
    This represents a set of APIs which are generated from a single data model.

    The motivation for having this class is the case when you want to create one SDK for multiple data models.
    """

    sub_apis: list[APIClass]
    variable: str
    name: str
    model: dm.DataModelId

    @classmethod
    def from_data_model(
        cls, data_model: dm.DataModel, api_class_by_view_id: dict[dm.ViewId, APIClass], config: PygenConfig
    ) -> APIsClass:
        sub_apis = sorted([api_class_by_view_id[view.as_id()] for view in data_model.views], key=lambda api: api.name)

        data_model_name = data_model.name or data_model.external_id

        return cls(
            sub_apis=sub_apis,
            variable=create_name(data_model_name, config.naming.apis_class.variable),
            name=f"{create_name(data_model_name, config.naming.apis_class.name)}APIs",
            model=data_model.as_id(),
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
