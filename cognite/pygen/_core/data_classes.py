from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from cognite.client.data_classes.data_modeling.data_types import ListablePropertyType
from typing_extensions import Self

from cognite.client.data_classes import data_modeling as dm

from cognite.pygen.config import PygenConfig
from cognite.pygen.utils.text import create_name

_PRIMITIVE_TYPES = (dm.Text, dm.Boolean, dm.Float32, dm.Float64, dm.Int32, dm.Int64, dm.Timestamp, dm.Date, dm.Json)
_EXTERNAL_TYPES = (dm.TimeSeriesReference, dm.FileReference, dm.SequenceReference)


@dataclass(frozen=True)
class Field(ABC):
    """
    A field represents a pydantic field in the generated pydantic class.
    """

    name: str
    prop_name: str

    @property
    def need_alias(self) -> bool:
        return self.name != self.prop_name

    @classmethod
    def from_property(
        cls,
        prop_name: str,
        prop: dm.MappedProperty | dm.ConnectionDefinition,
        data_class_by_view_id: dict[dm.ViewId, DataClass],
        config: PygenConfig,
    ) -> Field:
        name = create_name(prop_name, config.naming.data_class.field)
        if isinstance(prop, dm.SingleHopConnectionDefinition):
            return EdgeOneToMany(
                name=name, prop_name=prop_name, prop=prop, data_class=data_class_by_view_id[prop.source]
            )
        if not isinstance(prop, dm.MappedProperty):
            raise ValueError(f"Property type={type(prop)!r} is not supported")

        if isinstance(prop.type, _PRIMITIVE_TYPES) or isinstance(prop.type, _EXTERNAL_TYPES):
            type_ = _to_python_type(prop.type)
            if isinstance(prop.type, ListablePropertyType) and prop.type.is_list:
                return PrimitiveListField(
                    name=name,
                    prop_name=prop_name,
                    type_=type_,
                    is_nullable=prop.nullable,
                    prop=prop,
                )

            return PrimitiveField(
                name=name,
                prop_name=prop_name,
                type_=type_,
                is_nullable=prop.nullable,
                default=prop.default_value,
                prop=prop,
            )
        elif isinstance(prop.type, dm.DirectRelation):
            return EdgeOneToOne(
                name=name, prop_name=prop_name, prop=prop, data_class=data_class_by_view_id[prop.source]
            )
        else:
            raise NotImplementedError(f"Property type={type(prop)!r} is not supported")

    @abstractmethod
    def as_read_type_hint(self) -> str:
        ...

    @abstractmethod
    def as_write_type_hint(self) -> str:
        ...


@dataclass(frozen=True)
class PrimitiveFieldCore(Field, ABC):
    type_: str
    is_nullable: bool
    prop: dm.MappedProperty


@dataclass(frozen=True)
class PrimitiveField(PrimitiveFieldCore):
    """
    This represents a basic type such as str, int, float, bool, datetime.datetime, datetime.date.
    """

    default: str | int | dict | None = None

    def as_read_type_hint(self) -> str:
        rhs = str(self.default)
        if self.need_alias:
            rhs = f'Field({rhs}, alias="{self.prop_name}")'

        return f"Optional[{self.type_}] = {rhs}"

    def as_write_type_hint(self) -> str:
        out_type = self.type_
        if self.is_nullable:
            out_type = f"Optional[{out_type}] = {self.default}"
        return out_type


@dataclass(frozen=True)
class PrimitiveListField(PrimitiveFieldCore):
    """
    This represents a list of basic types such as list[str], list[int], list[float], list[bool], list[datetime.datetime], list[datetime.date].
    """

    def as_read_type_hint(self) -> str:
        rhs = ""
        if self.need_alias:
            rhs = f', alias="{self.prop_name}"'

        return f"list[{self.type_}] = Field(default_factory=list{rhs})"

    def as_write_type_hint(self) -> str:
        rhs = ""
        if self.is_nullable:
            rhs = f" = Field(default_factory=list)"
        return f"list[{self.type_}]{rhs}"


@dataclass(frozen=True)
class EdgeField(Field, ABC):
    """
    This represents an edge field linking to another data class.
    """

    data_class: DataClass


@dataclass(frozen=True)
class EdgeOneToOne(EdgeField):
    """
    This represents an edge field linking to another data class.
    """

    prop: dm.MappedProperty

    def as_read_type_hint(self) -> str:
        return "Optional[str] = None"

    def as_write_type_hint(self) -> str:
        return f"Union[{self.data_class.write_class_name}, str, None] = Field(None, repr=False)"


@dataclass(frozen=True)
class EdgeOneToMany(EdgeField):
    """
    This represents a list of edge fields linking to another data class.
    """

    prop: dm.SingleHopConnectionDefinition

    def as_read_type_hint(self) -> str:
        return "list[str] = Field(default_factory=list)"

    def as_write_type_hint(self) -> str:
        return f"Union[list[{self.data_class.write_class_name}], list[str]] = Field(default_factory=list, repr=False)"


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
    view_id: dm.ViewId
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
            view_id=view.as_id(),
        )

    def update_fields(
        self,
        properties: dict[str, dm.MappedProperty | dm.ConnectionDefinition],
        data_class_by_view_id: dict[dm.ViewId, DataClass],
        config: PygenConfig,
    ) -> None:
        for prop_name, prop in properties.items():
            field_ = Field.from_property(prop_name, prop, data_class_by_view_id, config)
            self.fields.append(field_)

    @property
    def import_(self) -> str:
        return (
            f"from .{self.file_name} "
            f"import {self.read_class_name}, {self.write_list_class_name}, {self.read_list_class_name}"
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
