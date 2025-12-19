"""This module contains the primitive fields. A primitive field is a field that contains pure data, such as a string,
it is in contrast to a connection field that contains a reference to another object."""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Literal

from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.data_modeling.data_types import Enum, ListablePropertyType

from cognite.pygen._constants import is_readonly_property

from .base import Field


@dataclass(frozen=True)
class ContainerProperty:
    source: dm.ContainerId
    identifier: str


@dataclass(frozen=True)
class BasePrimitiveField(Field, ABC):
    """This is a base class for all primitive fields

    For example, a field that is a bool, str, int, float, datetime.datetime, datetime.date, and so on, including
    any list of these types.
    """

    type_: dm.PropertyType
    is_nullable: bool
    container: ContainerProperty

    @property
    def is_write_field(self) -> bool:
        return not is_readonly_property(self.container.source, self.container.identifier)

    @property
    def is_time_field(self) -> bool:
        return isinstance(self.type_, dm.Timestamp | dm.Date)

    @property
    def is_timestamp(self) -> bool:
        return isinstance(self.type_, dm.Timestamp)

    @property
    def is_text_field(self) -> bool:
        return isinstance(self.type_, dm.Text)

    @property
    def type_as_string(self) -> str:
        return _to_python_type(self.type_)

    def as_typed_hint(self, operation: Literal["write", "read"] = "write") -> str:
        type_ = _to_python_type(self.type_, typed=True, operation=operation)
        if isinstance(self.type_, ListablePropertyType) and self.type_.is_list:
            type_ = f"list[{type_}]"
        if self.is_nullable:
            type_ = f"{type_} | None = None"
        return type_

    @classmethod
    def load(cls, base: Field, prop: dm.MappedProperty, variable: str) -> BasePrimitiveField | None:
        container = ContainerProperty(prop.container, prop.container_property_identifier)
        if isinstance(prop.type, ListablePropertyType) and prop.type.is_list:
            return PrimitiveListField(
                name=base.name,
                doc_name=base.doc_name,
                prop_name=base.prop_name,
                description=base.description,
                pydantic_field=base.pydantic_field,
                type_=prop.type,
                is_nullable=prop.nullable,
                variable=variable,
                container=container,
            )
        else:
            return PrimitiveField(
                name=base.name,
                doc_name=base.doc_name,
                prop_name=base.prop_name,
                description=base.description,
                pydantic_field=base.pydantic_field,
                type_=prop.type,
                is_nullable=prop.nullable,
                default=prop.default_value,
                container=container,
            )


@dataclass(frozen=True)
class PrimitiveField(BasePrimitiveField):
    """
    This represents a basic type such as str, int, float, bool, datetime.datetime, datetime.date.
    """

    default: str | int | dict | None = None

    @property
    def support_filtering(self) -> bool:
        return isinstance(
            self.type_, dm.Int32 | dm.Int64 | dm.Float32 | dm.Float64 | dm.Boolean | dm.Text | dm.Date | dm.Timestamp
        )

    @property
    def filtering_cls(self) -> str:
        if isinstance(self.type_, dm.Int32 | dm.Int64):
            return "IntFilter"
        elif isinstance(self.type_, dm.Float32 | dm.Float64):
            return "FloatFilter"
        elif isinstance(self.type_, dm.Boolean):
            return "BooleanFilter"
        elif isinstance(self.type_, dm.Text):
            return "StringFilter"
        elif isinstance(self.type_, dm.Date):
            return "DateFilter"
        elif isinstance(self.type_, dm.Timestamp):
            return "TimestampFilter"
        else:
            raise ValueError(f"type {self.type_} is not supported for filtering")

    @property
    def filter_attribute(self) -> str:
        return self.name

    @property
    def default_code(self) -> str:
        if self.default is None:
            return "None"
        elif isinstance(self.default, str):
            return f'"{self.default}"'
        elif isinstance(self.default, dict):
            return f"{self.default}"
        else:
            return f"{self.default}"

    def as_read_type_hint(self) -> str:
        # We allow string for enum responses. This is in case a new value is added to the enum
        str_ = " | str" if isinstance(self.type_, Enum) else ""
        if self.need_alias and self.is_nullable:
            return f"Optional[{self.type_as_string}]{str_} = {self.pydantic_field}" f'(None, alias="{self.prop_name}")'
        elif self.need_alias:
            return f'{self.type_as_string}{str_} = {self.pydantic_field}(alias="{self.prop_name}")'
        elif self.is_nullable:
            return f"Optional[{self.type_as_string}]{str_} = None"
        else:
            return f"{self.type_as_string}{str_}"

    def as_graphql_type_hint(self) -> str:
        if self.need_alias:
            return f"Optional[{self.type_as_string}] = {self.pydantic_field}" f'(None, alias="{self.prop_name}")'
        else:
            return f"Optional[{self.type_as_string}] = None"

    def as_write_type_hint(self) -> str:
        out_type = self.type_as_string
        if self.is_nullable and self.need_alias:
            out_type = (
                f"Optional[{self.type_as_string}] = "
                f'{self.pydantic_field}({self.default_code}, alias="{self.prop_name}")'
            )
        elif self.need_alias:
            out_type = f'{self.type_as_string} = {self.pydantic_field}(alias="{self.prop_name}")'
        elif self.is_nullable:
            out_type = f"Optional[{self.type_as_string}] = None"
        elif self.default is not None or self.is_nullable:
            out_type = f"{self.type_as_string} = {self.default_code}"
        return out_type


@dataclass(frozen=True)
class PrimitiveListField(BasePrimitiveField):
    """
    This represents a list of basic types such as list[str], list[int], list[float], list[bool],
    list[datetime.datetime], list[datetime.date].
    """

    variable: str

    def as_read_type_hint(self) -> str:
        str_ = " | str" if isinstance(self.type_, Enum) else ""
        return self.as_write_type_hint(str_)

    def as_graphql_type_hint(self) -> str:
        if self.need_alias:
            return f'Optional[list[{self.type_as_string}]] = {self.pydantic_field}(None, alias="{self.prop_name}")'
        else:
            return f"Optional[list[{self.type_as_string}]] = None"

    def as_write_type_hint(self, str_: str = "") -> str:
        type_ = self.type_as_string
        if self.is_nullable and self.need_alias:
            return f'Optional[list[{type_}]{str_}] = {self.pydantic_field}(None, alias="{self.prop_name}")'
        elif self.need_alias:
            return f'list[{type_}{str_}] = {self.pydantic_field}(alias="{self.prop_name}")'
        elif self.is_nullable:
            return f"Optional[list[{type_}{str_}]] = None"
        else:  # not self.is_nullable and not self.need_alias
            return f"list[{type_}{str_}]"

    @property
    def is_list(self) -> bool:
        return True

    def as_value(self) -> str:
        base = f"self.{self.name}"
        if isinstance(self.type_, dm.Date):
            return f"[{self.variable}.isoformat() for {self.variable} in {base} or []]"
        elif isinstance(self.type_, dm.Timestamp):
            return f'[{self.variable}.isoformat(timespec="milliseconds") for {self.variable} in {base} or []]'
        else:
            return base


def _to_python_type(
    type_: dm.DirectRelationReference | dm.PropertyType,
    typed: bool = False,
    operation: Literal["read", "write"] = "write",
) -> str:
    if isinstance(type_, dm.Int32 | dm.Int64):
        out_type = "int"
    elif isinstance(type_, dm.Boolean):
        out_type = "bool"
    elif isinstance(type_, dm.Float32 | dm.Float64):
        out_type = "float"
    elif isinstance(type_, dm.Date):
        if typed:
            out_type = "date"
        else:
            out_type = "datetime.date"
    elif isinstance(type_, dm.Timestamp):
        if typed:
            out_type = "datetime"
        else:
            out_type = "datetime.datetime"
    elif isinstance(type_, dm.Json):
        out_type = "dict"
    elif isinstance(type_, dm.TimeSeriesReference):
        if typed:
            out_type = "str"
        else:
            out_type = "TimeSeries"
    elif isinstance(type_, dm.Text | dm.DirectRelation | dm.CDFExternalIdReference | dm.DirectRelationReference):
        if typed and isinstance(type_, dm.DirectRelation | dm.DirectRelationReference):
            if operation == "write":
                out_type = "DirectRelationReference | tuple[str, str]"
            else:
                out_type = "DirectRelationReference"
        else:
            out_type = "str"
    elif isinstance(type_, Enum):
        values = ", ".join(f'"{k}"' for k in sorted(type_.values.keys()))
        out_type = f"Literal[{values}]"
    else:
        raise ValueError(f"Unknown type {type_}")

    return out_type
