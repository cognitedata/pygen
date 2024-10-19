"""This module contains the fields that are references to CDF External Fields. These fields are referencing
either TimeSeries, Sequence, or File objects. In other words, these fields are references to objects that are
outside of the data model."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.data_modeling.data_types import ListablePropertyType

from .base import Field
from .primitive import BasePrimitiveField, PrimitiveListField


@dataclass(frozen=True)
class CDFExternalField(BasePrimitiveField):
    """This represents a field that is a reference to a CDF External Field.

    For example, a field that is a reference to a TimeSeries, Sequence, or File.
    """

    @property
    def type_as_string(self) -> str:
        if isinstance(self.type_, dm.TimeSeriesReference):
            return "TimeSeries"
        elif isinstance(self.type_, dm.SequenceReference):
            return "SequenceRead"
        elif isinstance(self.type_, dm.FileReference):
            return "FileMetadata"
        else:
            raise ValueError(f"Unknown CDF External Field type: {self.type_}")

    @property
    def resource_write_name(self) -> str:
        if isinstance(self.type_, dm.TimeSeriesReference):
            return "time_series"
        elif isinstance(self.type_, dm.SequenceReference):
            return "sequences"
        elif isinstance(self.type_, dm.FileReference):
            return "files"
        else:
            raise ValueError(f"Unknown CDF External Field type: {self.type_}")

    def cognite_type_name(self, format: Literal["read", "write"]) -> str:
        type_name = self.type_name
        if format == "write":
            return f"Cognite{type_name}Write"
        else:
            return f"Cognite{type_name}"

    @property
    def type_name(self) -> str:
        return self.type_as_string.removesuffix("Read")

    @property
    def is_time_series(self) -> bool:
        return isinstance(self.type_, dm.TimeSeriesReference)

    @property
    def is_list(self) -> bool:
        return isinstance(self.type_, ListablePropertyType) and self.type_.is_list

    def as_read_type_hint(self) -> str:
        return self._create_type_hint(self.type_as_string)

    def as_graphql_type_hint(self) -> str:
        type_ = f"{self.type_name}GraphQL"
        if self.need_alias:
            return f'Optional[{type_}] = {self.pydantic_field}(None, alias="{self.prop_name}")'
        else:
            return f"Optional[{type_}] = None"

    def as_write_type_hint(self) -> str:
        type_ = f"{self.type_name}Write"
        return self._create_type_hint(type_)

    def _create_type_hint(self, type_: str) -> str:
        # CDF External Fields are always nullable
        if self.need_alias:
            return f'Union[{type_}, str, None] = {self.pydantic_field}(None, alias="{self.prop_name}")'
        else:
            return f"Union[{type_}, str, None] = None"

    def as_write(self) -> str:
        type_ = self.type_name
        return f"self.{self.name}.as_write() if isinstance(self.{self.name}, Cognite{type_}) else self.{self.name}"

    def as_write_graphql(self) -> str:
        return f"self.{self.name}.as_write() if self.{self.name} else None"

    def as_read_graphql(self) -> str:
        # Read is only used in graphql
        return f"self.{self.name}.as_read() if self.{self.name} else None"

    def as_value(self) -> str:
        return (
            f"self.{self.name} if isinstance(self.{self.name}, str) or self.{self.name} is None "
            f"else self.{self.name}.external_id"
        )

    @classmethod
    def load(cls, base: Field, prop: dm.MappedProperty, variable: str) -> CDFExternalField | None:
        if not isinstance(prop.type, dm.CDFExternalIdReference):
            return None
        if prop.type.is_list:
            return CDFExternalListField(
                name=base.name,
                prop_name=base.prop_name,
                doc_name=base.doc_name,
                type_=prop.type,
                is_nullable=prop.nullable,
                description=prop.description,
                pydantic_field=base.pydantic_field,
                variable=variable,
            )
        else:
            return CDFExternalField(
                name=base.name,
                prop_name=base.prop_name,
                doc_name=base.doc_name,
                type_=prop.type,
                is_nullable=prop.nullable,
                description=prop.description,
                pydantic_field=base.pydantic_field,
            )


@dataclass(frozen=True)
class CDFExternalListField(PrimitiveListField, CDFExternalField):
    """
    This represents a list of CDF types such as list[TimeSeries], list[Sequence], or list[File].
    """

    def as_read_type_hint(self) -> str:
        return self._create_type_hint(self.type_as_string)

    def _create_type_hint(self, type_: str) -> str:
        if self.need_alias:
            return f'Optional[list[Union[{type_}, str]]] = {self.pydantic_field}(None, alias="{self.prop_name}")'
        else:
            return f"Optional[list[Union[{type_}, str]]] = None"

    def as_write_type_hint(self) -> str:
        type_ = f"{self.type_name}Write"
        return self._create_type_hint(type_)

    def as_graphql_type_hint(self) -> str:
        type_ = f"{self.type_name}GraphQL"
        if self.need_alias:
            return f'Optional[list[{type_}]] = {self.pydantic_field}(None, alias="{self.prop_name}")'
        else:
            return f"Optional[list[{type_}]] = None"

    def as_write(self) -> str:
        return (
            f"[{self.variable}.as_write() if isinstance({self.variable}, {self.cognite_type_name('read')}) "
            f"else {self.variable}"
            f" for {self.variable} in self.{self.name}] if self.{self.name} is not None else None"
        )

    def as_write_graphql(self) -> str:
        return (
            f"[{self.variable}.as_write() for {self.variable} in self.{self.name} or []]"
            f" if self.{self.name} is not None else None"
        )

    def as_read_graphql(self) -> str:
        # Read is only used in graphql
        return (
            f"[{self.variable}.as_read() for {self.variable} in self.{self.name} or []]"
            f" if self.{self.name} is not None else None"
        )

    def as_value(self) -> str:
        return (
            f"[{self.variable} if isinstance({self.variable}, str) else {self.variable}.external_id "
            f"for {self.variable} in self.{self.name} or []] if self.{self.name} is not None else None"
        )
