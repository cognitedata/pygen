"""
Fields represents the fields of the data classes in the generated SDK.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import total_ordering
from typing import TYPE_CHECKING, Literal, TypeVar, cast

from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.data_modeling.data_types import ListablePropertyType

from cognite.pygen import config as pygen_config
from cognite.pygen.config.reserved_words import is_reserved_word
from cognite.pygen.utils.text import create_name, to_words

if TYPE_CHECKING:
    from . import DataClass, EdgeDataClass, NodeDataClass

_PRIMITIVE_TYPES = (dm.Text, dm.Boolean, dm.Float32, dm.Float64, dm.Int32, dm.Int64, dm.Timestamp, dm.Date, dm.Json)
_EXTERNAL_TYPES = (dm.TimeSeriesReference, dm.FileReference, dm.SequenceReference)

__all__ = [
    "Field",
    "PrimitiveFieldCore",
    "PrimitiveField",
    "PrimitiveListField",
    "CDFExternalField",
    "EdgeField",
    "EdgeOneToManyEdges",
    "EdgeOneToManyNodes",
    "EdgeToOneDataClass",
    "EdgeOneToOne",
    "EdgeOneToMany",
    "EdgeOneToEndNode",
    "T_Field",
]


@dataclass(frozen=True)
class Field(ABC):
    """
    A field represents a pydantic field in the generated pydantic class.

    Args:
        name: The name of the field. This is used in the generated Python code.
        doc_name: The name of the field in the documentation.
        prop_name: The name of the property in the data model. This is used when reading and writing to CDF.
        pydantic_field: The name to use for the import 'from pydantic import Field'. This is used in the edge case
                        when the name 'Field' name clashes with the data model class name.

    """

    name: str
    doc_name: str
    prop_name: str
    description: str | None
    pydantic_field: Literal["Field", "pydantic.Field"]

    @property
    def need_alias(self) -> bool:
        return self.name != self.prop_name

    @classmethod
    def from_property(
        cls,
        prop_name: str,
        prop: dm.MappedProperty | dm.ConnectionDefinition,
        data_class_by_view_id: dict[dm.ViewId, DataClass],
        config: pygen_config.PygenConfig,
        view_id: dm.ViewId,
        pydantic_field: Literal["Field", "pydantic.Field"],
    ) -> Field:
        field_naming = config.naming.field
        name = create_name(prop_name, field_naming.name)
        if is_reserved_word(name, "field", view_id, prop_name):
            name = f"{name}_"

        doc_name = to_words(name, singularize=True)
        variable = create_name(prop_name, field_naming.variable)

        if isinstance(prop, dm.SingleHopConnectionDefinition) and prop.edge_source:
            return EdgeOneToManyEdges(
                name=name,
                doc_name=doc_name,
                prop_name=prop_name,
                variable=variable,
                data_class=data_class_by_view_id[prop.edge_source],
                edge_type=prop.type,
                edge_direction=prop.direction,
                description=prop.description,
                pydantic_field=pydantic_field,
            )
        elif isinstance(prop, dm.SingleHopConnectionDefinition):
            return EdgeOneToManyNodes(
                name=name,
                doc_name=doc_name,
                prop_name=prop_name,
                variable=variable,
                data_class=data_class_by_view_id[prop.source],
                edge_type=prop.type,
                edge_direction=prop.direction,
                description=prop.description,
                pydantic_field=pydantic_field,
            )
        elif isinstance(prop, dm.MappedProperty) and isinstance(prop.type, ListablePropertyType) and prop.type.is_list:
            return PrimitiveListField(
                name=name,
                prop_name=prop_name,
                doc_name=doc_name,
                type_=prop.type,
                is_nullable=prop.nullable,
                pydantic_field=pydantic_field,
                description=prop.description,
            )
        elif isinstance(prop, dm.MappedProperty) and isinstance(prop.type, dm.CDFExternalIdReference):
            # Note: these are only CDF External Fields that are not listable. Listable CDF External Fields
            # are handled above.
            return CDFExternalField(
                name=name,
                prop_name=prop_name,
                doc_name=doc_name,
                type_=prop.type,
                is_nullable=prop.nullable,
                description=prop.description,
                pydantic_field=pydantic_field,
            )
        elif isinstance(prop, dm.MappedProperty) and isinstance(prop.type, dm.DirectRelation):
            if prop.source is not None:
                # Connected in View
                target_data_class = data_class_by_view_id[prop.source]
            else:
                raise NotImplementedError()
                # # Connected in Container
                # # Todo: This is a hack, we are assuming (gambling) that the container ExternalId is the same as the
                # #   view ExternalId. This is not always true.
                # if (
                #     view_id_no_version := ViewSpaceExternalId(prop.container.space, prop.container.external_id)
                # ) in data_class_by_view_id:
                # elif prop.type.container and (
                #         view_id_no_version := ViewSpaceExternalId(
                #             prop.type.container.space, prop.type.container.external_id
                #     in data_class_by_view_id
                # ):

            return EdgeOneToOne(
                name=name,
                prop_name=prop_name,
                description=prop.description,
                data_class=target_data_class,
                pydantic_field=pydantic_field,
                doc_name=doc_name,
            )
        elif isinstance(prop, dm.MappedProperty):
            return PrimitiveField(
                name=name,
                prop_name=prop_name,
                doc_name=doc_name,
                type_=prop.type,
                is_nullable=prop.nullable,
                default=prop.default_value,
                pydantic_field=pydantic_field,
                description=prop.description,
            )
        else:
            raise NotImplementedError(f"Property type={type(prop)!r} is not supported")

    @abstractmethod
    def as_read_type_hint(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def as_write_type_hint(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def as_apply(self) -> str:
        """Used in the .as_apply() method for the read version of the data class."""
        raise NotImplementedError

    @property
    def argument_documentation(self) -> str:
        if self.description:
            return self.description
        else:
            return f"The {self.doc_name} field."

    # The properties below are overwritten in the child classes
    @property
    def is_edge(self) -> bool:
        return False

    @property
    def is_time_field(self) -> bool:
        return False

    @property
    def is_timestamp(self) -> bool:
        return False

    @property
    def is_time_series(self) -> bool:
        return False

    @property
    def is_text_field(self) -> bool:
        return False


T_Field = TypeVar("T_Field", bound=Field)


@dataclass(frozen=True)
class PrimitiveFieldCore(Field, ABC):
    type_: dm.PropertyType
    is_nullable: bool

    @property
    def is_time_field(self) -> bool:
        return isinstance(self.type_, (dm.Timestamp, dm.Date))

    @property
    def is_timestamp(self) -> bool:
        return isinstance(self.type_, dm.Timestamp)

    @property
    def is_text_field(self) -> bool:
        return isinstance(self.type_, dm.Text)

    @property
    def type_as_string(self) -> str:
        return _to_python_type(self.type_)

    def as_apply(self) -> str:
        return f"self.{self.name}"


@dataclass(frozen=True)
class PrimitiveField(PrimitiveFieldCore):
    """
    This represents a basic type such as str, int, float, bool, datetime.datetime, datetime.date.
    """

    default: str | int | dict | None = None

    def as_read_type_hint(self) -> str:
        if self.need_alias:
            return f'Optional[{self.type_as_string}] = {self.pydantic_field}({self.default}, alias="{self.prop_name}")'
        else:
            return f"Optional[{self.type_as_string}] = {self.default}"

    def as_write_type_hint(self) -> str:
        out_type = self.type_as_string
        if self.is_nullable and self.need_alias:
            out_type = f'Optional[{out_type}] = {self.pydantic_field}({self.default}, alias="{self.prop_name}")'
        elif self.need_alias:
            out_type = f'{out_type} = {self.pydantic_field}(alias="{self.prop_name}")'
        elif self.is_nullable:
            out_type = f"Optional[{out_type}] = None"
        elif self.default is not None or self.is_nullable:
            out_type = f"{out_type} = {self.default}"
        return out_type


@dataclass(frozen=True)
class PrimitiveListField(PrimitiveFieldCore):
    """
    This represents a list of basic types such as list[str], list[int], list[float], list[bool],
    list[datetime.datetime], list[datetime.date].
    """

    def as_read_type_hint(self) -> str:
        if self.need_alias:
            return f'Optional[list[{self.type_as_string}]] = {self.pydantic_field}(None, alias="{self.prop_name}")'
        else:
            return f"Optional[list[{self.type_as_string}]] = None"

    def as_write_type_hint(self) -> str:
        type_ = self.type_as_string
        if self.is_nullable and self.need_alias:
            return f'Optional[list[{type_}]] = {self.pydantic_field}(None, alias="{self.prop_name}")'
        elif self.need_alias:
            return f'list[{type_}] = {self.pydantic_field}(alias="{self.prop_name}")'
        elif self.is_nullable:
            return f"Optional[list[{type_}]] = None"
        else:  # not self.is_nullable and not self.need_alias
            return f"list[{type_}]"


@dataclass(frozen=True)
class CDFExternalField(PrimitiveFieldCore):
    @property
    def is_time_series(self) -> bool:
        return isinstance(self.type_, dm.TimeSeriesReference)

    def as_read_type_hint(self) -> str:
        return self.as_write_type_hint()

    def as_write_type_hint(self) -> str:
        type_ = self.type_as_string
        if type_ != "str":
            type_ = f"{type_}, str"

        # CDF External Fields are always nullable
        if self.need_alias:
            out_type = f'Union[{type_}, None] = {self.pydantic_field}(None, alias="{self.prop_name}")'
        else:
            out_type = f"Union[{type_}, None] = None"
        return out_type


@dataclass(frozen=True)
class EdgeField(Field, ABC):
    """This represents a field linking to another data class(es)."""

    @property
    def is_edge(self) -> bool:
        return True


@dataclass(frozen=True)
class EdgeToOneDataClass(EdgeField, ABC):
    """This represents a field linking to a single data class."""

    data_class: DataClass


@dataclass(frozen=True)
class EdgeOneToOne(EdgeToOneDataClass):
    """This represents a one-to-one relation, which direct relation."""

    def as_read_type_hint(self) -> str:
        return self._type_hint(self.data_class.read_name)

    def as_write_type_hint(self) -> str:
        return self._type_hint(self.data_class.write_name)

    def _type_hint(self, data_class_name: str) -> str:
        left_side = f"Union[{data_class_name}, str, dm.NodeId, None] ="
        # Edge fields are always nullable
        if self.need_alias:
            return f'{left_side} {self.pydantic_field}(None, repr=False, alias="{self.prop_name}")'
        else:
            return f"{left_side} {self.pydantic_field}(None, repr=False)"

    def as_apply(self) -> str:
        return f"self.{self.name}.as_apply() if isinstance(self.{self.name}, DomainModel) else self.{self.name}"


@total_ordering
@dataclass(frozen=True)
class EdgeClasses:
    """This represents a specific edge type linking two data classes."""

    start_class: NodeDataClass
    edge_type: dm.DirectRelationReference
    end_class: NodeDataClass

    def __lt__(self, other: EdgeClasses) -> bool:
        if isinstance(other, EdgeClasses):
            return self.end_class < other.end_class
        return NotImplemented

    def __eq__(self, other: object) -> bool:
        if isinstance(other, EdgeClasses):
            return (
                self.end_class == other.end_class
                and self.edge_type == other.edge_type
                and self.start_class == other.start_class
            )
        return NotImplemented


@dataclass(frozen=True)
class EdgeOneToEndNode(EdgeField):
    """This represents a one-to-one edge where the end class can be one of multiple data classes.
    This is used for the end_node field in edge data classes, where the end_node can be one of multiple
    data classes.
    """

    edge_classes: list[EdgeClasses]

    @property
    def end_classes(self) -> list[DataClass]:
        return [edge_class.end_class for edge_class in self.edge_classes]

    def as_read_type_hint(self) -> str:
        return self._type_hint([data_class.read_name for data_class in self.end_classes])

    def as_write_type_hint(self) -> str:
        return self._type_hint([data_class.write_name for data_class in self.end_classes])

    def _type_hint(self, data_class_names: list[str]) -> str:
        data_class_names = list(set(data_class_names))
        left_side = f"Union[{', '.join(sorted(data_class_names))}, str, dm.NodeId]"
        if self.need_alias:
            return f'{left_side} = {self.pydantic_field}(alias="{self.prop_name}")'
        else:
            return left_side

    def as_apply(self) -> str:
        return f"self.{self.name}.as_apply() " f"if isinstance(self.{self.name}, DomainModel) " f"else self.{self.name}"


@dataclass(frozen=True)
class EdgeOneToMany(EdgeToOneDataClass, ABC):
    """This represents a one-to-many edge.
    Args:
        variable: Singular variable name used when iterating through the field.
    """

    variable: str
    edge_type: dm.DirectRelationReference
    edge_direction: Literal["outwards", "inwards"]


@dataclass(frozen=True)
class EdgeOneToManyNodes(EdgeOneToMany):
    """
    This represents a list of edge fields linking to another data class.
    """

    @property
    def node_class(self) -> NodeDataClass:
        from .data_classes import NodeDataClass

        return cast(NodeDataClass, self.data_class)

    def as_apply(self) -> str:
        return (
            f"[{self.variable}.as_apply() if isinstance({self.variable}, DomainModel) else {self.variable} "
            f"for {self.variable} in self.{self.name} or []]"
        )

    def as_read_type_hint(self) -> str:
        return self._type_hint(self.data_class.read_name)

    def as_write_type_hint(self) -> str:
        return self._type_hint(self.data_class.write_name)

    def _type_hint(self, data_class_name: str) -> str:
        left_side = f"Union[list[{data_class_name}], list[str], None]"
        # Edge fields are always nullable
        if self.need_alias:
            return f'{left_side} = {self.pydantic_field}(default=None, repr=False, alias="{self.prop_name}")'
        else:
            return f"{left_side} = {self.pydantic_field}(default=None, repr=False)"


@dataclass(frozen=True)
class EdgeOneToManyEdges(EdgeOneToMany):
    """
    This represents a list of edge fields linking to another data class.
    """

    @property
    def edge_class(self) -> EdgeDataClass:
        from .data_classes import EdgeDataClass

        return cast(EdgeDataClass, self.data_class)

    def as_apply(self) -> str:
        return f"[{self.variable}.as_apply() for {self.variable} in self.{self.name} or []]"

    def as_read_type_hint(self) -> str:
        return self._type_hint(self.data_class.read_name)

    def as_write_type_hint(self) -> str:
        return self._type_hint(self.data_class.write_name)

    def _type_hint(self, data_class_name: str) -> str:
        left_side = f"Optional[list[{data_class_name}]]"
        # Edge fields are always nullable
        if self.need_alias:
            return f'{left_side} = {self.pydantic_field}(default=None, repr=False, alias="{self.prop_name}")'
        else:
            return f"{left_side} = {self.pydantic_field}(default=None, repr=False)"


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
    elif isinstance(type_, dm.TimeSeriesReference):
        out_type = "TimeSeries"
    elif isinstance(type_, (dm.Text, dm.DirectRelation, dm.CDFExternalIdReference, dm.DirectRelationReference)):
        out_type = "str"
    else:
        raise ValueError(f"Unknown type {type_}")

    return out_type
