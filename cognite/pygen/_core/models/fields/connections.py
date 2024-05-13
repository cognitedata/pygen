"""This module contains the fields that contain a connection to another data class."""

from __future__ import annotations

import warnings
from abc import ABC
from dataclasses import dataclass
from functools import total_ordering
from typing import TYPE_CHECKING, ClassVar, Literal, cast

from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.data_modeling import MultiEdgeConnection
from cognite.client.data_classes.data_modeling.views import ReverseDirectRelation, SingleEdgeConnection

from .base import Field

if TYPE_CHECKING:
    from cognite.pygen._core.models.data_classes import DataClass, EdgeDataClass, NodeDataClass


@dataclass(frozen=True)
class EdgeField(Field, ABC):
    """This represents a field connecting to another data class(es)."""

    @property
    def is_edge(self) -> bool:
        return True

    @classmethod
    def load(
        cls,
        base: Field,
        prop: dm.ConnectionDefinition | dm.MappedProperty,
        variable: str,
        data_class_by_view_id: dict[dm.ViewId, DataClass],
    ) -> Field | None:
        if isinstance(prop, MultiEdgeConnection):
            if prop.edge_source:
                return EdgeOneToManyEdges(
                    name=base.name,
                    doc_name=base.doc_name,
                    prop_name=base.prop_name,
                    variable=variable,
                    data_class=data_class_by_view_id[prop.edge_source],
                    edge_type=prop.type,
                    edge_direction=prop.direction,
                    description=prop.description,
                    pydantic_field=base.pydantic_field,
                )
            else:
                return EdgeOneToManyNodes(
                    name=base.name,
                    doc_name=base.doc_name,
                    prop_name=base.prop_name,
                    variable=variable,
                    data_class=data_class_by_view_id[prop.source],
                    edge_type=prop.type,
                    edge_direction=prop.direction,
                    description=prop.description,
                    pydantic_field=base.pydantic_field,
                )
        elif isinstance(prop, dm.MappedProperty) and isinstance(prop.type, dm.DirectRelation):
            if prop.source:
                target_data_class = data_class_by_view_id[prop.source]
                return EdgeOneToOne(
                    name=base.name,
                    prop_name=base.prop_name,
                    description=prop.description,
                    data_class=target_data_class,
                    pydantic_field=base.pydantic_field,
                    doc_name=base.doc_name,
                )
            else:
                return EdgeOneToOneAny(
                    name=base.name,
                    prop_name=base.prop_name,
                    description=prop.description,
                    pydantic_field=base.pydantic_field,
                    doc_name=base.doc_name,
                )
        elif isinstance(prop, SingleEdgeConnection) and prop.edge_source:
            warnings.warn(
                f"SingleEdgeConnection with edge properties is not yet supported, skipping {base.prop_name}.",
                UserWarning,
                stacklevel=2,
            )
        elif isinstance(prop, SingleEdgeConnection):
            target_data_class = data_class_by_view_id[prop.source]
            return EdgeTypedOneToOne(
                name=base.name,
                variable=variable,
                edge_type=prop.type,
                edge_direction=prop.direction,
                prop_name=base.prop_name,
                description=prop.description,
                data_class=target_data_class,
                pydantic_field=base.pydantic_field,
                doc_name=base.doc_name,
            )
        elif isinstance(prop, ReverseDirectRelation):
            # ReverseDirectRelation are skipped as they are not used in the generated SDK.
            return None
        else:
            warnings.warn(
                f"Property type={type(prop)} is not yet supported, skipping {base.prop_name}.",
                UserWarning,
                stacklevel=2,
            )
        return None


@dataclass(frozen=True)
class EdgeToOneDataClass(EdgeField, ABC):
    """This represents a field connecting to a single data class."""

    data_class: DataClass


@dataclass(frozen=True)
class EdgeOneToOne(EdgeToOneDataClass):
    """This represents a one-to-one relation, which direct relation."""

    def as_read_type_hint(self) -> str:
        left_side = f"Union[{self.data_class.read_name}, str, dm.NodeId, None] ="
        return self._type_hint(left_side)

    def as_graphql_type_hint(self) -> str:
        left_side = f"Optional[{self.data_class.graphql_name}] ="
        return self._type_hint(left_side)

    def as_write_type_hint(self) -> str:
        if self.data_class.is_writable or self.data_class.is_interface:
            left_side = f"Union[{self.data_class.write_name}, str, dm.NodeId, None] ="
            repr_field = True
        else:
            left_side = "Union[str, dm.NodeId, None] ="
            repr_field = False
        return self._type_hint(left_side, repr_field)

    def _type_hint(self, left_side: str, repr_field: bool = True) -> str:
        # Edge fields are always nullable
        repr_ = ", repr=False" if repr_field else ""
        if self.need_alias:
            return f'{left_side} {self.pydantic_field}(None{repr_}, alias="{self.prop_name}")'
        else:
            return f"{left_side} {self.pydantic_field}(None{repr_})"

    def as_write(self) -> str:
        return f"self.{self.name}.as_write() if isinstance(self.{self.name}, DomainModel) else self.{self.name}"

    def as_read_graphql(self) -> str:
        return f"self.{self.name}.as_read() if isinstance(self.{self.name}, GraphQLCore) else self.{self.name}"


@dataclass(frozen=True)
class EdgeTypedOneToOne(EdgeToOneDataClass):
    """This represents a one-to-one relation that is implemented as an edge"""

    edge_type: dm.DirectRelationReference
    edge_direction: Literal["outwards", "inwards"]
    variable: str

    def as_read_type_hint(self) -> str:
        if self.edge_direction == "outwards":
            left_side = f"Union[{self.data_class.read_name}, str, dm.NodeId, None] ="
        else:
            left_side = f"Union[list[{self.data_class.read_name}], list[str], list[dm.NodeId], None] ="
        return self._type_hint(left_side)

    def as_graphql_type_hint(self) -> str:
        if self.edge_direction == "outwards":
            left_side = f"Optional[{self.data_class.graphql_name}] ="
        else:
            left_side = f"Optional[list[{self.data_class.graphql_name}]] ="
        return self._type_hint(left_side)

    def as_write_type_hint(self) -> str:
        if self.data_class.is_writable or self.data_class.is_interface:
            if self.edge_direction == "outwards":
                left_side = f"Union[{self.data_class.write_name}, str, dm.NodeId, None] ="
            else:
                left_side = f"Union[list[{self.data_class.write_name}], list[str], list[dm.NodeId], None] ="
        else:
            if self.edge_direction == "outwards":
                left_side = "Union[str, dm.NodeId, None] ="
            else:
                left_side = "Union[list[str], list[dm.NodeId], None] ="
        return self._type_hint(left_side)

    def _type_hint(self, left_side: str) -> str:
        # Edge fields are always nullable
        if self.need_alias:
            return f'{left_side} {self.pydantic_field}(None, repr=False, alias="{self.prop_name}")'
        else:
            return f"{left_side} {self.pydantic_field}(None, repr=False)"

    def as_write(self) -> str:
        return f"self.{self.name}.as_write() if isinstance(self.{self.name}, DomainModel) else self.{self.name}"

    def as_read_graphql(self) -> str:
        return f"self.{self.name}.as_read() if isinstance(self.{self.name}, GraphQLCore) else self.{self.name}"


@dataclass(frozen=True)
class EdgeOneToOneAny(EdgeField):
    """This represent a direct relation that has not specified the end class (i.e. no source was set)."""

    def as_read_type_hint(self) -> str:
        left_side = "Union[str, dm.NodeId, None] ="
        return self._type_hint(left_side)

    def as_write_type_hint(self) -> str:
        return self.as_read_type_hint()

    def as_graphql_type_hint(self) -> str:
        left_side = "Optional[str] ="
        return self._type_hint(left_side)

    def _type_hint(self, left_side: str) -> str:
        # Edge fields are always nullable
        if self.need_alias:
            return f'{left_side} {self.pydantic_field}(None, alias="{self.prop_name}")'
        else:
            return f"{left_side} None"

    def as_write(self) -> str:
        return f"self.{self.name}"

    def as_read_graphql(self) -> str:
        return f"self.{self.name}"


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

    def as_graphql_type_hint(self) -> str:
        data_class_names = list(set([data_class.graphql_name for data_class in self.end_classes]))
        data_class_names_hint = ", ".join(sorted(data_class_names))
        left_side = f"Union[{data_class_names_hint}, None]"
        if self.need_alias:
            return f'{left_side} = {self.pydantic_field}(None, alias="{self.prop_name}")'
        else:
            return f"{left_side} = None"

    def as_write_type_hint(self) -> str:
        return self._type_hint(
            [
                data_class.write_name
                for data_class in self.end_classes
                if data_class.is_writable or data_class.is_interface
            ]
        )

    def _type_hint(self, data_class_names: list[str]) -> str:
        data_class_names = list(set(data_class_names))
        data_class_names_hint = ", ".join(sorted(data_class_names))
        if data_class_names_hint:
            left_side = f"Union[{data_class_names_hint}, str, dm.NodeId]"
        else:
            left_side = "Union[str, dm.NodeId]"
        if self.need_alias:
            return f'{left_side} = {self.pydantic_field}(alias="{self.prop_name}")'
        else:
            return left_side

    def as_write(self) -> str:
        return f"self.{self.name}.as_write() if isinstance(self.{self.name}, DomainModel) else self.{self.name}"

    def as_read_graphql(self) -> str:
        return f"self.{self.name}.as_read() if isinstance(self.{self.name}, GraphQLCore) else self.{self.name}"


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
        from cognite.pygen._core.models.data_classes import NodeDataClass

        return cast(NodeDataClass, self.data_class)

    def as_write(self) -> str:
        return (
            f"[{self.variable}.as_write() if isinstance({self.variable}, DomainModel) else {self.variable} "
            f"for {self.variable} in self.{self.name} or []]"
        )

    def as_read_graphql(self) -> str:
        return (
            f"[{self.variable}.as_read() if isinstance({self.variable}, GraphQLCore) else {self.variable} "
            f"for {self.variable} in self.{self.name} or []]"
        )

    def as_graphql_type_hint(self) -> str:
        left_side = f"Optional[list[{self.data_class.graphql_name}]]"
        return self._type_hint(left_side)

    def as_read_type_hint(self) -> str:
        left_side = f"Union[list[{self.data_class.read_name}], list[str], list[dm.NodeId], None]"
        return self._type_hint(left_side)

    def as_write_type_hint(self) -> str:
        if self.data_class.is_writable or self.data_class.is_interface:
            left_side = f"Union[list[{self.data_class.write_name}], list[str], list[dm.NodeId], None]"
            repr_field = True
        else:
            left_side = "Union[list[str], None]"
            repr_field = False
        return self._type_hint(left_side, repr_field)

    def _type_hint(self, left_side: str, repr_field: bool = True) -> str:
        # Edge fields are always nullable
        repr_ = ", repr=False" if repr_field else ""
        if self.need_alias:
            return f'{left_side} = {self.pydantic_field}(default=None{repr_}, alias="{self.prop_name}")'
        else:
            return f"{left_side} = {self.pydantic_field}(default=None{repr_})"


@dataclass(frozen=True)
class EdgeOneToManyEdges(EdgeOneToMany):
    """
    This represents a list of edge fields linking to another data class.
    """

    @property
    def edge_class(self) -> EdgeDataClass:
        from cognite.pygen._core.models.data_classes import EdgeDataClass

        return cast(EdgeDataClass, self.data_class)

    def as_write(self) -> str:
        return f"[{self.variable}.as_write() for {self.variable} in self.{self.name} or []]"

    def as_read_graphql(self) -> str:
        return f"[{self.variable}.as_read() for {self.variable} in self.{self.name} or []]"

    def as_read_type_hint(self) -> str:
        return self._type_hint(self.data_class.read_name)

    def as_graphql_type_hint(self) -> str:
        return self._type_hint(self.data_class.graphql_name)

    def as_write_type_hint(self) -> str:
        return self._type_hint(self.data_class.write_name)

    def _type_hint(self, data_class_name: str) -> str:
        left_side = f"Optional[list[{data_class_name}]]"
        # Edge fields are always nullable
        if self.need_alias:
            return f'{left_side} = {self.pydantic_field}(default=None, repr=False, alias="{self.prop_name}")'
        else:
            return f"{left_side} = {self.pydantic_field}(default=None, repr=False)"


@dataclass(frozen=True)
class BaseConnectionField(Field, ABC):
    reference: ClassVar[list[str]] = ["str", "dm.NodeId"]
    edge_type: dm.DirectRelationReference | None
    direction: Literal["outwards", "inwards"] | None
    end_classes: list[DataClass] | None

    @property
    def is_relation(self) -> bool:
        return self.edge_type is None

    @property
    def is_edge(self) -> bool:
        return True

    @classmethod
    def load(
        cls,
        base: Field,
        prop: dm.ConnectionDefinition | dm.MappedProperty,
        variable: str,
        data_class_by_view_id: dict[dm.ViewId, DataClass],
    ) -> Field | None:
        if isinstance(prop, MultiEdgeConnection):
            return OneToManyConnectionField(
                name=base.name,
                doc_name=base.doc_name,
                prop_name=base.prop_name,
                pydantic_field=base.pydantic_field,
                variable=variable,
                edge_type=prop.type,
                direction=prop.direction,
                description=prop.description,
                end_classes=[data_class_by_view_id[prop.source]],
            )
        return None

    @property
    def _type_hint_right_side(self) -> str:
        if self.need_alias:
            return f'{self.pydantic_field}(default=None, repr=False, alias="{self.prop_name}")'
        else:
            return f"{self.pydantic_field}(default=None, repr=False)"


@dataclass(frozen=True)
class OneToManyConnectionField(BaseConnectionField):
    variable: str


@dataclass(frozen=True)
class OneToOneConnectionField(BaseConnectionField): ...
