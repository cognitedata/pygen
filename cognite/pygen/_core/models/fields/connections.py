"""This module contains the fields that contain a connection to another data class. These fields are references to
another object in the data model."""

from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import total_ordering
from typing import TYPE_CHECKING, ClassVar, Literal, cast

from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.data_modeling.views import (
    ReverseDirectRelation,
    SingleEdgeConnection,
    SingleReverseDirectRelation,
)

from cognite.pygen.warnings import MissingReverseDirectRelationTargetWarning

from .base import Field

if TYPE_CHECKING:
    from cognite.pygen._core.models.data_classes import EdgeDataClass, NodeDataClass


@total_ordering
@dataclass(frozen=True)
class EdgeClass:
    """This represents a specific edge type linking two data classes."""

    start_class: NodeDataClass
    edge_type: dm.DirectRelationReference
    end_class: NodeDataClass
    used_directions: set[Literal["outwards", "inwards"]]

    def __lt__(self, other: EdgeClass) -> bool:
        if isinstance(other, EdgeClass):
            return (
                self.start_class.read_name,
                self.end_class.read_name,
                self.edge_type.space,
                self.edge_type.external_id,
            ) < (
                other.start_class.read_name,
                other.end_class.read_name,
                other.edge_type.space,
                other.edge_type.external_id,
            )
        return NotImplemented

    def __eq__(self, other: object) -> bool:
        if isinstance(other, EdgeClass):
            return (
                self.end_class.read_name == other.end_class.read_name
                and self.edge_type == other.edge_type
                and self.start_class.read_name == other.start_class.read_name
            )
        return NotImplemented

    def __repr__(self) -> str:
        return f"EdgeClass({self.start_class.read_name} - {self.edge_type} - {self.end_class.read_name})"


@dataclass(frozen=True)
class EndNodeField(Field):
    """This represents a one-to-one edge where the end class can be one of multiple data classes.
    This is used for the end_node field in edge data classes, where the end_node can be one of multiple
    data classes.

    This is a special class that is not instantiated from a property, but is created in the edge data class.
    """

    edge_classes: list[EdgeClass]

    @property
    def is_connection(self) -> bool:
        return True

    @property
    def destination_classes(self) -> list[NodeDataClass]:
        seen = set()
        output: list[NodeDataClass] = []
        for edge_class in self.edge_classes:
            if "outwards" in edge_class.used_directions and edge_class.end_class.read_name not in seen:
                output.append(edge_class.end_class)
                seen.add(edge_class.end_class.read_name)
            if "inwards" in edge_class.used_directions and edge_class.start_class.read_name not in seen:
                output.append(edge_class.start_class)
                seen.add(edge_class.start_class.read_name)
        return output

    def as_read_type_hint(self) -> str:
        return self._type_hint([data_class.read_name for data_class in self.destination_classes])

    def as_graphql_type_hint(self) -> str:
        if self.destination_classes:
            data_class_names = list(set([data_class.graphql_name for data_class in self.destination_classes]))
        else:
            data_class_names = ["dm.NodeId"]
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
                for data_class in self.destination_classes
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
        if self.destination_classes:
            return f"self.{self.name}.as_write() if isinstance(self.{self.name}, DomainModel) else self.{self.name}"
        else:
            return f"self.{self.name}"

    def as_read_graphql(self) -> str:
        return f"self.{self.name}.as_read() if isinstance(self.{self.name}, GraphQLCore) else self.{self.name}"


@dataclass(frozen=True)
class BaseConnectionField(Field, ABC):
    _wrap_list: ClassVar[bool] = False
    edge_type: dm.DirectRelationReference | None
    edge_direction: Literal["outwards", "inwards"]
    type_hint_node_reference: list[str]
    through: dm.PropertyId | None
    destination_class: NodeDataClass | None
    edge_class: EdgeDataClass | None

    @property
    def linked_class(self) -> NodeDataClass | EdgeDataClass:
        if self.edge_class:
            return self.edge_class
        elif self.destination_class:
            return self.destination_class
        raise ValueError("Bug in Pygen: Direct relation without source does not have a linked class")

    @property
    def reverse_property(self) -> BaseConnectionField:
        if self.through is None:
            raise ValueError("Bug in Pygen: Trying to get reverse property for a non-reverse direct relation")
        if self.destination_class is None:
            raise ValueError("Bug in Pygen: Trying to get reverse property for a direct relation without source")
        other = next((field for field in self.destination_class if field.prop_name == self.through.property), None)
        if other is None:
            raise ValueError(f"Bug in Pygen: Missing reverse property in {self.destination_class.read_name}")
        elif isinstance(other, BaseConnectionField):
            return other
        raise ValueError("Bug in Pygen: Reverse property is not a connection field")

    @property
    def is_direct_relation(self) -> bool:
        """Returns True if the connection is a direct relation."""
        return self.edge_type is None and self.through is None

    @property
    def is_direct_relation_no_source(self) -> bool:
        """Returns True if the connection is a direct relation without a source."""
        return self.is_direct_relation and self.destination_class is None

    @property
    def is_reverse_direct_relation(self) -> bool:
        """Returns True if the connection is a reverse direct relation."""
        return self.through is not None

    @property
    def is_write_field(self) -> bool:
        """Returns True if the connection is writable."""
        return not self.is_reverse_direct_relation

    @property
    def is_edge(self) -> bool:
        """Returns True if the connection is an edge."""
        return self.edge_type is not None

    @property
    def is_edge_without_properties(self) -> bool:
        """Returns True if the connection is an edge without properties."""
        return self.is_edge and self.edge_class is None

    @property
    def is_edge_with_properties(self) -> bool:
        """Returns True if the connection is an edge with properties."""
        return self.is_edge and self.edge_class is not None

    @property
    def is_connection(self) -> bool:
        """Returns True if the connection is a connection."""
        return True

    @property
    def edge_type_str(self) -> str:
        """Returns the edge type as a string."""
        if self.edge_type is None:
            raise ValueError("Bug in Pygen: Missing edge type")
        return f'dm.DirectRelationReference("{ self.edge_type.space }", "{ self.edge_type.external_id }")'

    @property
    def through_str(self) -> str:
        """Returns the through property as a string."""
        if self.through is None:
            raise ValueError("Bug in Pygen: Missing through property")
        view_id = cast(dm.ViewId, self.through.source)
        return (
            f'dm.ViewId("{view_id.space}", "{view_id.external_id}", '
            f'"{view_id.version}").as_property_ref("{self.through.property}")'
        )

    @property
    def is_one_to_many(self) -> bool:
        """Returns True if the connection is a one-to-many connection."""
        raise NotImplementedError()

    @property
    def is_one_to_one(self) -> bool:
        """Returns True if the connection is a one-to-one connection."""
        return not self.is_one_to_many

    @classmethod
    def load(
        cls,
        base: Field,
        prop: dm.ConnectionDefinition | dm.MappedProperty,
        variable: str,
        node_class_by_view_id: dict[dm.ViewId, NodeDataClass],
        edge_class_by_view_id: dict[dm.ViewId, EdgeDataClass],
        has_default_instance_space: bool,
        view_id: dm.ViewId,
        direct_relations_by_view_id: dict[dm.ViewId, set[str]],
    ) -> Field | None:
        """Load a connection field from a property"""
        if not isinstance(prop, dm.EdgeConnection | dm.MappedProperty | ReverseDirectRelation):
            return None
        if isinstance(prop, ReverseDirectRelation):
            field_string = f"{view_id}.{base.prop_name}"
            target = (
                direct_relations_by_view_id.get(prop.through.source)
                if isinstance(prop.through.source, dm.ViewId)
                else None
            )
            if target is None or prop.through.property not in target:
                target_str = (
                    f"{prop.through.source}.{prop.through.property}" if target is not None else str(prop.through.source)
                )
                warnings.warn(MissingReverseDirectRelationTargetWarning(target_str, field_string), stacklevel=2)
                return None
        edge_type = prop.type if isinstance(prop, dm.EdgeConnection) else None
        direction: Literal["outwards", "inwards"]
        if isinstance(prop, dm.EdgeConnection):
            direction = prop.direction
        elif isinstance(prop, dm.MappedProperty):
            direction = "outwards"
        elif isinstance(prop, ReverseDirectRelation):
            direction = "inwards"
        else:
            warnings.warn(f"Unknown connection type {prop}", stacklevel=2)
            return None

        through = prop.through if isinstance(prop, ReverseDirectRelation) else None

        destination_class = node_class_by_view_id.get(prop.source) if prop.source else None
        type_hint_node_reference = ["str", "dm.NodeId"] if has_default_instance_space else ["dm.NodeId"]
        if isinstance(prop, ReverseDirectRelation) or (isinstance(prop, dm.EdgeConnection) and prop.edge_source):
            type_hint_node_reference = []
        edge_class = (
            edge_class_by_view_id.get(prop.edge_source)
            if isinstance(prop, dm.EdgeConnection) and prop.edge_source
            else None
        )

        if cls._is_supported_one_to_many_connection(prop):
            return OneToManyConnectionField(
                name=base.name,
                doc_name=base.doc_name,
                prop_name=base.prop_name,
                pydantic_field=base.pydantic_field,
                through=through,
                variable=variable,
                edge_type=edge_type,
                edge_direction=direction,
                description=prop.description,
                type_hint_node_reference=type_hint_node_reference,
                destination_class=destination_class,
                edge_class=edge_class,
            )
        elif cls._is_supported_one_to_one_connection(prop):
            return OneToOneConnectionField(
                name=base.name,
                doc_name=base.doc_name,
                prop_name=base.prop_name,
                pydantic_field=base.pydantic_field,
                edge_type=edge_type,
                through=through,
                edge_direction=direction,
                description=prop.description,
                type_hint_node_reference=type_hint_node_reference,
                destination_class=destination_class,
                edge_class=edge_class,
            )
        else:
            return None

    @classmethod
    def _is_supported_one_to_many_connection(cls, prop: dm.ConnectionDefinition | dm.MappedProperty) -> bool:
        if isinstance(prop, dm.MultiEdgeConnection):
            return True
        elif isinstance(prop, dm.MappedProperty) and isinstance(prop.type, dm.DirectRelation) and prop.type.is_list:
            return True
        elif isinstance(prop, dm.MultiReverseDirectRelation):
            return True
        return False

    @classmethod
    def _is_supported_one_to_one_connection(cls, prop: dm.ConnectionDefinition | dm.MappedProperty) -> bool:
        if isinstance(prop, dm.MappedProperty) and isinstance(prop.type, dm.DirectRelation) and not prop.type.is_list:
            return True
        elif isinstance(prop, SingleEdgeConnection):
            return True
        elif isinstance(prop, SingleReverseDirectRelation):
            return True
        return False

    def as_read_type_hint(self) -> str:
        """Return the type hint for the field in the read data class."""
        if self.edge_class:
            types = [self.edge_class.read_name]
        elif self.destination_class:
            types = [self.destination_class.read_name]
        else:
            types = []
        return self._create_type_hint(types, self.type_hint_node_reference)

    def as_write_type_hint(self) -> str:
        """Return the type hint for the field in the write data class."""

        cls_: NodeDataClass | EdgeDataClass | None = None
        if self.edge_class:
            cls_ = self.edge_class
        elif self.destination_class:
            cls_ = self.destination_class

        types: list[str] = []
        if cls_ and (cls_.is_writable or cls_.is_interface):
            types = [cls_.write_name]

        return self._create_type_hint(types, self.type_hint_node_reference)

    def as_graphql_type_hint(self) -> str:
        """Return the type hint for the field in the GraphQL data class."""
        if self.edge_class:
            types = [self.edge_class.graphql_name]
        elif self.destination_class:
            types = [self.destination_class.graphql_name]
        else:
            types = []
        return self._create_type_hint(types, [])

    def _create_type_hint(self, types: list[str], type_hint_node_reference: list[str]) -> str:
        field_kwargs = {
            #  All connection fields are nullable
            "default": "None",
        }
        if types:
            field_kwargs["repr"] = "False"
        if self.need_alias:
            field_kwargs["alias"] = f'"{self.prop_name}"'
        types.extend(type_hint_node_reference)
        types_hint = ", ".join(types)
        if self._wrap_list and len(types) == 1:
            types_hint = f"list[{types_hint}]"
        elif self._wrap_list:
            types_hint = f"list[Union[{types_hint}]]"
        field_args = ", ".join([f"{key}={value}" for key, value in field_kwargs.items()])
        if len(types) == 1 or self._wrap_list:
            type_hint = f"Optional[{types_hint}]"
        elif len(types) == 0:
            # GraphQL Hint for direct relation with no source
            type_hint = "Optional[str]"
        else:
            type_hint = f"Union[{types_hint}, None]"
        return f"{type_hint} = {self.pydantic_field}({field_args})"

    def as_write(self) -> str:
        """Return the code to convert the field from read to write data class."""
        method = "as_write"
        if self.destination_class and not self.destination_class.is_writable:
            method = "as_id"

        base_cls = "DomainRelation" if self.is_edge_with_properties else "DomainModel"

        return self._create_as_method(method, base_cls, bool(self.type_hint_node_reference))

    def as_read_graphql(self) -> str:
        """Return the code to convert the field from the GraphQL to the read data class."""
        return self._create_as_method("as_read", "GraphQLCore", False)

    def as_write_graphql(self) -> str:
        """Return the code to convert the field from the write data class to the GraphQL."""
        return self._create_as_method("as_write", "GraphQLCore", False)

    @abstractmethod
    def _create_as_method(self, method: str, base_cls: str, use_node_reference: bool) -> str:
        raise NotImplementedError()


@dataclass(frozen=True)
class OneToManyConnectionField(BaseConnectionField):
    _wrap_list: ClassVar[bool] = True
    variable: str

    @property
    def is_one_to_many(self) -> bool:
        return True

    def _create_as_method(self, method: str, base_cls: str, use_node_reference: bool) -> str:
        if self.destination_class and use_node_reference:
            inner = f"{self.variable}.{method}() if isinstance({self.variable}, {base_cls}) else {self.variable}"
        elif self.destination_class:
            inner = f"{self.variable}.{method}()"
        else:
            inner = f"{self.variable}"
        return f"[{inner} for {self.variable} in self.{self.name} or []]"

    def as_value(self) -> str:
        if not self.is_direct_relation:
            raise NotImplementedError("as_value is not implemented for edge fields")
        return f"""[
                {{
                "space": self.space if isinstance({ self.variable }, str) else { self.variable }.space,
                "externalId": { self.variable } if isinstance({self.variable}, str) else {self.variable}.external_id,
                }}
                for { self.variable } in self.{ self.name } or []
            ]"""

    def as_typed_hint(self, operation: Literal["write", "read"] = "write") -> str:
        if self.is_direct_relation and operation == "write":
            return "list[DirectRelationReference | tuple[str, str]] | None = None"
        elif self.is_direct_relation and operation == "read":
            return "list[DirectRelationReference] | None = None"
        raise NotImplementedError("as_typed_hint is not implemented for edge fields")

    def as_typed_init_set(self) -> str:
        if self.is_direct_relation:
            return (
                f"[DirectRelationReference.load({self.variable}) for {self.variable} in {self.name}] "
                f"if {self.name} else None"
            )
        return self.name


@dataclass(frozen=True)
class OneToOneConnectionField(BaseConnectionField):
    _wrap_list: ClassVar[bool] = False

    @property
    def is_one_to_many(self) -> bool:
        return False

    def _create_as_method(self, method: str, base_cls: str, use_node_reference: bool) -> str:
        if self.destination_class:
            return f"self.{self.name}.{method}()\nif isinstance(self.{self.name}, {base_cls})\nelse self.{self.name}"
        else:
            return f"self.{self.name}"

    def as_value(self) -> str:
        if not self.is_direct_relation:
            raise NotImplementedError("as_value is not implemented for edge fields")
        return f"""{{
                "space":  self.space if isinstance(self.{ self.name }, str) else self.{ self.name }.space,
                "externalId": self.{ self.name } if isinstance(self.{self.name}, str) else self.{self.name}.external_id,
            }}"""

    def as_typed_hint(self, operation: Literal["write", "read"] = "write") -> str:
        if self.is_direct_relation and operation == "write":
            return "DirectRelationReference | tuple[str, str] | None = None"
        elif self.is_direct_relation and operation == "read":
            return "DirectRelationReference | None = None"
        raise NotImplementedError("as_typed_hint is not implemented for edge fields")

    def as_typed_init_set(self) -> str:
        if self.is_direct_relation:
            return f"DirectRelationReference.load({self.name}) if {self.name} else None"
        return self.name
