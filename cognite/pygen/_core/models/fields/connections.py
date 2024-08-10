"""This module contains the fields that contain a connection to another data class. These fields are references to
another object in the data model."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import total_ordering
from typing import TYPE_CHECKING, ClassVar, Literal

from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.data_modeling.views import (
    ReverseDirectRelation,
    SingleEdgeConnection,
    SingleReverseDirectRelation,
)

from .base import Field

if TYPE_CHECKING:
    from cognite.pygen._core.models.data_classes import DataClass, EdgeDataClass, NodeDataClass


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

    edge_classes: list[EdgeClasses]

    @property
    def is_connection(self) -> bool:
        return True

    @property
    def end_classes(self) -> list[NodeDataClass]:
        seen = set()
        output: list[NodeDataClass] = []
        for edge_class in self.edge_classes:
            if edge_class.end_class.read_name not in seen:
                output.append(edge_class.end_class)
                seen.add(edge_class.end_class.read_name)
        return output

    def as_read_type_hint(self) -> str:
        return self._type_hint([data_class.read_name for data_class in self.end_classes])

    def as_graphql_type_hint(self) -> str:
        if self.end_classes:
            data_class_names = list(set([data_class.graphql_name for data_class in self.end_classes]))
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
        if self.end_classes:
            return f"self.{self.name}.as_write() if isinstance(self.{self.name}, DomainModel) else self.{self.name}"
        else:
            return f"self.{self.name}"

    def as_read_graphql(self) -> str:
        return f"self.{self.name}.as_read() if isinstance(self.{self.name}, GraphQLCore) else self.{self.name}"


@dataclass(frozen=True)
class BaseConnectionField(Field, ABC):
    _node_reference: ClassVar[list[str]] = ["str", "dm.NodeId"]
    _wrap_list: ClassVar[bool] = False
    edge_type: dm.DirectRelationReference | None
    edge_direction: Literal["outwards", "inwards"]
    end_classes: list[DataClass] | None
    use_node_reference: bool
    through: dm.PropertyId | None

    @property
    def data_class(self) -> DataClass:
        if self.end_classes is None:
            raise ValueError("Bug in Pygen: Missing end class")
        elif len(self.end_classes) > 1:
            raise ValueError("Bug in Pygen: Multiple end classes")
        return self.end_classes[0]

    @property
    def reverse_property(self) -> BaseConnectionField:
        if self.through is None:
            raise ValueError("Bug in Pygen: Trying to get reverse property for a non-reverse direct relation")
        other = next((field for field in self.data_class if field.prop_name == self.through.property), None)
        if other is None:
            raise ValueError(f"Bug in Pygen: Missing reverse property in {self.data_class.read_name}")
        elif isinstance(other, BaseConnectionField):
            return other
        raise ValueError("Bug in Pygen: Reverse property is not a connection field")

    @property
    def is_direct_relation(self) -> bool:
        return self.edge_type is None and self.through is None

    @property
    def is_reverse_direct_relation(self) -> bool:
        return self.through is not None

    @property
    def is_write_field(self) -> bool:
        return not self.is_reverse_direct_relation

    @property
    def is_edge(self) -> bool:
        return self.edge_type is not None

    @property
    def is_no_property_edge(self) -> bool:
        from cognite.pygen._core.models.data_classes import NodeDataClass

        return self.is_edge and all(isinstance(data_class, NodeDataClass) for data_class in self.end_classes or [])

    @property
    def is_property_edge(self) -> bool:
        from cognite.pygen._core.models.data_classes import EdgeDataClass

        return self.is_edge and all(isinstance(data_class, EdgeDataClass) for data_class in self.end_classes or [])

    @property
    def is_connection(self) -> bool:
        return True

    @property
    def edge_type_str(self) -> str:
        if self.edge_type is None:
            raise ValueError("Bug in Pygen: Missing edge type")
        return f'dm.DirectRelationReference("{ self.edge_type.space }", "{ self.edge_type.external_id }")'

    @property
    def is_one_to_many(self) -> bool:
        raise NotImplementedError()

    @property
    def is_one_to_one(self) -> bool:
        return not self.is_one_to_many

    @property
    def destination_classes(self) -> list[DataClass]:
        from cognite.pygen._core.models.data_classes import EdgeDataClass

        output: list[DataClass] = []
        seen: set[str] = set()
        for data_class in self.end_classes or []:
            if isinstance(data_class, EdgeDataClass):
                for edge_class in data_class.end_node_field.edge_classes:
                    if self.edge_direction == "outwards":
                        destination = edge_class.end_class
                    else:
                        destination = edge_class.start_class
                    if destination.read_name not in seen:
                        output.append(destination)
                        seen.add(destination.read_name)
            else:
                raise ValueError("Bug in Pygen: Destination classes should only be edge data classes")
        return output

    @classmethod
    def load(
        cls,
        base: Field,
        prop: dm.ConnectionDefinition | dm.MappedProperty,
        variable: str,
        node_class_by_view_id: dict[dm.ViewId, NodeDataClass],
        edge_class_by_view_id: dict[dm.ViewId, EdgeDataClass],
    ) -> Field | None:
        if not isinstance(prop, (dm.EdgeConnection, dm.MappedProperty, ReverseDirectRelation)):
            return None
        edge_type = prop.type if isinstance(prop, dm.EdgeConnection) else None
        direction: Literal["outwards", "inwards"] = (
            prop.direction if isinstance(prop, dm.EdgeConnection) else "outwards"
        )
        through = prop.through if isinstance(prop, ReverseDirectRelation) else None
        use_node_reference = True
        end_classes: list[DataClass] | None
        if isinstance(prop, dm.EdgeConnection) and prop.edge_source:
            end_classes = [edge_class_by_view_id[prop.edge_source]]
            use_node_reference = False
        elif (
            isinstance(prop, dm.ConnectionDefinition) or isinstance(prop, dm.MappedProperty)
        ) and prop.source is not None:
            end_classes = [node_class_by_view_id[prop.source]]
            if isinstance(prop, ReverseDirectRelation):
                use_node_reference = False
        else:
            end_classes = None

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
                end_classes=end_classes,
                use_node_reference=use_node_reference,
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
                end_classes=end_classes,
                use_node_reference=use_node_reference,
            )
        else:
            return None

    @classmethod
    def _is_supported_one_to_many_connection(cls, prop: dm.ConnectionDefinition | dm.MappedProperty) -> bool:
        if isinstance(prop, dm.MultiEdgeConnection):
            return True
        elif isinstance(prop, SingleEdgeConnection) and prop.direction == "inwards" and not prop.edge_source:
            return True
        elif isinstance(prop, dm.MappedProperty) and isinstance(prop.type, dm.DirectRelation) and prop.type.is_list:
            return True
        elif isinstance(prop, dm.MultiReverseDirectRelation):
            return True
        return False

    @classmethod
    def _is_supported_one_to_one_connection(cls, prop: dm.ConnectionDefinition | dm.MappedProperty) -> bool:
        if isinstance(prop, dm.MappedProperty) and isinstance(prop.type, dm.DirectRelation):
            return True
        elif isinstance(prop, SingleEdgeConnection) and prop.direction == "outwards" and not prop.edge_source:
            return True
        elif isinstance(prop, SingleReverseDirectRelation):
            return True
        return False

    def as_read_type_hint(self) -> str:
        return self._create_type_hint(
            [data_class.read_name for data_class in self.end_classes or []], self.use_node_reference
        )

    def as_write_type_hint(self) -> str:
        return self._create_type_hint(
            [
                data_class.write_name
                for data_class in self.end_classes or []
                if data_class.is_writable or data_class.is_interface
            ],
            self.use_node_reference,
        )

    def as_graphql_type_hint(self) -> str:
        types = [data_class.graphql_name for data_class in self.end_classes or []]
        return self._create_type_hint(types, False)

    def _create_type_hint(self, types: list[str], use_node_reference: bool) -> str:
        field_kwargs = {
            #  All connection fields are nullable
            "default": "None",
        }
        if types:
            field_kwargs["repr"] = "False"
        if self.need_alias:
            field_kwargs["alias"] = f'"{self.prop_name}"'
        if use_node_reference:
            types.extend(self._node_reference)
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
        method = "as_write"
        if isinstance(self.end_classes, list) and len(self.end_classes) == 1 and not self.end_classes[0].is_writable:
            method = "as_id"

        return self._create_as_method(method, "DomainModel", self.use_node_reference)

    def as_read_graphql(self) -> str:
        return self._create_as_method("as_read", "GraphQLCore", False)

    def as_write_graphql(self) -> str:
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
        if self.end_classes and use_node_reference:
            inner = f"{self.variable}.{method}() if isinstance({self.variable}, {base_cls}) else {self.variable}"
        elif self.end_classes:
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
        if self.end_classes:
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
