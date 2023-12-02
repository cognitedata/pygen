"""This represents the generated data classes in the SDK"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, cast

from cognite.client.data_classes import data_modeling as dm

from cognite.pygen import config as pygen_config
from .fields import (
    Field,
    EdgeOneToMany,
    RequiredEdgeOneToOne,
)
from .filter_method import FilterMethod, FilterParameter
from ._base import DataClass
from cognite.pygen.config.reserved_words import is_reserved_word
from cognite.pygen.utils.text import create_name, to_words


@dataclass
class NodeDataClass(DataClass):
    @property
    def import_pydantic_field(self) -> str:
        if self.pydantic_field == "Field":
            return "from pydantic import Field"
        else:
            return "import pydantic"

    @property
    def is_edge_class(self) -> bool:
        return False


@dataclass
class EdgeDataClass(DataClass):
    _start_class: NodeDataClass | None = None
    _end_class: NodeDataClass | None = None
    _edge_type: dm.DirectRelationReference | None = None

    @property
    def start_class(self) -> NodeDataClass:
        if self._start_class is None:
            raise ValueError("EdgeDataClass has not been initialized.")
        return self._start_class

    @property
    def end_class(self) -> NodeDataClass:
        if self._end_class is None:
            raise ValueError("EdgeDataClass has not been initialized.")
        return self._end_class

    @property
    def edge_type(self) -> dm.DirectRelationReference:
        if self._edge_type is None:
            raise ValueError("EdgeDataClass has not been initialized.")
        return self._edge_type

    @classmethod
    def from_field_data_classes(cls, field: EdgeOneToMany, data_class: NodeDataClass, config: pygen_config.PygenConfig):
        edge_fields: list[Field] = []  # Space and External ID are automatically added
        list_method = FilterMethod.from_fields(edge_fields, config.filtering, is_edge_class=True)
        return cls(
            field.data_class.view_name,
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            field.data_class.view_id,
            field.data_class.view_version,
            edge_fields,
            list_method,
            data_class,
            cast(NodeDataClass, field.data_class),
            field.prop.type,
        )

    def node_parameters(self, instance_space: str) -> Iterable[FilterParameter]:
        nodes = [
            (self.start_class, "source", self.start_class.variable),
            (self.end_class, "target", self.end_class.variable),
        ]
        if self.start_class.variable == self.end_class.variable:
            nodes = [
                (self.start_class, "source", f"from_{self.start_class.variable}"),
                (self.end_class, "target", f"to_{self.end_class.variable}"),
            ]

        for class_, location, name in nodes:
            yield FilterParameter(
                name=name,
                type_="str | list[str] | dm.NodeId | list[dm.NodeId]",
                description=f"ID of the {location} { class_.doc_list_name}.",
                default=None,
            )
            yield FilterParameter(
                name=f"{name}_space",
                type_="str",
                description=f"Location of the {class_.doc_list_name}.",
                default=f'"{instance_space}"',
                is_nullable=False,
            )


@dataclass
class EdgeWithPropertyDataClass(DataClass):
    _start_class: NodeDataClass | None = None
    _end_class: NodeDataClass | None = None
    _edge_type: dm.DirectRelationReference | None = None

    @property
    def is_edge_class(self) -> bool:
        return True

    @property
    def start_class(self) -> NodeDataClass:
        if self._start_class is None:
            raise ValueError("EdgeDataClass has not been initialized.")
        return self._start_class

    @property
    def end_class(self) -> NodeDataClass:
        if self._end_class is None:
            raise ValueError("EdgeDataClass has not been initialized.")
        return self._end_class

    @property
    def edge_type(self) -> dm.DirectRelationReference:
        if self._edge_type is None:
            raise ValueError("EdgeDataClass has not been initialized.")
        return self._edge_type

    def update_nodes(
        self,
        data_class_by_view_id: dict[dm.ViewId, DataClass],
        views: Iterable[dm.View],
        field_naming: pygen_config.FieldNaming,
    ):
        source_view, source_property, prop_name = self._find_source_view_and_property(views)
        self._start_class = cast(NodeDataClass, data_class_by_view_id[source_view.as_id()])
        self._end_class = cast(NodeDataClass, data_class_by_view_id[source_property.source])
        self._edge_type = source_property.type

        end_class = self._end_class
        # Todo avoid repeating the code below here and the load method
        name = create_name(end_class.view_name, field_naming.name)
        view_id = dm.ViewId(end_class.view_id.space, end_class.view_id.external_id, end_class.view_version)
        if is_reserved_word(name, "field", view_id, end_class.view_name):
            name = f"{name}_"

        doc_name = to_words(name, singularize=True)

        variable = create_name(end_class.view_name, field_naming.variable)

        edge_api_class_input = f"{self.view_name}_{end_class.view_name}"
        edge_api_class = f"{create_name(edge_api_class_input, field_naming.edge_api_class)}API"
        edge_api_attribute = f"{create_name(end_class.view_name, field_naming.api_class_attribute)}_edge"

        self.fields.append(
            RequiredEdgeOneToOne(
                name=name,
                doc_name=doc_name,
                prop_name=name,
                pydantic_field=self.pydantic_field,
                data_class=self.end_class,
                prop=source_property,
                variable=variable,
                edge_api_class=edge_api_class,
                edge_api_attribute=edge_api_attribute,
            )
        )

    def _find_source_view_and_property(
        self, views: Iterable[dm.View]
    ) -> tuple[dm.View, dm.SingleHopConnectionDefinition, str]:
        for view in views:
            for prop_name, prop in view.properties.items():
                if (
                    isinstance(prop, dm.SingleHopConnectionDefinition)
                    and prop.edge_source
                    and prop.edge_source.space == self.view_id.space
                    and prop.edge_source.external_id == self.view_id.external_id
                ):
                    return view, prop, prop_name
        raise ValueError("Could not find source view and property")

    @property
    def import_pydantic_field(self) -> str:
        if self.pydantic_field == "Field":
            return "from pydantic import Field"
        else:
            return "import pydantic"

    def node_parameters(self, instance_space: str) -> Iterable[FilterParameter]:
        nodes = [
            (self.start_class, "source", self.start_class.variable),
            (self.end_class, "target", self.end_class.variable),
        ]
        if self.start_class.variable == self.end_class.variable:
            nodes = [
                (self.start_class, "source", f"from_{self.start_class.variable}"),
                (self.end_class, "target", f"to_{self.end_class.variable}"),
            ]

        for class_, location, name in nodes:
            yield FilterParameter(
                name=name,
                type_="str | list[str] | dm.NodeId | list[dm.NodeId]",
                description=f"ID of the {location} { class_.doc_list_name}.",
                default=None,
            )
            yield FilterParameter(
                name=f"{name}_space",
                type_="str",
                description=f"Location of the {class_.doc_list_name}.",
                default=f'"{instance_space}"',
                is_nullable=False,
            )
