"""This represents the generated data classes in the SDK"""
from __future__ import annotations

import warnings
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Literal, cast, overload

from cognite.client.data_classes import data_modeling as dm

from cognite.pygen import config as pygen_config
from cognite.pygen.config.reserved_words import is_reserved_word
from cognite.pygen.utils.text import create_name, to_words

from .fields import (
    CDFExternalField,
    EdgeField,
    EdgeOneToManyNodes,
    Field,
    PrimitiveField,
    PrimitiveFieldCore,
    T_Field,
)
from .filter_method import FilterMethod, FilterParameter


@dataclass
class DataClass:
    """
    This represents a data class. It is created from a view.
    """

    read_name: str
    write_name: str
    read_list_name: str
    write_list_name: str
    doc_name: str
    doc_list_name: str
    variable: str
    variable_list: str
    file_name: str
    view_id: dm.ViewId
    fields: list[Field]

    @staticmethod
    def to_view_name(view: dm.View) -> str:
        return (view.name or view.external_id).replace(" ", "_")

    @classmethod
    def from_view(cls, view: dm.View, view_name: str, data_class: pygen_config.DataClassNaming) -> DataClass:
        class_name = create_name(view_name, data_class.name)
        if is_reserved_word(class_name, "data class", view.as_id()):
            class_name = f"{class_name}_"

        variable_name = create_name(view_name, data_class.variable)
        variable_list = create_name(view_name, data_class.variable_list)
        doc_name = to_words(view_name, singularize=True)
        doc_list_name = to_words(view_name, pluralize=True)
        if variable_name == variable_list:
            variable_list = f"{variable_list}_list"
        raw_file_name = create_name(view_name, data_class.file)
        file_name = f"_{raw_file_name}"
        if is_reserved_word(file_name, "filename", view.as_id()):
            file_name = f"{file_name}_"

        used_for = view.used_for
        if used_for == "all":
            used_for = "node"
            warnings.warn("View used_for is set to 'all'. This is not supported. Using 'node' instead.", stacklevel=2)

        args = dict(
            read_name=class_name,
            write_name=f"{class_name}Apply",
            read_list_name=f"{class_name}List",
            write_list_name=f"{class_name}ApplyList",
            doc_name=doc_name,
            doc_list_name=doc_list_name,
            variable=variable_name,
            variable_list=variable_list,
            file_name=file_name,
            view_id=view.as_id(),
            fields=[],
        )

        if used_for == "node":
            return NodeDataClass(**args)
        elif used_for == "edge":
            raise NotImplementedError
        else:
            raise ValueError(f"Unsupported used_for={used_for}")

    def update_fields(
        self,
        properties: dict[str, dm.MappedProperty | dm.ConnectionDefinition],
        data_class_by_view_id: dict[dm.ViewId, DataClass],
        config: pygen_config.PygenConfig,
    ) -> None:
        if self.fields:
            # Data class is already initialized, this happens when a data class is used for two or more views.
            return
        pydantic_field = self.pydantic_field
        for prop_name, prop in properties.items():
            field_ = Field.from_property(
                prop_name,
                prop,
                data_class_by_view_id,
                config,
                self.view_id,
                pydantic_field=pydantic_field,
            )
            self.fields.append(field_)

    @property
    def text_field_names(self) -> str:
        return f"{self.read_name}TextFields"

    @property
    def field_names(self) -> str:
        return f"{self.read_name}Fields"

    @property
    def properties_dict_name(self) -> str:
        return f"_{self.read_name.upper()}_PROPERTIES_BY_FIELD"

    @property
    def pydantic_field(self) -> Literal["Field", "pydantic.Field"]:
        if any(
            name == "Field" for name in [self.read_name, self.write_name, self.read_list_name, self.write_list_name]
        ):
            return "pydantic.Field"
        else:
            return "Field"

    @property
    def init_import(self) -> str:
        import_classes = [self.read_name, self.write_name, self.read_list_name, self.write_list_name]
        if self.has_primitive_fields:
            import_classes.append(self.field_names)
        if self.has_text_field:
            import_classes.append(self.text_field_names)
        return f"from .{self.file_name} import {', '.join(sorted(import_classes))}"

    def __iter__(self) -> Iterator[Field]:
        return iter(self.fields)

    @overload
    def fields_of_type(self, type_: type[T_Field]) -> Iterable[T_Field]:
        ...

    @overload
    def fields_of_type(self, type_: tuple[type[Field], ...]) -> Iterable[tuple[type[Field], ...]]:
        ...

    def fields_of_type(
        self, field_type: type[T_Field] | tuple[type[Field], ...]
    ) -> Iterable[T_Field] | Iterable[tuple[type[Field], ...]]:
        return (field_ for field_ in self if isinstance(field_, field_type))

    def has_field_of_type(self, type_: type[Field] | tuple[type[Field], ...]) -> bool:
        return any(isinstance(field_, type_) for field_ in self)

    def is_all_fields_of_type(self, type_: type[Field] | tuple[type[Field], ...]) -> bool:
        return all(isinstance(field_, type_) for field_ in self)

    def primitive_fields_of_type(self, type_: type[dm.PropertyType]) -> Iterable[PrimitiveFieldCore]:
        return (field_ for field_ in self.fields_of_type(PrimitiveFieldCore) if isinstance(field_.type_, type_))

    def has_primitive_fields_of_type(self, type_: type[dm.PropertyType] | tuple[type[dm.PropertyType], ...]) -> bool:
        return any(self.primitive_fields_of_type(type_))

    def is_all_primitive_fields_of_type(self, type_: type[dm.PropertyType] | tuple[type[dm.PropertyType], ...]) -> bool:
        return all(self.primitive_fields_of_type(type_))

    @property
    def text_fields(self) -> Iterable[PrimitiveFieldCore]:
        return (field_ for field_ in self.primitive_core_fields if field_.is_text_field)

    @property
    def single_timeseries_fields(self) -> Iterable[CDFExternalField]:
        return (field_ for field_ in self.cdf_external_fields if isinstance(field_.type_, dm.TimeSeriesReference))

    @property
    def has_time_field(self) -> bool:
        return any(field_.is_time_field for field_ in self.fields)

    @property
    def has_text_field(self) -> bool:
        return any(field_.is_text_field for field_ in self.fields)

    @property
    def _field_type_hints(self) -> Iterable[str]:
        return (hint for field_ in self.fields for hint in (field_.as_read_type_hint(), field_.as_write_type_hint()))

    @property
    def use_optional_type(self) -> bool:
        return any("Optional" in hint for hint in self._field_type_hints)

    @property
    def use_pydantic_field(self) -> bool:
        pydantic_field = self.pydantic_field
        return any(pydantic_field in hint for hint in self._field_type_hints)

    @property
    def dependencies(self) -> list[DataClass]:
        unique: dict[dm.ViewId, DataClass] = {}
        for field_ in self.fields:
            if isinstance(field_, EdgeField):
                # This will overwrite any existing data class with the same view id
                # however, this is not a problem as all data classes are uniquely identified by their view id
                unique[field_.data_class.view_id] = field_.data_class
        return sorted(unique.values(), key=lambda x: x.write_name)

    # @property
    # def dependencies_edges(self) -> list[EdgeWithPropertyDataClass]:

    # @property
    # def has_single_timeseries_fields(self) -> bool:
    #     return any(
    #         for field_ in self.single_timeseries_fields

    @property
    def primitive_fields_literal(self) -> str:
        return ", ".join(
            f'"{field_.prop_name}"' for field_ in self if isinstance(field_, (PrimitiveField, CDFExternalField))
        )

    @property
    def text_fields_literals(self) -> str:
        return ", ".join(f'"{field_.name}"' for field_ in self.text_fields)

    # @property
    # def one_to_many_edges_docs(self) -> str:
    #     if len(edges) == 1:

    @property
    def fields_literals(self) -> str:
        return ", ".join(f'"{field_.name}"' for field_ in self if isinstance(field_, PrimitiveFieldCore))

    @property
    def filter_name(self) -> str:
        return f"_create_{self.variable}_filter"

    @property
    def is_edge_class(self) -> bool:
        return False


@dataclass
class NodeDataClass(DataClass):
    @property
    def import_pydantic_field(self) -> str:
        if self.pydantic_field == "Field":
            return "from pydantic import Field"
        else:
            return "import pydantic"


@dataclass
class EdgeDataClass(DataClass):
    _start_class: NodeDataClass | None = None
    _end_class: NodeDataClass | None = None
    _edge_type: dm.DirectRelationReference | None = None

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

    @classmethod
    def from_field_data_classes(
        cls, field: EdgeOneToManyNodes, data_class: NodeDataClass, config: pygen_config.PygenConfig
    ):
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


# @dataclass
# class EdgeWithPropertyDataClass(DataClass):
#
#     @property
#     def is_edge_class(self) -> bool:
#
#     @property
#     def start_class(self) -> NodeDataClass:
#         if self._start_class is None:
#
#     @property
#     def end_class(self) -> NodeDataClass:
#         if self._end_class is None:
#
#     @property
#     def edge_type(self) -> dm.DirectRelationReference:
#         if self._edge_type is None:
#
#     def update_nodes(
#         self,
#         data_class_by_view_id: dict[dm.ViewId, DataClass],
#         views: Iterable[dm.View],
#         field_naming: pygen_config.FieldNaming,
#     ):
#
#         # Todo avoid repeating the code below here and the load method
#         if is_reserved_word(name, "field", view_id, end_class.view_name):
#
#
#
#
#         self.fields.append(
#             EdgeOneToEndNode(
#
#     def _find_source_view_and_property(
#         self, views: Iterable[dm.View]
#     ) -> tuple[dm.View, dm.SingleHopConnectionDefinition, str]:
#         for view in views:
#             for prop_name, prop in view.properties.items():
#                 if (
#                     and prop.edge_source
#                     and prop.edge_source.space == self.view_id.space
#                     and prop.edge_source.external_id == self.view_id.external_id
#                 ):
#
#     @property
#     def import_pydantic_field(self) -> str:
#         if self.pydantic_field == "Field":
#
#     def node_parameters(self, instance_space: str) -> Iterable[FilterParameter]:
#         if self.start_class.variable == self.end_class.variable:
#
#         for class_, location, name in nodes:
#             yield FilterParameter(
#             yield FilterParameter(
