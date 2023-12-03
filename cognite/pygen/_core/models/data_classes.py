"""This represents the generated data classes in the SDK"""
from __future__ import annotations

import warnings
from abc import abstractmethod
from collections import defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from typing import cast, overload

from cognite.client.data_classes import data_modeling as dm

from cognite.pygen import config as pygen_config
from cognite.pygen.config.reserved_words import is_reserved_word
from cognite.pygen.utils.text import create_name, to_words

from .fields import (
    EdgeOneToEndNode,
    EdgeOneToManyNodes,
    Field,
    PrimitiveField,
    PrimitiveFieldCore,
    CDFExternalField,
    EdgeField,
    EdgeOneToMany,
    EdgeOneToOne,
    PrimitiveListField,
    EdgeOneToManyEdges,
    EdgeToOneDataClass,
    EdgeToMultipleDataClasses,
    T_Field,
)
from .filter_method import FilterMethod, FilterParameter


@dataclass
class DataClass:
    """
    This represents a data class. It is created from a view.
    """

    # view_name: str
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
    # _list_method: FilterMethod | None = None

    # @property
    # def list_method(self) -> FilterMethod:
    #     if self._list_method is None:
    #         raise ValueError("DataClass has not been initialized.")
    #     return self._list_method

    @classmethod
    def from_view(cls, view: dm.View, data_class: pygen_config.DataClassNaming) -> DataClass:
        view_name = (view.name or view.external_id).replace(" ", "_")
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
        query_file_name = f"{raw_file_name}_query"
        query_class_name = f"{class_name.replace('_', '')}QueryAPI"

        used_for = view.used_for
        if used_for == "all":
            used_for = "node"
            warnings.warn("View used_for is set to 'all'. This is not supported. Using 'node' instead.", stacklevel=2)

        args = dict(
            view_name=view_name,
            read_name=class_name,
            write_name=f"{class_name}Apply",
            read_list_name=f"{class_name}List",
            write_list_name=f"{class_name}ApplyList",
            doc_name=doc_name,
            doc_list_name=doc_list_name,
            variable=variable_name,
            variable_list=variable_list,
            file_name=file_name,
            query_file_name=query_file_name,
            query_class_name=query_class_name,
            view_id=view.as_id(),
            view_version=view.version,
        )

        if used_for == "node":
            return NodeDataClass(**args)
        elif used_for == "edge":
            return EdgeWithPropertyDataClass(**args)
        else:
            raise ValueError(f"Unsupported used_for={used_for}")

    def update_fields(
        self,
        properties: dict[str, dm.MappedProperty | dm.ConnectionDefinition],
        data_class_by_view_id: dict[dm.ViewId, DataClass],
        config: pygen_config.PygenConfig,
    ) -> None:
        pydantic_field = self.pydantic_field
        for prop_name, prop in properties.items():
            field_ = Field.from_property(
                prop_name,
                prop,
                data_class_by_view_id,
                config,
                self.view_name,
                dm.ViewId(self.view_id.space, self.view_id.external_id, self.view_version),
                pydantic_field=pydantic_field,
            )
            self.fields.append(field_)
        self._list_method = FilterMethod.from_fields(self.fields, config.filtering, self.is_edge_class)

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
    def pydantic_field(self) -> str:
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
        self, type_: type[T_Field] | tuple[type[Field], ...]
    ) -> Iterable[T_Field] | Iterable[tuple[type[Field], ...]]:
        return (field_ for field_ in self if isinstance(field_, type_))

    def has_field_of_type(self, type_: type[Field] | tuple[type[Field], ...]) -> bool:
        return any(isinstance(field_, type_) for field_ in self)

    def is_all_fields_of_type(self, type_: type[Field] | tuple[type[Field], ...]) -> bool:
        return all(isinstance(field_, type_) for field_ in self)

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
    #     return [data_class for data_class in self.dependencies if isinstance(data_class, EdgeWithPropertyDataClass)]

    @property
    def has_single_timeseries_fields(self) -> bool:
        return any(
            isinstance(field_.prop.type, dm.TimeSeriesReference) and not isinstance(field_, PrimitiveListField)
            for field_ in self.single_timeseries_fields
        )

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
    #     edges = list(self.one_to_many_edges)
    #     if len(edges) == 1:
    #         return f"`{edges[0].name}`"
    #     else:
    #         return ", ".join(f"`{field_.name}`" for field_ in edges[:-1]) + f" or `{edges[-1].name}`"

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
#     _start_class: NodeDataClass | None = None
#     _end_class: NodeDataClass | None = None
#     _edge_type: dm.DirectRelationReference | None = None
#
#     @property
#     def is_edge_class(self) -> bool:
#         return True
#
#     @property
#     def start_class(self) -> NodeDataClass:
#         if self._start_class is None:
#             raise ValueError("EdgeDataClass has not been initialized.")
#         return self._start_class
#
#     @property
#     def end_class(self) -> NodeDataClass:
#         if self._end_class is None:
#             raise ValueError("EdgeDataClass has not been initialized.")
#         return self._end_class
#
#     @property
#     def edge_type(self) -> dm.DirectRelationReference:
#         if self._edge_type is None:
#             raise ValueError("EdgeDataClass has not been initialized.")
#         return self._edge_type
#
#     def update_nodes(
#         self,
#         data_class_by_view_id: dict[dm.ViewId, DataClass],
#         views: Iterable[dm.View],
#         field_naming: pygen_config.FieldNaming,
#     ):
#         source_view, source_property, prop_name = self._find_source_view_and_property(views)
#         self._start_class = cast(NodeDataClass, data_class_by_view_id[source_view.as_id()])
#         self._end_class = cast(NodeDataClass, data_class_by_view_id[source_property.source])
#         self._edge_type = source_property.type
#
#         end_class = self._end_class
#         # Todo avoid repeating the code below here and the load method
#         name = create_name(end_class.view_name, field_naming.name)
#         view_id = dm.ViewId(end_class.view_id.space, end_class.view_id.external_id, end_class.view_version)
#         if is_reserved_word(name, "field", view_id, end_class.view_name):
#             name = f"{name}_"
#
#         doc_name = to_words(name, singularize=True)
#
#         variable = create_name(end_class.view_name, field_naming.variable)
#
#         edge_api_class_input = f"{self.view_name}_{end_class.view_name}"
#         edge_api_class = f"{create_name(edge_api_class_input, field_naming.edge_api_class)}API"
#         edge_api_attribute = f"{create_name(end_class.view_name, field_naming.api_class_attribute)}_edge"
#
#         self.fields.append(
#             EdgeOneToEndNode(
#                 name=name,
#                 doc_name=doc_name,
#                 prop_name=name,
#                 pydantic_field=self.pydantic_field,
#                 data_class=self.end_class,
#                 prop=source_property,
#                 variable=variable,
#                 edge_api_class=edge_api_class,
#                 edge_api_attribute=edge_api_attribute,
#             )
#         )
#
#     def _find_source_view_and_property(
#         self, views: Iterable[dm.View]
#     ) -> tuple[dm.View, dm.SingleHopConnectionDefinition, str]:
#         for view in views:
#             for prop_name, prop in view.properties.items():
#                 if (
#                     isinstance(prop, dm.SingleHopConnectionDefinition)
#                     and prop.edge_source
#                     and prop.edge_source.space == self.view_id.space
#                     and prop.edge_source.external_id == self.view_id.external_id
#                 ):
#                     return view, prop, prop_name
#         raise ValueError("Could not find source view and property")
#
#     @property
#     def import_pydantic_field(self) -> str:
#         if self.pydantic_field == "Field":
#             return "from pydantic import Field"
#         else:
#             return "import pydantic"
#
#     def node_parameters(self, instance_space: str) -> Iterable[FilterParameter]:
#         nodes = [
#             (self.start_class, "source", self.start_class.variable),
#             (self.end_class, "target", self.end_class.variable),
#         ]
#         if self.start_class.variable == self.end_class.variable:
#             nodes = [
#                 (self.start_class, "source", f"from_{self.start_class.variable}"),
#                 (self.end_class, "target", f"to_{self.end_class.variable}"),
#             ]
#
#         for class_, location, name in nodes:
#             yield FilterParameter(
#                 name=name,
#                 type_="str | list[str] | dm.NodeId | list[dm.NodeId]",
#                 description=f"ID of the {location} { class_.doc_list_name}.",
#                 default=None,
#             )
#             yield FilterParameter(
#                 name=f"{name}_space",
#                 type_="str",
#                 description=f"Location of the {class_.doc_list_name}.",
#                 default=f'"{instance_space}"',
#                 is_nullable=False,
#             )
