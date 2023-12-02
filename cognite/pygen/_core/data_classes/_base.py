from __future__ import annotations

import warnings
from abc import abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterator, Iterable

from cognite.client.data_classes import data_modeling as dm

from cognite.pygen import config as pygen_config
from cognite.pygen._core.data_classes import NodeDataClass, EdgeWithPropertyDataClass
from cognite.pygen.config.reserved_words import is_reserved_word
from cognite.pygen.utils.text import create_name, to_words


@dataclass
class DataClass:
    """
    This represents a data class. It is created from a view.
    """

    view_name: str
    read_name: str
    write_name: str
    read_list_name: str
    write_list_name: str
    doc_name: str
    doc_list_name: str
    variable: str
    variable_list: str
    file_name: str
    query_file_name: str
    query_class_name: str
    view_id: ViewSpaceExternalId
    view_version: str
    fields: list[Field] = field(default_factory=list)
    _list_method: FilterMethod | None = None

    @property
    def list_method(self) -> FilterMethod:
        if self._list_method is None:
            raise ValueError("DataClass has not been initialized.")
        return self._list_method

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
            view_id=ViewSpaceExternalId.from_(view),
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
        data_class_by_view_id: dict[ViewSpaceExternalId, DataClass],
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

    @property
    def one_to_one_edges(self) -> Iterable[EdgeOneToOne]:
        return (field_ for field_ in self.fields if isinstance(field_, EdgeOneToOne))

    @property
    def one_to_many_edges(self) -> Iterable[EdgeOneToMany]:
        return (field_ for field_ in self.fields if isinstance(field_, EdgeOneToMany))

    @property
    def primitive_fields(self) -> Iterable[PrimitiveField]:
        return (field_ for field_ in self.fields if isinstance(field_, PrimitiveField))

    @property
    def primitive_core_fields(self) -> Iterable[PrimitiveFieldCore]:
        return (field_ for field_ in self.fields if isinstance(field_, PrimitiveFieldCore))

    @property
    def property_fields(self) -> Iterable[PrimitiveFieldCore | EdgeOneToOne]:
        return (field_ for field_ in self if isinstance(field_, (PrimitiveFieldCore, EdgeOneToOne)))

    @property
    def text_fields(self) -> Iterable[PrimitiveFieldCore]:
        return (field_ for field_ in self.primitive_core_fields if field_.is_text_field)

    @property
    def cdf_external_fields(self) -> Iterable[CDFExternalField]:
        return (field_ for field_ in self.fields if isinstance(field_, CDFExternalField))

    @property
    def single_timeseries_fields(self) -> Iterable[CDFExternalField]:
        return (field_ for field_ in self.cdf_external_fields if isinstance(field_.prop.type, dm.TimeSeriesReference))

    @property
    def property_edges(self) -> Iterable[EdgeOneToMany]:
        return (field_ for field_ in self.fields if isinstance(field_, EdgeOneToMany) and field_.is_property_edge)

    @property
    def has_one_to_many_edges(self) -> bool:
        return any(isinstance(field_, EdgeOneToMany) for field_ in self.fields)

    @property
    def has_edges(self) -> bool:
        return any(isinstance(field_, EdgeField) for field_ in self.fields)

    @property
    def has_edge_with_property(self) -> bool:
        return any(isinstance(field_, EdgeOneToMany) and field_.is_property_edge for field_ in self.fields)

    @property
    def all_one_to_many_is_property_edges(self) -> bool:
        return all(field_.is_property_edge for field_ in self.one_to_many_edges)

    @property
    def has_time_field_on_property_edge(self) -> bool:
        return any(field_.is_time_field for edge in self.property_edges for field_ in edge.data_class.fields)

    @property
    def has_primitive_fields(self) -> bool:
        return any(isinstance(field_, PrimitiveFieldCore) for field_ in self.fields)

    @property
    def has_only_one_to_many_edges(self) -> bool:
        return all(isinstance(field_, EdgeOneToMany) for field_ in self.fields)

    @property
    def fields_by_container(self) -> dict[dm.ContainerId, list[PrimitiveFieldCore | EdgeOneToOne]]:
        # Todo This should be deleted
        result: dict[dm.ContainerId, list[PrimitiveFieldCore | EdgeOneToOne]] = defaultdict(list)
        for field_ in self:
            if isinstance(field_, (PrimitiveFieldCore, EdgeOneToOne)):
                result[field_.prop.container].append(field_)
        return dict(result)

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
        unique: dict[ViewSpaceExternalId, DataClass] = {}
        for field_ in self.fields:
            if isinstance(field_, EdgeField):
                # This will overwrite any existing data class with the same view id
                # however, this is not a problem as all data classes are uniquely identified by their view id
                unique[field_.data_class.view_id] = field_.data_class
        return sorted(unique.values(), key=lambda x: x.write_name)

    @property
    def dependencies_edges(self) -> list[EdgeWithPropertyDataClass]:
        return [data_class for data_class in self.dependencies if isinstance(data_class, EdgeWithPropertyDataClass)]

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

    @property
    def one_to_many_edges_docs(self) -> str:
        edges = list(self.one_to_many_edges)
        if len(edges) == 1:
            return f"`{edges[0].name}`"
        else:
            return ", ".join(f"`{field_.name}`" for field_ in edges[:-1]) + f" or `{edges[-1].name}`"

    @property
    def fields_literals(self) -> str:
        return ", ".join(f'"{field_.name}"' for field_ in self if isinstance(field_, PrimitiveFieldCore))

    @property
    def filter_name(self) -> str:
        return f"_create_{self.variable}_filter"

    @property
    @abstractmethod
    def is_edge_class(self) -> bool:
        raise NotImplementedError()
