"""This represents the generated data classes in the SDK"""

from __future__ import annotations

import warnings
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from functools import total_ordering
from typing import Literal, cast, overload

from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.data_modeling.views import EdgeConnection, ViewProperty

from cognite.pygen import config as pygen_config
from cognite.pygen.config.reserved_words import is_reserved_word
from cognite.pygen.utils.cdf import _find_first_node_type
from cognite.pygen.utils.text import create_name, to_pascal, to_words

from .fields import (
    CDFExternalField,
    EdgeClasses,
    EdgeOneToEndNode,
    EdgeOneToMany,
    EdgeToOneDataClass,
    Field,
    PrimitiveField,
    PrimitiveFieldCore,
    T_Field,
)


@total_ordering
@dataclass
class DataClass:
    """
    This represents a data class. It is created from a view.
    """

    read_name: str
    write_name: str
    graphql_name: str
    read_list_name: str
    write_list_name: str
    doc_name: str
    doc_list_name: str
    variable: str
    variable_list: str
    file_name: str
    view_id: dm.ViewId
    fields: list[Field]
    implements: list[DataClass]
    is_writable: bool
    is_interface: bool

    def __lt__(self, other: DataClass):
        if isinstance(other, DataClass):
            return self.read_name < other.read_name
        else:
            return NotImplemented

    def __eq__(self, other: object):
        if isinstance(other, DataClass):
            return self.read_name, self.view_id == other.read_name, other.view_id
        else:
            return NotImplemented

    @staticmethod
    def to_base_name(view: dm.View) -> str:
        return view.external_id.replace(" ", "_")

    @classmethod
    def to_base_name_with_version(cls, view: dm.View) -> str:
        return f"{cls.to_base_name(view)}v{to_pascal(view.version)}".replace(" ", "_")

    @classmethod
    def to_base_name_with_space(cls, view: dm.View) -> str:
        return f"{cls.to_base_name(view)}s{to_pascal(view.space)}".replace(" ", "_")

    @classmethod
    def to_base_name_with_space_and_version(cls, view: dm.View) -> str:
        return f"{cls.to_base_name(view)}v{to_pascal(view.version)}s{to_pascal(view.space)}".replace(" ", "_")

    @classmethod
    def from_view(cls, view: dm.View, base_name: str, data_class: pygen_config.DataClassNaming) -> DataClass:
        class_name = create_name(base_name, data_class.name)
        if is_reserved_word(class_name, "data class", view.as_id()):
            class_name = f"{class_name}_"

        variable_name = create_name(base_name, data_class.variable)
        variable_list = create_name(base_name, data_class.variable_list)
        doc_name = to_words(base_name, singularize=True)
        doc_list_name = to_words(base_name, pluralize=True)
        if variable_name == variable_list:
            variable_list = f"{variable_list}_list"
        raw_file_name = create_name(base_name, data_class.file)
        file_name = f"_{raw_file_name}"
        if is_reserved_word(file_name, "filename", view.as_id()):
            file_name = f"{file_name}_"

        used_for = view.used_for
        if used_for == "all":
            used_for = "node"
            warnings.warn("View used_for is set to 'all'. This is not supported. Using 'node' instead.", stacklevel=2)

        args = dict(
            read_name=class_name,
            write_name=f"{class_name}Write",
            graphql_name=f"{class_name}GraphQL",
            read_list_name=f"{class_name}List",
            write_list_name=f"{class_name}WriteList",
            doc_name=doc_name,
            doc_list_name=doc_list_name,
            variable=variable_name,
            variable_list=variable_list,
            file_name=file_name,
            view_id=view.as_id(),
            fields=[],
            implements=[],
            is_writable=view.writable,
            is_interface=False,
        )

        if used_for == "node":
            node_type = _find_first_node_type(view.filter)

            return NodeDataClass(**args, node_type=node_type)
        elif used_for == "edge":
            return EdgeDataClass(**args)
        else:
            raise ValueError(f"Unsupported used_for={used_for}")

    def update_fields(
        self,
        properties: dict[str, ViewProperty],
        data_class_by_view_id: dict[dm.ViewId, DataClass],
        views: list[dm.View],
        config: pygen_config.PygenConfig,
    ) -> None:
        dependencies = [
            data_class_by_view_id[prop.edge_source or prop.source]
            for prop in properties.values()
            if isinstance(prop, EdgeConnection)
        ]
        pydantic_field: Literal["Field", "pydantic.Field"] = "Field"
        if self.pydantic_field == "pydantic.Field" or any(
            dependency.pydantic_field == "pydantic.Field" for dependency in dependencies
        ):
            pydantic_field = "pydantic.Field"

        for prop_name, prop in properties.items():
            field_ = Field.from_property(
                prop_name,
                prop,
                data_class_by_view_id,
                config,
                self.view_id,
                pydantic_field=pydantic_field,
            )
            if field_ is None:
                # Reverse direct relations are skipped
                continue
            self.fields.append(field_)

    def update_implements_interface_and_writable(self, parents: list[DataClass], is_interface: bool):
        self.is_interface = is_interface
        self.implements.extend(parents)
        self.is_writable = self.is_writable or self.is_all_fields_of_type(EdgeOneToMany)

    @property
    def read_base_class(self) -> str:
        if self.implements:
            return ", ".join(f"{interface.read_name}" for interface in self.implements)
        else:
            return "DomainModel"

    @property
    def write_base_class(self) -> str:
        if self.implements:
            return ", ".join(f"{interface.write_name}" for interface in self.implements)
        else:
            return "DomainModelWrite"

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
        ) or any(
            name == "Field"
            for dependency in self.dependencies
            for name in [
                dependency.read_name,
                dependency.write_name,
                dependency.read_list_name,
                dependency.write_list_name,
            ]
        ):
            return "pydantic.Field"
        else:
            return "Field"

    @property
    def init_import(self) -> str:
        import_classes = [self.read_name, self.graphql_name]
        if self.is_writable:
            import_classes.append(self.write_name)
            import_classes.append(f"{self.read_name}Apply")
        import_classes.append(self.read_list_name)
        if self.is_writable:
            import_classes.append(self.write_list_name)
            import_classes.append(f"{self.read_name}ApplyList")
        if self.has_field_of_type(PrimitiveFieldCore):
            import_classes.append(self.field_names)
        if self.has_primitive_field_of_type(dm.Text):
            import_classes.append(self.text_field_names)
        return f"from .{self.file_name} import {', '.join(sorted(import_classes))}"

    def __iter__(self) -> Iterator[Field]:
        return iter(self.fields)

    @property
    def non_parent_fields(self) -> Iterator[Field]:
        parent_fields = {field.prop_name for parent in self.implements for field in parent}
        return (field for field in self if field.prop_name not in parent_fields)

    @overload
    def fields_of_type(self, field_type: type[T_Field]) -> Iterator[T_Field]: ...

    @overload
    def fields_of_type(self, field_type: tuple[type[Field], ...]) -> Iterator[tuple[Field]]: ...

    def fields_of_type(
        self, field_type: type[T_Field] | tuple[type[Field], ...]
    ) -> Iterator[T_Field] | Iterator[tuple[Field]]:
        return (field_ for field_ in self if isinstance(field_, field_type))  # type: ignore[return-value]

    def has_field_of_type(self, type_: type[Field] | tuple[type[Field], ...]) -> bool:
        return any(isinstance(field_, type_) for field_ in self)

    def is_all_fields_of_type(self, type_: type[Field] | tuple[type[Field], ...]) -> bool:
        return all(isinstance(field_, type_) for field_ in self)

    def primitive_fields_of_type(
        self, type_: type[dm.PropertyType] | tuple[type[dm.PropertyType], ...]
    ) -> Iterable[PrimitiveFieldCore]:
        return (
            field_
            for field_ in self.fields_of_type(PrimitiveFieldCore)  # type: ignore[type-abstract]
            if isinstance(field_.type_, type_)
        )

    def has_primitive_field_of_type(self, type_: type[dm.PropertyType] | tuple[type[dm.PropertyType], ...]) -> bool:
        return any(self.primitive_fields_of_type(type_))

    def is_all_primitive_fields_of_type(self, type_: type[dm.PropertyType] | tuple[type[dm.PropertyType], ...]) -> bool:
        return all(self.primitive_fields_of_type(type_))

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
            if isinstance(field_, EdgeToOneDataClass):
                # This will overwrite any existing data class with the same view id
                # however, this is not a problem as all data classes are uniquely identified by their view id
                unique[field_.data_class.view_id] = field_.data_class
            elif isinstance(field_, EdgeOneToEndNode):
                for class_ in field_.end_classes:
                    unique[class_.view_id] = class_
        return sorted(unique.values(), key=lambda x: x.write_name)

    @property
    def primitive_fields_literal(self) -> str:
        return ", ".join(
            f'"{field_.prop_name}"' for field_ in self if isinstance(field_, (PrimitiveField, CDFExternalField))
        )

    @property
    def text_fields_literals(self) -> str:
        return ", ".join(
            f'"{field_.name}"' for field_ in self.primitive_fields_of_type((dm.Text, dm.CDFExternalIdReference))
        )

    @property
    def fields_literals(self) -> str:
        return ", ".join(f'"{field_.name}"' for field_ in self if isinstance(field_, PrimitiveFieldCore))

    @property
    def filter_name(self) -> str:
        return f"_create_{self.variable}_filter"

    @property
    def is_edge_class(self) -> bool:
        return False

    @property
    def one_to_many_edges_docs(self) -> str:
        edges = list(self.fields_of_type(EdgeOneToMany))  # type: ignore[type-abstract]
        if len(edges) == 1:
            return f"`{edges[0].name}`"
        else:
            return ", ".join(f"`{field_.name}`" for field_ in edges[:-1]) + f" or `{edges[-1].name}`"

    @property
    def import_pydantic_field(self) -> str:
        if self.pydantic_field == "Field":
            return "from pydantic import Field"
        else:
            return "import pydantic"

    @property
    def has_any_field_model_prefix(self) -> bool:
        return any(field_.name.startswith("model") for field_ in self)


@dataclass
class NodeDataClass(DataClass):
    node_type: dm.DirectRelationReference | None


@dataclass
class EdgeDataClass(DataClass):
    @property
    def is_edge_class(self) -> bool:
        return True

    @property
    def end_node_field(self) -> EdgeOneToEndNode:
        try:
            return next(field_ for field_ in self.fields if isinstance(field_, EdgeOneToEndNode))
        except StopIteration:
            raise ValueError("EdgeDataClass has not been initialized.") from None

    def update_fields(
        self,
        properties: dict[str, ViewProperty],
        data_class_by_view_id: dict[dm.ViewId, DataClass],
        views: list[dm.View],
        config: pygen_config.PygenConfig,
    ):
        edge_classes = []
        for view in views:
            start_class = data_class_by_view_id[view.as_id()]
            for _prop_name, prop in view.properties.items():
                if isinstance(prop, dm.SingleHopConnectionDefinition) and prop.edge_source == self.view_id:
                    edge_classes.append(
                        EdgeClasses(
                            cast(NodeDataClass, start_class),
                            prop.type,
                            cast(NodeDataClass, data_class_by_view_id[prop.source]),
                        )
                    )

        self.fields.append(
            EdgeOneToEndNode(
                name="end_node",
                doc_name="end node",
                prop_name="end_node",
                description="The end node of this edge.",
                pydantic_field="Field",
                edge_classes=edge_classes,
            )
        )
        super().update_fields(properties, data_class_by_view_id, views, config)
