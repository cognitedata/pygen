"""This represents the generated data classes in the SDK"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from functools import total_ordering
from typing import Literal, overload

from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.data_modeling.views import ViewProperty

from cognite.pygen import config as pygen_config
from cognite.pygen.config.reserved_words import is_reserved_word
from cognite.pygen.utils.cdf import _find_first_node_type
from cognite.pygen.utils.text import create_name, to_pascal, to_words

from .fields import (
    BaseConnectionField,
    BasePrimitiveField,
    CDFExternalField,
    EdgeClass,
    EndNodeField,
    Field,
    OneToManyConnectionField,
    OneToOneConnectionField,
    PrimitiveField,
    T_Field,
)


@total_ordering
@dataclass
class DataClass:
    """This represents a data class. It is created from a view."""

    read_name: str
    write_name: str
    graphql_name: str
    read_list_name: str
    write_list_name: str
    doc_name: str
    doc_list_name: str
    description: str | None
    variable: str
    variable_list: str
    file_name: str
    view_id: dm.ViewId
    fields: list[Field]
    implements: list[DataClass]
    is_writable: bool
    is_interface: bool
    direct_children: list[DataClass]

    initialization: set[Literal["fields", "parents", "children"]]

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
        """Creates Python compatible base name from a view."""
        return view.external_id.replace(" ", "_")

    @classmethod
    def to_base_name_with_version(cls, view: dm.View) -> str:
        """Creates Python compatible base name from a view with version."""
        return f"{cls.to_base_name(view)}v{to_pascal(view.version)}".replace(" ", "_")

    @classmethod
    def to_base_name_with_space(cls, view: dm.View) -> str:
        """Creates Python compatible base name from a view with space."""
        return f"{cls.to_base_name(view)}s{to_pascal(view.space)}".replace(" ", "_")

    @classmethod
    def to_base_name_with_space_and_version(cls, view: dm.View) -> str:
        """Creates Python compatible base name from a view with space and version."""
        return f"{cls.to_base_name(view)}v{to_pascal(view.version)}s{to_pascal(view.space)}".replace(" ", "_")

    @classmethod
    def from_view(
        cls, view: dm.View, base_name: str, used_for: Literal["node", "edge"], data_class: pygen_config.DataClassNaming
    ) -> DataClass:
        """Create a DataClass from a view."""
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

        if view.used_for != used_for and view.used_for != "all":
            raise ValueError(f"View {view.as_id()} cannot be used for {used_for}")

        args = dict(
            read_name=class_name,
            write_name=f"{class_name}Write",
            graphql_name=f"{class_name}GraphQL",
            read_list_name=f"{class_name}List",
            write_list_name=f"{class_name}WriteList",
            doc_name=doc_name,
            doc_list_name=doc_list_name,
            description=view.description,
            variable=variable_name,
            variable_list=variable_list,
            file_name=file_name,
            view_id=view.as_id(),
            fields=[],
            implements=[],
            is_writable=view.writable,
            is_interface=False,
            direct_children=[],
            initialization=set(),
        )

        if used_for == "node":
            node_type = _find_first_node_type(view.filter)

            return NodeDataClass(**args, node_type=node_type, has_edge_class=view.used_for == "all")
        elif used_for == "edge":
            return EdgeDataClass(**args, has_node_class=view.used_for == "all")
        else:
            raise ValueError(f"Unsupported used_for={used_for}")

    def update_fields(
        self,
        properties: dict[str, ViewProperty],
        node_class_by_view_id: dict[dm.ViewId, NodeDataClass],
        edge_class_by_view_id: dict[dm.ViewId, EdgeDataClass],
        views: list[dm.View],
        has_default_instance_space: bool,
        direct_relations_by_view_id: dict[dm.ViewId, set[str]],
        config: pygen_config.PygenConfig,
    ) -> None:
        """Update the fields of the data class.

        This needs to be called after the data class has been created to update all fields with dependencies
        on other data classes.
        """
        view_by_id = {view.as_id(): view for view in views}
        for prop_name, prop in properties.items():
            field_ = Field.from_property(
                prop_name,
                prop,
                node_class_by_view_id,
                edge_class_by_view_id,
                config,
                self.view_id,
                # This is the default value for pydantic_field, it will be updated later
                pydantic_field=self.pydantic_field,
                has_default_instance_space=has_default_instance_space,
                direct_relations_by_view_id=direct_relations_by_view_id,
                view_by_id=view_by_id,
            )
            if field_ is None:
                # Reverse direct relations are skipped
                continue
            self.fields.append(field_)

        if any(dependency.pydantic_field == "pydantic.Field" for dependency in self.dependencies):
            for field_ in self.fields:
                # All fields are frozen, so we need to set the attribute directly
                object.__setattr__(field_, "pydantic_field", "pydantic.Field")
        self.initialization.add("fields")

    def update_implements_interface_and_writable(self, parents: list[DataClass], is_interface: bool):
        """Update the implements, is_interface and is_writable attributes of the data class."""
        self.is_interface = is_interface
        self.implements.extend(parents)
        self.is_writable = self.is_writable or self.is_all_fields_of_type(OneToManyConnectionField)
        self.initialization.add("parents")

    def update_direct_children(self, children: list[DataClass]):
        """Update the direct children of the data class."""
        self.direct_children.extend(children)
        self.initialization.add("children")

    @property
    def read_base_class(self) -> str:
        """Parent read classes."""
        if self.implements:
            return ", ".join(f"{interface.read_name}" for interface in self.implements)
        else:
            return "DomainModel"

    @property
    def write_base_class(self) -> str:
        """Parent write classes."""
        if self.implements:
            return ", ".join(f"{interface.write_name}" for interface in self.implements)
        else:
            return "DomainModelWrite"

    @property
    def query_cls_name(self) -> str:
        """The name of the class used to create queries for this data class."""
        return f"{self.read_name}Query"

    @property
    def view_id_str(self) -> str:
        """The view id as a string."""
        return f'dm.ViewId("{self.view_id.space}", "{self.view_id.external_id}", "{self.view_id.version}")'

    @property
    def has_filtering_fields(self) -> bool:
        """Check if the data class has any fields that support filtering."""
        return any(field_.support_filtering for field_ in self.fields_of_type(PrimitiveField))

    @property
    def filtering_fields(self) -> Iterable[PrimitiveField]:
        """Return all fields that support filtering"""
        return (field_ for field_ in self.fields_of_type(PrimitiveField) if field_.support_filtering)

    @property
    def filtering_import(self) -> str:
        """Import the filtering classes used in the data class."""
        return "\n    ".join(
            # The string filter is always included, and is thus part of the jinja template
            f"{cls_name},"
            for cls_name in sorted(set(field_.filtering_cls for field_ in self.filtering_fields))
            if cls_name != "StringFilter"
        )

    @property
    def typed_read_bases_classes(self) -> str:
        """The parent read classes for the typed data class."""
        if self.implements:
            return ", ".join(f"{interface.read_name}" for interface in self.implements)
        else:
            return "TypedEdge" if isinstance(self, EdgeDataClass) else "TypedNode"

    @property
    def typed_write_bases_classes(self) -> str:
        """The parent write classes for the typed data class."""
        if self.implements:
            return ", ".join(f"{interface.read_name}Apply" for interface in self.implements)
        else:
            return "TypedEdgeApply" if isinstance(self, EdgeDataClass) else "TypedNodeApply"

    @property
    def text_field_names(self) -> str:
        """The name of the text fields Literal."""
        return f"{self.read_name}TextFields"

    @property
    def field_names(self) -> str:
        """The name of the fields Literal."""
        return f"{self.read_name}Fields"

    @property
    def properties_dict_name(self) -> str:
        """The name of the properties dictionary."""
        return f"_{self.read_name.upper()}_PROPERTIES_BY_FIELD"

    @property
    def pydantic_field(self) -> Literal["Field", "pydantic.Field"]:
        """The name of the pydantic field to use.
        This is in case we need to use pydantic.Field from pydantic instead of Field.
        """
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
        """The data class __init__ imports of this data class"""
        import_classes = [self.read_name, self.graphql_name]
        if self.is_writable or self.is_interface:
            import_classes.append(self.write_name)
            import_classes.append(f"{self.read_name}Apply")
        import_classes.append(self.read_list_name)
        if self.is_writable or self.is_interface:
            import_classes.append(self.write_list_name)
            import_classes.append(f"{self.read_name}ApplyList")
        import_classes.append(self.field_names)
        import_classes.append(self.text_field_names)
        return f"from .{self.file_name} import {', '.join(sorted(import_classes))}"

    def __iter__(self) -> Iterator[Field]:
        return iter(self.fields)

    def non_parent_fields(self, fields: Iterator[Field] | None = None) -> Iterator[Field]:
        """Return all fields that are not inherited from a parent."""
        parent_fields = {field.prop_name for parent in self.implements for field in parent}
        return (field for field in fields or self if field.prop_name not in parent_fields)

    @property
    def read_fields(self) -> Iterator[Field]:
        """These fields are used when creating the read data class."""
        return self.non_parent_fields()

    @property
    def write_fields(self) -> Iterator[Field]:
        """These fields are used when creating the write data class."""
        return (field for field in self.non_parent_fields() if field.is_write_field)

    @property
    def write_connection_fields(self) -> Iterator[BaseConnectionField]:
        """These fields are used when creating the write data class."""
        return (
            field
            for field in self.write_fields
            if isinstance(field, BaseConnectionField) and not field.is_direct_relation_no_source
        )

    @property
    def has_write_connection_fields(self) -> bool:
        """Check if the data class has any write connection fields."""
        return any(self.write_connection_fields)

    @overload
    def fields_of_type(self, field_type: type[T_Field]) -> Iterator[T_Field]: ...

    @overload
    def fields_of_type(self, field_type: tuple[type[Field], ...]) -> Iterator[tuple[Field]]: ...

    def fields_of_type(
        self, field_type: type[T_Field] | tuple[type[Field], ...]
    ) -> Iterator[T_Field] | Iterator[tuple[Field]]:
        """Return all fields of a specific type."""
        return (field_ for field_ in self if isinstance(field_, field_type))  # type: ignore[return-value]

    def has_field_of_type(self, type_: type[Field] | tuple[type[Field], ...]) -> bool:
        """Check if the data class has any fields of a specific type."""
        return any(isinstance(field_, type_) for field_ in self)

    def is_all_fields_of_type(self, type_: type[Field] | tuple[type[Field], ...]) -> bool:
        """Check if all fields are of a specific type."""
        return all(isinstance(field_, type_) for field_ in self)

    def primitive_fields_of_type(
        self, type_: type[dm.PropertyType] | tuple[type[dm.PropertyType], ...]
    ) -> Iterable[BasePrimitiveField]:
        """Return all primitive fields of a specific type."""
        return (
            field_
            for field_ in self.fields_of_type(BasePrimitiveField)  # type: ignore[type-abstract]
            if isinstance(field_.type_, type_)
        )

    def has_primitive_field_of_type(self, type_: type[dm.PropertyType] | tuple[type[dm.PropertyType], ...]) -> bool:
        """Check if the data class has any fields of a specific type."""
        return any(self.primitive_fields_of_type(type_))

    def is_all_primitive_fields_of_type(self, type_: type[dm.PropertyType] | tuple[type[dm.PropertyType], ...]) -> bool:
        """Check if all fields are of a specific type."""
        return all(self.primitive_fields_of_type(type_))

    def has_timeseries_fields(self) -> bool:
        """Check if the data class has any time series fields."""
        return any(isinstance(field_, CDFExternalField) and field_.is_time_series for field_ in self)

    def timeseries_fields(self) -> Iterable[CDFExternalField]:
        """Return all time series fields."""
        return (field_ for field_ in self if isinstance(field_, CDFExternalField) and field_.is_time_series)

    @property
    def _field_type_hints(self) -> Iterable[str]:
        return (hint for field_ in self.fields for hint in (field_.as_read_type_hint(), field_.as_write_type_hint()))

    @property
    def use_pydantic_field(self) -> bool:
        pydantic_field = self.pydantic_field
        return any(pydantic_field in hint for hint in self._field_type_hints)

    @property
    def dependencies(self) -> list[DataClass]:
        """Return a list of all data class dependencies (through fields)."""
        unique: dict[dm.ViewId, DataClass] = {}
        for field_ in self.fields:
            if isinstance(field_, BaseConnectionField):
                if field_.edge_class:
                    unique[field_.edge_class.view_id] = field_.edge_class
                elif field_.destination_class:
                    # This will overwrite any existing data class with the same view id
                    # however, this is not a problem as all data classes are uniquely identified by their view id
                    unique[field_.destination_class.view_id] = field_.destination_class
            elif isinstance(field_, EndNodeField):
                for class_ in field_.destination_classes:
                    unique[class_.view_id] = class_

        return sorted(unique.values(), key=lambda x: x.write_name)

    @property
    def dependencies_with_edge_destinations(self) -> list[DataClass]:
        """Return a list of all dependencies which also includes the edge
        destination if the dependency is a EdgeClass."""
        unique: dict[dm.ViewId, DataClass] = {}
        for field_ in self.fields:
            if isinstance(field_, BaseConnectionField):
                if field_.destination_class:
                    # This will overwrite any existing data class with the same view id
                    # however, this is not a problem as all data classes are uniquely identified by their view id
                    unique[field_.destination_class.view_id] = field_.destination_class
                if field_.edge_class:
                    unique[field_.edge_class.view_id] = field_.edge_class
                    for edge_class in field_.edge_class.end_node_field.edge_classes:
                        if field_.edge_direction == "outwards":
                            unique[edge_class.end_class.view_id] = edge_class.end_class
                        else:
                            unique[edge_class.start_class.view_id] = edge_class.start_class
            elif isinstance(field_, EndNodeField):
                for class_ in field_.destination_classes:
                    unique[class_.view_id] = class_

        return sorted(unique.values(), key=lambda x: x.read_name)

    @property
    def has_dependencies(self) -> bool:
        """Check if the data class has any dependencies."""
        return bool(self.dependencies)

    @property
    def has_dependencies_not_self(self) -> bool:
        """Check if the data class has any dependencies that are not itself."""
        return any(dependency != self for dependency in self.dependencies)

    @property
    def has_edges_or_direct_relations(self) -> bool:
        """Whether the data class has any fields that are edges or direct relations."""
        return any(
            isinstance(field_, BaseConnectionField) and (field_.is_edge or field_.is_direct_relation) for field_ in self
        )

    @property
    def has_reverse_direct_relations(self) -> bool:
        """Whether the data class has any fields that are reverse direct relations."""
        return any(isinstance(field_, BaseConnectionField) and field_.is_reverse_direct_relation for field_ in self)

    @property
    def container_fields(self) -> Iterable[Field]:
        """Container fields are fields that store their value in a container.

        That is all primitive fields (including CDFExternalField) and direct relations.
        """
        return (
            field_
            for field_ in self
            if isinstance(field_, BasePrimitiveField)
            or (isinstance(field_, BaseConnectionField) and field_.is_direct_relation)
        )

    def container_fields_sorted(self, include: Literal["all", "only-self"] | DataClass = "all") -> list[Field]:
        """Return all container fields sorted by type."""

        def key(x: Field) -> int:
            return {True: 1, False: 0}[x.is_nullable] if isinstance(x, BasePrimitiveField) else 1

        if include == "all":
            return sorted(self.container_fields, key=key)
        elif include == "only-self":
            parent_fields = {field.name for parent in self.implements for field in parent.container_fields}
            return sorted([f for f in self.container_fields if f.name not in parent_fields], key=key)
        elif isinstance(include, DataClass):
            fields_by_parent = {parent.read_name: set(parent.container_fields_sorted()) for parent in self.implements}
            if include.read_name not in fields_by_parent:
                raise ValueError(f"Data class {include.read_name} is not a parent of {self.read_name}")
            return sorted(fields_by_parent[include.read_name], key=key)
        else:
            raise TypeError(f"Invalid value for include: {include}")

    @property
    def has_container_fields(self) -> bool:
        """Check if the data class has any container fields."""
        return any(self.container_fields)

    @property
    def one_to_many_edges_without_properties(self) -> Iterable[OneToManyConnectionField]:
        """All MultiEdges without properties on the edge."""
        return (field_ for field_ in self.fields_of_type(OneToManyConnectionField) if field_.is_edge_without_properties)

    @property
    def one_to_one_edge_without_properties(self) -> Iterable[OneToOneConnectionField]:
        """All MultiEdges without properties on the edge."""
        return (field_ for field_ in self.fields_of_type(OneToOneConnectionField) if field_.is_edge_without_properties)

    @property
    def one_to_many_edges_with_properties(self) -> Iterable[OneToManyConnectionField]:
        """All MultiEdges with properties."""
        return (field_ for field_ in self.fields_of_type(OneToManyConnectionField) if field_.is_edge_with_properties)

    @property
    def one_to_one_edges_with_properties(self) -> Iterable[OneToOneConnectionField]:
        """All SingleEdges with properties."""
        return (field_ for field_ in self.fields_of_type(OneToOneConnectionField) if field_.is_edge_with_properties)

    @property
    def one_to_one_direct_relations_with_source(self) -> Iterable[OneToOneConnectionField]:
        """All direct relations."""
        return (
            field_
            for field_ in self.fields_of_type(OneToOneConnectionField)
            if field_.is_direct_relation and field_.destination_class
        )

    @property
    def one_to_one_reverse_direct_relation(self) -> Iterable[OneToOneConnectionField]:
        """All one to one reverse direct relations."""
        return (
            field_
            for field_ in self.fields_of_type(OneToOneConnectionField)
            if field_.is_reverse_direct_relation and field_.destination_class
        )

    @property
    def one_to_many_direct_relations_with_source(self) -> Iterable[OneToManyConnectionField]:
        """All direct relations."""
        return (
            field_
            for field_ in self.fields_of_type(OneToManyConnectionField)
            if field_.is_direct_relation and field_.destination_class
        )

    @property
    def one_to_many_reverse_direct_relations(self) -> Iterable[OneToManyConnectionField]:
        """All one to many reverse direct relations."""
        return (
            field_
            for field_ in self.fields_of_type(OneToManyConnectionField)
            if field_.is_reverse_direct_relation and field_.destination_class
        )

    @property
    def has_one_to_one_direct_relations_with_source(self) -> bool:
        """Check if the data class has any one to one direct relations."""
        return any(self.one_to_one_direct_relations_with_source)

    @property
    def primitive_fields_literal(self) -> str:
        """Return a literal with all primitive fields."""
        return ", ".join(
            f'"{field_.prop_name}"' for field_ in self if isinstance(field_, PrimitiveField | CDFExternalField)
        )

    @property
    def text_fields_literals(self) -> str:
        """Return a literal with all text fields."""
        return ", ".join(
            f'"{field_.name}"' for field_ in self.primitive_fields_of_type((dm.Text, dm.CDFExternalIdReference))
        )

    @property
    def fields_literals(self) -> str:
        """Return a literal with all fields."""
        return ", ".join(f'"{field_.name}"' for field_ in self if isinstance(field_, BasePrimitiveField))

    @property
    def container_field_variables(self) -> str:
        """Return a string with all container fields as variables."""
        return ", ".join(
            f"{field_.name}={field_.name}"
            for field_ in self
            if isinstance(field_, BasePrimitiveField)
            or (isinstance(field_, BaseConnectionField) and field_.is_direct_relation)
        )

    @property
    def filter_name(self) -> str:
        """The name of the filter class."""
        return f"_create_{self.variable}_filter"

    @property
    def is_edge_class(self) -> bool:
        """Check if the data class is an edge class."""
        return False

    @property
    def has_writable_connection_fields(self) -> bool:
        return any(
            isinstance(field_, BaseConnectionField) and field_.destination_class and field_.is_write_field
            for field_ in self
        )

    @property
    def connections_docs_write(self) -> str:
        """Return a string with all connections that are write fields."""
        connections = [f for f in self.fields_of_type(BaseConnectionField) if f.destination_class and f.is_write_field]  # type: ignore[type-abstract]
        if len(connections) == 0:
            raise ValueError("No connections found")
        elif len(connections) == 1:
            return f"`{connections[0].name}`"
        else:
            return ", ".join(f"`{field_.name}`" for field_ in connections[:-1]) + f" or `{connections[-1].name}`"

    @property
    def connections_docs(self) -> str:
        """Return a string with all connections."""
        connections = [f for f in self.fields_of_type(BaseConnectionField) if f.destination_class]  # type: ignore[type-abstract]
        if len(connections) == 0:
            raise ValueError("No connections found")
        elif len(connections) == 1:
            return f"`{connections[0].name}`"
        else:
            return ", ".join(f"`{field_.name}`" for field_ in connections[:-1]) + f" and `{connections[-1].name}`"

    @property
    def import_pydantic_field(self) -> str:
        """Import the pydantic field used in the data class."""
        if self.pydantic_field == "Field":
            return "from pydantic import Field"
        else:
            return "import pydantic"

    @property
    def has_any_field_model_prefix(self) -> bool:
        """Check if any field has a model prefix."""
        return any(field_.name.startswith("model") for field_ in self)

    @property
    def has_edges(self) -> bool:
        """Check if the data class has any edges."""
        return any(isinstance(field_, BaseConnectionField) and field_.is_edge for field_ in self)

    @property
    def direct_children_literal(self) -> str:
        """Returns a Literal class with all direct children external IDs."""
        args = ", ".join(f'"{child.view_id.external_id}"' for child in self.direct_children)
        return f"Literal[{args}]"


@dataclass
class NodeDataClass(DataClass):
    """This represent data class used for views marked as used_for='node'."""

    node_type: dm.DirectRelationReference | None
    has_edge_class: bool

    @property
    def typed_properties_name(self) -> str:
        """The name of the typed properties class."""
        if self.has_edge_class:
            return f"_{self.read_name.removesuffix('Node')}Properties"
        else:
            return f"_{self.read_name}Properties"


@dataclass
class EdgeDataClass(DataClass):
    """This represent data class used for views marked as used_for='edge'."""

    has_node_class: bool
    _end_node_field: EndNodeField | None = None

    @property
    def typed_properties_name(self) -> str:
        """The name of the typed properties class."""
        if self.has_node_class:
            return f"_{self.read_name.removesuffix('Edge')}Properties"
        else:
            return f"_{self.read_name}Properties"

    @property
    def is_edge_class(self) -> bool:
        """Check if the data class is an edge class."""
        return True

    @property
    def end_node_field(self) -> EndNodeField:
        """The end node field of the edge class."""
        if self._end_node_field:
            return self._end_node_field
        raise ValueError("EdgeDataClass has not been initialized.")

    def update_fields(
        self,
        properties: dict[str, ViewProperty],
        node_class_by_view_id: dict[dm.ViewId, NodeDataClass],
        edge_class_by_view_id: dict[dm.ViewId, EdgeDataClass],
        views: list[dm.View],
        has_default_instance_space: bool,
        direct_relations_by_view_id: dict[dm.ViewId, set[str]],
        config: pygen_config.PygenConfig,
    ):
        """Update the fields of the data class."""
        # Find all node views that have an edge with properties in this view
        # and get the node class it is pointing to.
        edge_classes: dict[tuple[str, dm.DirectRelationReference, str], EdgeClass] = {}
        for view in views:
            view_id = view.as_id()
            if view_id not in node_class_by_view_id:
                continue
            source_class = node_class_by_view_id[view_id]
            for prop in view.properties.values():
                if isinstance(prop, dm.EdgeConnection) and prop.edge_source == self.view_id:
                    destination_class = node_class_by_view_id[prop.source]
                    start, end = (
                        (source_class, destination_class)
                        if prop.direction == "outwards"
                        else (destination_class, source_class)
                    )
                    identifier = start.read_name, prop.type, end.read_name
                    if edge_class := edge_classes.get(identifier):
                        edge_class.used_directions.add(prop.direction)
                    else:
                        edge_classes[identifier] = EdgeClass(start, prop.type, end, {prop.direction})

        self._end_node_field = EndNodeField(
            name="end_node",
            doc_name="end node",
            prop_name="end_node",
            description="The end node of this edge.",
            pydantic_field="Field",
            edge_classes=list(edge_classes.values()),
        )
        self.fields.append(self._end_node_field)
        super().update_fields(
            properties,
            node_class_by_view_id,
            edge_class_by_view_id,
            views,
            has_default_instance_space,
            direct_relations_by_view_id,
            config,
        )
