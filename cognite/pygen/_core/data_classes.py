from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from typing import cast

from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.data_modeling.data_types import ListablePropertyType
from typing_extensions import Self

from cognite.pygen.config import PygenConfig
from cognite.pygen.utils.text import create_name

_PRIMITIVE_TYPES = (dm.Text, dm.Boolean, dm.Float32, dm.Float64, dm.Int32, dm.Int64, dm.Timestamp, dm.Date, dm.Json)
_EXTERNAL_TYPES = (dm.TimeSeriesReference, dm.FileReference, dm.SequenceReference)


@dataclass(frozen=True)
class Field(ABC):
    """
    A field represents a pydantic field in the generated pydantic class.

    Args:
        name: The name of the field. This is used in the generated Python code.
        prop_name: The name of the property in the data model. This is used when reading and writing to CDF.
        pydantic_field: The name to use for the import 'from pydantic import Field'. This is used in the edge case
                        when the name 'Field' name clashes with the data model class name.

    """

    name: str
    prop_name: str
    pydantic_field: str

    @property
    def need_alias(self) -> bool:
        return self.name != self.prop_name

    @classmethod
    def from_property(
        cls,
        prop_name: str,
        prop: dm.MappedProperty | dm.ConnectionDefinition,
        data_class_by_view_id: dict[tuple[str, str], DataClass],
        config: PygenConfig,
        view_name: str,
        pydantic_field: str = "Field",
    ) -> Field:
        name = create_name(prop_name, config.naming.field.name)
        if isinstance(prop, dm.SingleHopConnectionDefinition):
            variable = create_name(prop_name, config.naming.field.variable)

            edge_api_class_input = f"{view_name}_{prop_name}"
            edge_api_class = f"{create_name(edge_api_class_input, config.naming.field.edge_api_class)}API"
            edge_api_attribute = create_name(prop_name, config.naming.field.api_class_attribute)
            return EdgeOneToMany(
                name=name,
                prop_name=prop_name,
                prop=prop,
                data_class=data_class_by_view_id[(prop.source.space, prop.source.external_id)],
                variable=variable,
                pydantic_field=pydantic_field,
                edge_api_class=edge_api_class,
                edge_api_attribute=edge_api_attribute,
            )
        if not isinstance(prop, dm.MappedProperty):
            raise ValueError(f"Property type={type(prop)!r} is not supported")

        if isinstance(prop.type, _PRIMITIVE_TYPES) or isinstance(prop.type, _EXTERNAL_TYPES):
            type_ = _to_python_type(prop.type)
            if isinstance(prop.type, ListablePropertyType) and prop.type.is_list:
                return PrimitiveListField(
                    name=name,
                    prop_name=prop_name,
                    type_=type_,
                    is_nullable=prop.nullable,
                    prop=prop,
                    pydantic_field=pydantic_field,
                )

            return PrimitiveField(
                name=name,
                prop_name=prop_name,
                type_=type_,
                is_nullable=prop.nullable,
                default=prop.default_value,
                prop=prop,
                pydantic_field=pydantic_field,
            )
        elif isinstance(prop.type, dm.DirectRelation):
            # For direct relation the source is required.
            view_id = cast(dm.ViewId, prop.source)
            target_data_class = data_class_by_view_id[(view_id.space, view_id.external_id)]
            return EdgeOneToOne(
                name=name,
                prop_name=prop_name,
                prop=prop,
                data_class=target_data_class,
                pydantic_field=pydantic_field,
            )

        else:
            raise NotImplementedError(f"Property type={type(prop)!r} is not supported")

    @abstractmethod
    def as_read_type_hint(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def as_write_type_hint(self) -> str:
        raise NotImplementedError()

    @property
    @abstractmethod
    def is_edge(self) -> bool:
        raise NotImplementedError()

    @property
    @abstractmethod
    def is_time_field(self) -> bool:
        raise NotImplementedError()


@dataclass(frozen=True)
class PrimitiveFieldCore(Field, ABC):
    type_: str
    is_nullable: bool
    prop: dm.MappedProperty

    @property
    def is_edge(self) -> bool:
        return False

    @property
    def is_time_field(self) -> bool:
        return self.type_ in ("datetime.datetime", "datetime.date")


@dataclass(frozen=True)
class PrimitiveField(PrimitiveFieldCore):
    """
    This represents a basic type such as str, int, float, bool, datetime.datetime, datetime.date.
    """

    default: str | int | dict | None = None

    def as_read_type_hint(self) -> str:
        rhs = str(self.default)
        if self.need_alias:
            rhs = f'{self.pydantic_field}({rhs}, alias="{self.prop_name}")'

        return f"Optional[{self.type_}] = {rhs}"

    def as_write_type_hint(self) -> str:
        out_type = self.type_
        if self.is_nullable:
            out_type = f"Optional[{out_type}] = {self.default}"
        return out_type


@dataclass(frozen=True)
class PrimitiveListField(PrimitiveFieldCore):
    """
    This represents a list of basic types such as list[str], list[int], list[float], list[bool],
    list[datetime.datetime], list[datetime.date].
    """

    def as_read_type_hint(self) -> str:
        alias = ""
        if self.need_alias:
            alias = f', alias="{self.prop_name}"'

        if alias:
            rhs = f", {self.pydantic_field}(default_factory=list{alias})"
        else:
            rhs = " = []"
        return f"list[{self.type_}]{rhs}"

    def as_write_type_hint(self) -> str:
        rhs = ""
        if self.is_nullable:
            rhs = " = []"
        return f"list[{self.type_}]{rhs}"


@dataclass(frozen=True)
class EdgeField(Field, ABC):
    """
    This represents an edge field linking to another data class.
    """

    data_class: DataClass

    @property
    def is_edge(self) -> bool:
        return True

    @property
    def is_time_field(self) -> bool:
        return False


@dataclass(frozen=True)
class EdgeOneToOne(EdgeField):
    """
    This represents an edge field linking to another data class.
    """

    prop: dm.MappedProperty

    def as_read_type_hint(self) -> str:
        return "Optional[str] = None"

    def as_write_type_hint(self) -> str:
        return f"Union[{self.data_class.write_name}, str, None] = {self.pydantic_field}(None, repr=False)"


@dataclass(frozen=True)
class EdgeOneToMany(EdgeField):
    """
    This represents a list of edge fields linking to another data class.
    """

    variable: str
    edge_api_class: str
    edge_api_attribute: str
    prop: dm.SingleHopConnectionDefinition

    def as_read_type_hint(self) -> str:
        return "list[str] = []"

    def as_write_type_hint(self) -> str:
        return (
            f"Union[list[{self.data_class.write_name}], list[str]] "
            f"= {self.pydantic_field}(default_factory=list, repr=False)"
        )


@dataclass(frozen=True)
class DataClass:
    """
    This represents a data class. It is created from a view.
    """

    view_name: str
    read_name: str
    write_name: str
    read_list_name: str
    write_list_name: str
    variable: str
    variable_list: str
    file_name: str
    view_id: dm.ViewId
    fields: list[Field] = field(default_factory=list)

    @classmethod
    def from_view(cls, view: dm.View, config: PygenConfig) -> Self:
        view_name = (view.name or view.external_id).replace(" ", "_")
        class_name = create_name(view_name, config.naming.data_class.name)
        variable_name = create_name(view_name, config.naming.data_class.variable)
        variable_list = create_name(view_name, config.naming.data_class.variable_list)
        file_name = f"_{create_name(view_name, config.naming.data_class.file)}"
        return cls(
            view_name=view_name,
            read_name=class_name,
            write_name=f"{class_name}Apply",
            read_list_name=f"{class_name}List",
            write_list_name=f"{class_name}ApplyList",
            variable=variable_name,
            variable_list=variable_list,
            file_name=file_name,
            view_id=view.as_id(),
        )

    def update_fields(
        self,
        properties: dict[str, dm.MappedProperty | dm.ConnectionDefinition],
        data_class_by_view_id: dict[tuple[str, str], DataClass],
        config: PygenConfig,
    ) -> None:
        pydantic_field = self.pydantic_field
        for prop_name, prop in properties.items():
            field_ = Field.from_property(
                prop_name, prop, data_class_by_view_id, config, self.view_name, pydantic_field=pydantic_field
            )
            self.fields.append(field_)

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
        return f"from .{self.file_name} " f"import {self.read_name}, {self.write_name}, {self.read_list_name}"

    def __iter__(self) -> Iterator[Field]:
        return iter(self.fields)

    @property
    def one_to_one_edges(self) -> Iterable[EdgeOneToOne]:
        return (field_ for field_ in self.fields if isinstance(field_, EdgeOneToOne))

    @property
    def one_to_many_edges(self) -> Iterable[EdgeOneToMany]:
        return (field_ for field_ in self.fields if isinstance(field_, EdgeOneToMany))

    @property
    def has_one_to_many_edges(self) -> bool:
        return any(isinstance(field_, EdgeOneToMany) for field_ in self.fields)

    @property
    def has_edges(self) -> bool:
        return any(isinstance(field_, EdgeField) for field_ in self.fields)

    @property
    def has_only_one_to_many_edges(self) -> bool:
        return all(isinstance(field_, EdgeOneToMany) for field_ in self.fields)

    @property
    def fields_by_container(self) -> dict[dm.ContainerId, list[PrimitiveFieldCore | EdgeOneToOne]]:
        result: dict[dm.ContainerId, list[PrimitiveFieldCore | EdgeOneToOne]] = defaultdict(list)
        for field_ in self:
            if isinstance(field_, (PrimitiveFieldCore, EdgeOneToOne)):
                result[field_.prop.container].append(field_)
        return dict(result)

    @property
    def has_time_field(self) -> bool:
        return any(field_.is_time_field for field_ in self.fields)

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
    def import_pydantic_field(self) -> str:
        if self.pydantic_field == "Field":
            return "from pydantic import Field"
        else:
            return "import pydantic"

    @property
    def dependencies(self) -> list[DataClass]:
        unique: dict[dm.ViewId, DataClass] = {}
        for field_ in self.fields:
            if isinstance(field_, EdgeField):
                # This will overwrite any existing data class with the same view id
                # however, this is not a problem as all data classes are uniquely identified by their view id
                unique[field_.data_class.view_id] = field_.data_class
        return sorted(unique.values(), key=lambda x: x.write_name)


@dataclass(frozen=True)
class APIClass:
    # Todo: Check is variable and variable_list are needed
    variable: str
    variable_list: str
    client_attribute: str
    name: str
    file_name: str
    view_id: dm.ViewId

    @classmethod
    def from_view(cls, view: dm.View, config: PygenConfig) -> APIClass:
        raw_name = view.name or view.external_id

        raw_name = raw_name.replace(" ", "_")

        return cls(
            variable=create_name(raw_name, config.naming.api_class.variable),
            variable_list=create_name(raw_name, config.naming.api_class.variable_list),
            client_attribute=create_name(raw_name, config.naming.api_class.client_attribute),
            name=f"{create_name(raw_name, config.naming.api_class.name)}API",
            file_name=create_name(raw_name, config.naming.api_class.file_name),
            view_id=view.as_id(),
        )


@dataclass(frozen=True)
class MultiAPIClass:
    """
    This represents a set of APIs which are generated from a single data model.

    The motivation for having this class is the case when you want to create one SDK for multiple data models.
    """

    sub_apis: list[APIClass]
    client_attribute: str
    name: str
    model_id: dm.DataModelId

    @classmethod
    def from_data_model(
        cls, data_model: dm.DataModel, api_class_by_view_id: dict[tuple[str, str], APIClass], config: PygenConfig
    ) -> MultiAPIClass:
        sub_apis = sorted(
            [api_class_by_view_id[(view.space, view.external_id)] for view in data_model.views],
            key=lambda api: api.name,
        )

        data_model_name = data_model.name or data_model.external_id

        return cls(
            sub_apis=sub_apis,
            client_attribute=create_name(data_model_name, config.naming.multi_api_class.client_attribute),
            name=f"{create_name(data_model_name, config.naming.multi_api_class.name)}APIs",
            model_id=data_model.as_id(),
        )


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
    elif isinstance(type_, (dm.Text, dm.DirectRelation, dm.CDFExternalIdReference, dm.DirectRelationReference)):
        out_type = "str"
    else:
        raise ValueError(f"Unknown type {type_}")

    return out_type
