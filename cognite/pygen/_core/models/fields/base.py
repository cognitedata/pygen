"""This module contains the base class for all fields."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypeVar

from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.data_modeling.views import (
    ReverseDirectRelation,
    SingleEdgeConnection,
    ViewProperty,
)

from cognite.pygen import config as pygen_config
from cognite.pygen.config.reserved_words import is_reserved_word
from cognite.pygen.utils.text import create_name, to_words

if TYPE_CHECKING:
    from cognite.pygen._core.models.data_classes import DataClass

_PRIMITIVE_TYPES = (dm.Text, dm.Boolean, dm.Float32, dm.Float64, dm.Int32, dm.Int64, dm.Timestamp, dm.Date, dm.Json)
_EXTERNAL_TYPES = (dm.TimeSeriesReference, dm.FileReference, dm.SequenceReference)


@dataclass(frozen=True)
class Field(ABC):
    """
    A field represents a pydantic field in the generated pydantic class.

    Args:
        name: The name of the field. This is used in the generated Python code.
        doc_name: The name of the field in the documentation.
        prop_name: The name of the property in the data model. This is used when reading and writing to CDF.
        pydantic_field: The name to use for the import 'from pydantic import Field'. This is used in the edge case
                        when the name 'Field' name clashes with the data model class name.

    """

    name: str
    doc_name: str
    prop_name: str
    description: str | None
    pydantic_field: Literal["Field", "pydantic.Field"]

    @property
    def need_alias(self) -> bool:
        return self.name != self.prop_name

    @classmethod
    def from_property(
        cls,
        prop_name: str,
        prop: ViewProperty,
        data_class_by_view_id: dict[dm.ViewId, DataClass],
        config: pygen_config.PygenConfig,
        view_id: dm.ViewId,
        pydantic_field: Literal["Field", "pydantic.Field"],
    ) -> Field | None:
        from .cdf_reference import CDFExternalField, CDFExternalListField
        from .connections import (
            EdgeOneToManyEdges,
            EdgeOneToManyNodes,
            EdgeOneToOne,
            EdgeOneToOneAny,
            EdgeTypedOneToOne,
        )
        from .primitive import PrimitiveField, PrimitiveListField

        field_naming = config.naming.field
        name = create_name(prop_name, field_naming.name)
        if is_reserved_word(name, "field", view_id, prop_name):
            name = f"{name}_"

        doc_name = to_words(name, singularize=True)
        variable = create_name(prop_name, field_naming.variable)

        if isinstance(prop, dm.SingleHopConnectionDefinition) and prop.edge_source:
            return EdgeOneToManyEdges(
                name=name,
                doc_name=doc_name,
                prop_name=prop_name,
                variable=variable,
                data_class=data_class_by_view_id[prop.edge_source],
                edge_type=prop.type,
                edge_direction=prop.direction,
                description=prop.description,
                pydantic_field=pydantic_field,
            )
        elif isinstance(prop, dm.SingleHopConnectionDefinition):
            return EdgeOneToManyNodes(
                name=name,
                doc_name=doc_name,
                prop_name=prop_name,
                variable=variable,
                data_class=data_class_by_view_id[prop.source],
                edge_type=prop.type,
                edge_direction=prop.direction,
                description=prop.description,
                pydantic_field=pydantic_field,
            )
        elif isinstance(prop, dm.MappedProperty) and prop.type.is_list:
            if isinstance(prop.type, dm.CDFExternalIdReference):
                return CDFExternalListField(
                    name=name,
                    prop_name=prop_name,
                    doc_name=doc_name,
                    type_=prop.type,
                    is_nullable=prop.nullable,
                    description=prop.description,
                    pydantic_field=pydantic_field,
                    variable=variable,
                )
            else:
                return PrimitiveListField(
                    name=name,
                    prop_name=prop_name,
                    doc_name=doc_name,
                    type_=prop.type,
                    is_nullable=prop.nullable,
                    pydantic_field=pydantic_field,
                    description=prop.description,
                    variable=variable,
                )
        elif isinstance(prop, dm.MappedProperty) and isinstance(prop.type, dm.CDFExternalIdReference):
            # Note: these are only CDF External Fields that are not listable. Listable CDF External Fields
            # are handled above.
            return CDFExternalField(
                name=name,
                prop_name=prop_name,
                doc_name=doc_name,
                type_=prop.type,
                is_nullable=prop.nullable,
                description=prop.description,
                pydantic_field=pydantic_field,
            )
        elif isinstance(prop, dm.MappedProperty) and isinstance(prop.type, dm.DirectRelation) and prop.source:
            # Connected in View
            target_data_class = data_class_by_view_id[prop.source]
            return EdgeOneToOne(
                name=name,
                prop_name=prop_name,
                description=prop.description,
                data_class=target_data_class,
                pydantic_field=pydantic_field,
                doc_name=doc_name,
            )
        elif isinstance(prop, dm.MappedProperty) and isinstance(prop.type, dm.DirectRelation):
            return EdgeOneToOneAny(
                name=name,
                prop_name=prop_name,
                description=prop.description,
                pydantic_field=pydantic_field,
                doc_name=doc_name,
            )
        elif isinstance(prop, dm.MappedProperty):
            return PrimitiveField(
                name=name,
                prop_name=prop_name,
                doc_name=doc_name,
                type_=prop.type,
                is_nullable=prop.nullable,
                default=prop.default_value,
                pydantic_field=pydantic_field,
                description=prop.description,
            )
        elif isinstance(prop, SingleEdgeConnection) and prop.edge_source:
            raise NotImplementedError("SingleEdgeConnection with edge properties is not yet supported")
        elif isinstance(prop, SingleEdgeConnection):
            target_data_class = data_class_by_view_id[prop.source]
            return EdgeTypedOneToOne(
                name=name,
                variable=variable,
                edge_type=prop.type,
                edge_direction=prop.direction,
                prop_name=prop_name,
                description=prop.description,
                data_class=target_data_class,
                pydantic_field=pydantic_field,
                doc_name=doc_name,
            )
        elif isinstance(prop, ReverseDirectRelation):
            # ReverseDirectRelation are skipped as they are not used in the generated SDK.
            return None
        else:
            raise NotImplementedError(f"Property type={type(prop)} is not yet supported")

    @abstractmethod
    def as_read_type_hint(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def as_write_type_hint(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def as_graphql_type_hint(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def as_write(self) -> str:
        """Used in the .as_write() method for the read version of the data class."""
        raise NotImplementedError

    def as_write_graphql(self) -> str:
        """Used in the .as_write() method for the graphQL version of the data class."""
        return self.as_write()

    @abstractmethod
    def as_read(self) -> str:
        """Used in the .as_read() method for the graphQL version of the data class."""

    @property
    def argument_documentation(self) -> str:
        if self.description:
            return self.description
        else:
            return f"The {self.doc_name} field."

    # The properties below are overwritten in the child classes
    @property
    def is_edge(self) -> bool:
        return False

    @property
    def is_time_field(self) -> bool:
        return False

    @property
    def is_timestamp(self) -> bool:
        return False

    @property
    def is_time_series(self) -> bool:
        return False

    @property
    def is_list(self) -> bool:
        return False

    @property
    def is_text_field(self) -> bool:
        return False


T_Field = TypeVar("T_Field", bound=Field)


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
    elif isinstance(type_, dm.TimeSeriesReference):
        out_type = "TimeSeries"
    elif isinstance(type_, (dm.Text, dm.DirectRelation, dm.CDFExternalIdReference, dm.DirectRelationReference)):
        out_type = "str"
    else:
        raise ValueError(f"Unknown type {type_}")

    return out_type
