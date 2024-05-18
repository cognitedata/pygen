"""This module contains the base class for all fields."""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypeVar

from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.data_modeling.views import (
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
class Field:
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
        from .cdf_reference import CDFExternalField
        from .connections import BaseConnectionField
        from .primitive import BasePrimitiveField

        field_naming = config.naming.field
        name = create_name(prop_name, field_naming.name)
        if is_reserved_word(name, "field", view_id, prop_name):
            name = f"{name}_"

        doc_name = to_words(name, singularize=True)
        variable = create_name(prop_name, field_naming.variable)
        base = cls(
            name=name,
            doc_name=doc_name,
            prop_name=prop_name,
            description=prop.description if hasattr(prop, "description") else None,
            pydantic_field=pydantic_field,
        )
        if isinstance(prop, dm.ConnectionDefinition) or (
            isinstance(prop, dm.MappedProperty) and isinstance(prop.type, dm.DirectRelation)
        ):
            return BaseConnectionField.load(base, prop, variable, data_class_by_view_id)
        elif isinstance(prop, dm.MappedProperty) and isinstance(prop.type, dm.CDFExternalIdReference):
            return CDFExternalField.load(base, prop, variable)
        elif isinstance(prop, dm.MappedProperty):
            return BasePrimitiveField.load(base, prop, variable)
        else:
            warnings.warn(
                f"Unsupported property type: {type(prop)}. Skipping field {prop_name}", UserWarning, stacklevel=2
            )
            return None

    def as_read_type_hint(self) -> str:
        raise NotImplementedError()

    def as_write_type_hint(self) -> str:
        raise NotImplementedError()

    def as_graphql_type_hint(self) -> str:
        raise NotImplementedError()

    def as_write(self) -> str:
        """Used in the .as_write() method for the read version of the data class."""
        raise NotImplementedError

    def as_write_graphql(self) -> str:
        """Used in the .as_write() method for the graphQL version of the data class."""
        return self.as_write()

    def as_read_graphql(self) -> str:
        """Used in the .as_read() method for the graphQL version of the data class."""
        raise NotImplementedError

    def as_value(self) -> str:
        """Used in the ._to_instances_write() method to write the value of the field to the node instance.
        This should only be implemented for container fields, i.e., fields that store their value in a container."""
        raise NotImplementedError

    @property
    def argument_documentation(self) -> str:
        if self.description:
            return self.description
        else:
            return f"The {self.doc_name} field."

    # The properties below are overwritten in the child classes
    @property
    def is_connection(self) -> bool:
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
