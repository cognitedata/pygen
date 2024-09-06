"""This module contains the base class for all fields."""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from string import digits
from typing import TYPE_CHECKING, Literal, TypeVar, cast

from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.data_modeling.views import (
    ViewProperty,
)

from cognite.pygen import config as pygen_config
from cognite.pygen.config.reserved_words import is_reserved_word
from cognite.pygen.utils.text import create_name, to_words

if TYPE_CHECKING:
    from cognite.pygen._core.models.data_classes import EdgeDataClass, NodeDataClass

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
        prop_id: str,
        prop: ViewProperty,
        node_class_by_view_id: dict[dm.ViewId, NodeDataClass],
        edge_class_by_view_id: dict[dm.ViewId, EdgeDataClass],
        config: pygen_config.PygenConfig,
        view_id: dm.ViewId,
        pydantic_field: Literal["Field", "pydantic.Field"],
        has_default_instance_space: bool,
        direct_relations_by_view_id: dict[dm.ViewId, set[str]],
        view_by_id: dict[dm.ViewId, dm.View],
    ) -> Field | None:
        from .cdf_reference import CDFExternalField
        from .connections import BaseConnectionField
        from .primitive import BasePrimitiveField

        field_naming = config.naming.field
        name = create_name(prop_id, field_naming.name)
        if name in {"type", "version"}:
            # Special handling for reserved words
            prop_view_id = _get_prop_view_id(prop_id, view_id, view_by_id)
            prefix = prop_view_id.external_id.removeprefix("Cognite").removeprefix("3D").lstrip(digits)
            name = create_name(f"{prefix}_{name}", field_naming.name)
        elif is_reserved_word(name, "field", view_id, prop_id):
            name = f"{name}_"

        doc_name = to_words(name, singularize=True)
        variable = create_name(prop_id, field_naming.variable)
        description: str | None = None
        if hasattr(prop, "description") and isinstance(prop.description, str):
            # This is a workaround for the fact that the description can contain curly quotes
            # which is ruff will complain about. (These comes from che Core model)
            description = prop.description.replace("‘", "'").replace("’", "'")  # noqa: RUF001

        base = cls(
            name=name,
            doc_name=doc_name,
            prop_name=prop_id,
            description=description,
            pydantic_field=pydantic_field,
        )
        if isinstance(prop, dm.ConnectionDefinition) or (
            isinstance(prop, dm.MappedProperty) and isinstance(prop.type, dm.DirectRelation)
        ):
            return BaseConnectionField.load(
                base,
                prop,
                variable,
                node_class_by_view_id,
                edge_class_by_view_id,
                has_default_instance_space,
                view_id,
                direct_relations_by_view_id,
            )
        elif isinstance(prop, dm.MappedProperty) and isinstance(prop.type, dm.CDFExternalIdReference):
            return CDFExternalField.load(base, prop, variable)
        elif isinstance(prop, dm.MappedProperty) and isinstance(prop.type, dm.PropertyType):
            return BasePrimitiveField.load(base, prop, variable)
        else:
            warnings.warn(
                f"Unsupported property type: {type(prop)}. Skipping field {prop_id}", UserWarning, stacklevel=2
            )
            return None

    def as_read_type_hint(self) -> str:
        raise NotImplementedError()

    def as_write_type_hint(self) -> str:
        raise NotImplementedError()

    def as_graphql_type_hint(self) -> str:
        raise NotImplementedError()

    def as_typed_hint(self, operation: Literal["write", "read"] = "write") -> str:
        raise NotImplementedError()

    def as_typed_init_set(self) -> str:
        return self.name

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

    @property
    def is_write_field(self) -> bool:
        return True


T_Field = TypeVar("T_Field", bound=Field)


def _get_prop_view_id(prop_id: str, view_id: dm.ViewId, view_by_id: dict[dm.ViewId, dm.View]) -> dm.ViewId:
    return cast(dm.ViewId, _search_source_view(prop_id, view_by_id[view_id], view_by_id))


def _search_source_view(prop_id: str, view: dm.View, view_by_id: dict[dm.ViewId, dm.View]) -> dm.ViewId | None:
    if prop_id in view.properties and not view.implements:
        return view.as_id()
    elif prop_id not in view.properties:
        return None
    # In properties, and have parents.
    candidates = set()
    for parent_id in view.implements:
        parent = view_by_id[parent_id]
        parent_view_id = _search_source_view(prop_id, parent, view_by_id)
        if parent_view_id:
            candidates.add(parent_view_id)
    if len(candidates) == 1:
        return candidates.pop()
    elif len(candidates) > 1:
        used = candidates.pop()
        warnings.warn(
            f"Multiple candidates for source view of property {prop_id}. Using {used}",
            UserWarning,
            stacklevel=2,
        )
        return used
    else:
        # Original to this view.
        return view.as_id()
