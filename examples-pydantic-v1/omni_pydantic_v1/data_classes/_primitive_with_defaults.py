from __future__ import annotations

from typing import Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)


__all__ = [
    "PrimitiveWithDefaults",
    "PrimitiveWithDefaultsApply",
    "PrimitiveWithDefaultsList",
    "PrimitiveWithDefaultsApplyList",
    "PrimitiveWithDefaultsFields",
    "PrimitiveWithDefaultsTextFields",
]


PrimitiveWithDefaultsTextFields = Literal["default_string"]
PrimitiveWithDefaultsFields = Literal[
    "auto_increment_int_32", "default_boolean", "default_float_32", "default_object", "default_string"
]

_PRIMITIVEWITHDEFAULTS_PROPERTIES_BY_FIELD = {
    "auto_increment_int_32": "autoIncrementInt32",
    "default_boolean": "defaultBoolean",
    "default_float_32": "defaultFloat32",
    "default_object": "defaultObject",
    "default_string": "defaultString",
}


class PrimitiveWithDefaults(DomainModel):
    """This represents the reading version of primitive with default.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive with default.
        data_record: The data record of the primitive with default node.
        auto_increment_int_32: The auto increment int 32 field.
        default_boolean: The default boolean field.
        default_float_32: The default float 32 field.
        default_object: The default object field.
        default_string: The default string field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    auto_increment_int_32: int = Field(alias="autoIncrementInt32")
    default_boolean: Optional[bool] = Field(None, alias="defaultBoolean")
    default_float_32: Optional[float] = Field(None, alias="defaultFloat32")
    default_object: Optional[dict] = Field(None, alias="defaultObject")
    default_string: Optional[str] = Field(None, alias="defaultString")

    def as_apply(self) -> PrimitiveWithDefaultsApply:
        """Convert this read version of primitive with default to the writing version."""
        return PrimitiveWithDefaultsApply(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            auto_increment_int_32=self.auto_increment_int_32,
            default_boolean=self.default_boolean,
            default_float_32=self.default_float_32,
            default_object=self.default_object,
            default_string=self.default_string,
        )


class PrimitiveWithDefaultsApply(DomainModelApply):
    """This represents the writing version of primitive with default.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive with default.
        data_record: The data record of the primitive with default node.
        auto_increment_int_32: The auto increment int 32 field.
        default_boolean: The default boolean field.
        default_float_32: The default float 32 field.
        default_object: The default object field.
        default_string: The default string field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    auto_increment_int_32: int = Field(alias="autoIncrementInt32")
    default_boolean: Optional[bool] = Field(True, alias="defaultBoolean")
    default_float_32: Optional[float] = Field(0.42, alias="defaultFloat32")
    default_object: Optional[dict] = Field({"foo": "bar"}, alias="defaultObject")
    default_string: Optional[str] = Field("my default text", alias="defaultString")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            PrimitiveWithDefaults, dm.ViewId("pygen-models", "PrimitiveWithDefaults", "1")
        )

        properties: dict[str, Any] = {}

        if self.auto_increment_int_32 is not None:
            properties["autoIncrementInt32"] = self.auto_increment_int_32

        if self.default_boolean is not None or write_none:
            properties["defaultBoolean"] = self.default_boolean

        if self.default_float_32 is not None or write_none:
            properties["defaultFloat32"] = self.default_float_32

        if self.default_object is not None or write_none:
            properties["defaultObject"] = self.default_object

        if self.default_string is not None or write_none:
            properties["defaultString"] = self.default_string

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        return resources


class PrimitiveWithDefaultsList(DomainModelList[PrimitiveWithDefaults]):
    """List of primitive with defaults in the read version."""

    _INSTANCE = PrimitiveWithDefaults

    def as_apply(self) -> PrimitiveWithDefaultsApplyList:
        """Convert these read versions of primitive with default to the writing versions."""
        return PrimitiveWithDefaultsApplyList([node.as_apply() for node in self.data])


class PrimitiveWithDefaultsApplyList(DomainModelApplyList[PrimitiveWithDefaultsApply]):
    """List of primitive with defaults in the writing version."""

    _INSTANCE = PrimitiveWithDefaultsApply


def _create_primitive_with_default_filter(
    view_id: dm.ViewId,
    min_auto_increment_int_32: int | None = None,
    max_auto_increment_int_32: int | None = None,
    default_boolean: bool | None = None,
    min_default_float_32: float | None = None,
    max_default_float_32: float | None = None,
    default_string: str | list[str] | None = None,
    default_string_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_auto_increment_int_32 is not None or max_auto_increment_int_32 is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("autoIncrementInt32"),
                gte=min_auto_increment_int_32,
                lte=max_auto_increment_int_32,
            )
        )
    if isinstance(default_boolean, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("defaultBoolean"), value=default_boolean))
    if min_default_float_32 is not None or max_default_float_32 is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("defaultFloat32"), gte=min_default_float_32, lte=max_default_float_32
            )
        )
    if isinstance(default_string, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("defaultString"), value=default_string))
    if default_string and isinstance(default_string, list):
        filters.append(dm.filters.In(view_id.as_property_ref("defaultString"), values=default_string))
    if default_string_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("defaultString"), value=default_string_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
