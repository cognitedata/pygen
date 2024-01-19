from __future__ import annotations

import datetime
from typing import Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelCore,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)


__all__ = [
    "PrimitiveNullable",
    "PrimitiveNullableApply",
    "PrimitiveNullableList",
    "PrimitiveNullableApplyList",
    "PrimitiveNullableFields",
    "PrimitiveNullableTextFields",
]


PrimitiveNullableTextFields = Literal["text"]
PrimitiveNullableFields = Literal[
    "boolean", "date", "float_32", "float_64", "int_32", "int_64", "json_", "text", "timestamp"
]

_PRIMITIVENULLABLE_PROPERTIES_BY_FIELD = {
    "boolean": "boolean",
    "date": "date",
    "float_32": "float32",
    "float_64": "float64",
    "int_32": "int32",
    "int_64": "int64",
    "json_": "json",
    "text": "text",
    "timestamp": "timestamp",
}


class PrimitiveNullable(DomainModel):
    """This represents the reading version of primitive nullable.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive nullable.
        boolean: The boolean field.
        date: The date field.
        float_32: The float 32 field.
        float_64: The float 64 field.
        int_32: The int 32 field.
        int_64: The int 64 field.
        json_: The json field.
        text: The text field.
        timestamp: The timestamp field.
        created_time: The created time of the primitive nullable node.
        last_updated_time: The last updated time of the primitive nullable node.
        deleted_time: If present, the deleted time of the primitive nullable node.
        version: The version of the primitive nullable node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    boolean: Optional[bool] = None
    date: Optional[datetime.date] = None
    float_32: Optional[float] = Field(None, alias="float32")
    float_64: Optional[float] = Field(None, alias="float64")
    int_32: Optional[int] = Field(None, alias="int32")
    int_64: Optional[int] = Field(None, alias="int64")
    json_: Optional[dict] = Field(None, alias="json")
    text: Optional[str] = None
    timestamp: Optional[datetime.datetime] = None

    def as_apply(self) -> PrimitiveNullableApply:
        """Convert this read version of primitive nullable to the writing version."""
        return PrimitiveNullableApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.version,
            boolean=self.boolean,
            date=self.date,
            float_32=self.float_32,
            float_64=self.float_64,
            int_32=self.int_32,
            int_64=self.int_64,
            json_=self.json_,
            text=self.text,
            timestamp=self.timestamp,
        )


class PrimitiveNullableApply(DomainModelApply):
    """This represents the writing version of primitive nullable.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive nullable.
        boolean: The boolean field.
        date: The date field.
        float_32: The float 32 field.
        float_64: The float 64 field.
        int_32: The int 32 field.
        int_64: The int 64 field.
        json_: The json field.
        text: The text field.
        timestamp: The timestamp field.
        existing_version: Fail the ingestion request if the primitive nullable version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    boolean: Optional[bool] = None
    date: Optional[datetime.date] = None
    float_32: Optional[float] = Field(None, alias="float32")
    float_64: Optional[float] = Field(None, alias="float64")
    int_32: Optional[int] = Field(None, alias="int32")
    int_64: Optional[int] = Field(None, alias="int64")
    json_: Optional[dict] = Field(None, alias="json")
    text: Optional[str] = None
    timestamp: Optional[datetime.datetime] = None

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
            PrimitiveNullable, dm.ViewId("pygen-models", "PrimitiveNullable", "1")
        )

        properties: dict[str, Any] = {}

        if self.boolean is not None or write_none:
            properties["boolean"] = self.boolean

        if self.date is not None or write_none:
            properties["date"] = self.date.isoformat() if self.date else None

        if self.float_32 is not None or write_none:
            properties["float32"] = self.float_32

        if self.float_64 is not None or write_none:
            properties["float64"] = self.float_64

        if self.int_32 is not None or write_none:
            properties["int32"] = self.int_32

        if self.int_64 is not None or write_none:
            properties["int64"] = self.int_64

        if self.json_ is not None or write_none:
            properties["json"] = self.json_

        if self.text is not None or write_none:
            properties["text"] = self.text

        if self.timestamp is not None or write_none:
            properties["timestamp"] = self.timestamp.isoformat(timespec="milliseconds") if self.timestamp else None

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
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


class PrimitiveNullableList(DomainModelList[PrimitiveNullable]):
    """List of primitive nullables in the read version."""

    _INSTANCE = PrimitiveNullable

    def as_apply(self) -> PrimitiveNullableApplyList:
        """Convert these read versions of primitive nullable to the writing versions."""
        return PrimitiveNullableApplyList([node.as_apply() for node in self.data])


class PrimitiveNullableApplyList(DomainModelApplyList[PrimitiveNullableApply]):
    """List of primitive nullables in the writing version."""

    _INSTANCE = PrimitiveNullableApply


def _create_primitive_nullable_filter(
    view_id: dm.ViewId,
    boolean: bool | None = None,
    min_date: datetime.date | None = None,
    max_date: datetime.date | None = None,
    min_float_32: float | None = None,
    max_float_32: float | None = None,
    min_float_64: float | None = None,
    max_float_64: float | None = None,
    min_int_32: int | None = None,
    max_int_32: int | None = None,
    min_int_64: int | None = None,
    max_int_64: int | None = None,
    text: str | list[str] | None = None,
    text_prefix: str | None = None,
    min_timestamp: datetime.datetime | None = None,
    max_timestamp: datetime.datetime | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if boolean is not None and isinstance(boolean, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("boolean"), value=boolean))
    if min_date or max_date:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("date"),
                gte=min_date.isoformat() if min_date else None,
                lte=max_date.isoformat() if max_date else None,
            )
        )
    if min_float_32 or max_float_32:
        filters.append(dm.filters.Range(view_id.as_property_ref("float32"), gte=min_float_32, lte=max_float_32))
    if min_float_64 or max_float_64:
        filters.append(dm.filters.Range(view_id.as_property_ref("float64"), gte=min_float_64, lte=max_float_64))
    if min_int_32 or max_int_32:
        filters.append(dm.filters.Range(view_id.as_property_ref("int32"), gte=min_int_32, lte=max_int_32))
    if min_int_64 or max_int_64:
        filters.append(dm.filters.Range(view_id.as_property_ref("int64"), gte=min_int_64, lte=max_int_64))
    if text is not None and isinstance(text, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("text"), value=text))
    if text and isinstance(text, list):
        filters.append(dm.filters.In(view_id.as_property_ref("text"), values=text))
    if text_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("text"), value=text_prefix))
    if min_timestamp or max_timestamp:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("timestamp"),
                gte=min_timestamp.isoformat(timespec="milliseconds") if min_timestamp else None,
                lte=max_timestamp.isoformat(timespec="milliseconds") if max_timestamp else None,
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
