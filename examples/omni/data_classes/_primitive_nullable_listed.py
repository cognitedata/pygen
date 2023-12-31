from __future__ import annotations

import datetime
from typing import Literal, Optional, Union

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
    "PrimitiveNullableListed",
    "PrimitiveNullableListedApply",
    "PrimitiveNullableListedList",
    "PrimitiveNullableListedApplyList",
    "PrimitiveNullableListedFields",
    "PrimitiveNullableListedTextFields",
]


PrimitiveNullableListedTextFields = Literal["text"]
PrimitiveNullableListedFields = Literal[
    "boolean", "date", "float_32", "float_64", "int_32", "int_64", "json_", "text", "timestamp"
]

_PRIMITIVENULLABLELISTED_PROPERTIES_BY_FIELD = {
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


class PrimitiveNullableListed(DomainModel):
    """This represents the reading version of primitive nullable listed.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive nullable listed.
        boolean: The boolean field.
        date: The date field.
        float_32: The float 32 field.
        float_64: The float 64 field.
        int_32: The int 32 field.
        int_64: The int 64 field.
        json_: The json field.
        text: The text field.
        timestamp: The timestamp field.
        created_time: The created time of the primitive nullable listed node.
        last_updated_time: The last updated time of the primitive nullable listed node.
        deleted_time: If present, the deleted time of the primitive nullable listed node.
        version: The version of the primitive nullable listed node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    boolean: Optional[list[bool]] = None
    date: Optional[list[datetime.date]] = None
    float_32: Optional[list[float]] = Field(None, alias="float32")
    float_64: Optional[list[float]] = Field(None, alias="float64")
    int_32: Optional[list[int]] = Field(None, alias="int32")
    int_64: Optional[list[int]] = Field(None, alias="int64")
    json_: Optional[list[dict]] = Field(None, alias="json")
    text: Optional[list[str]] = None
    timestamp: Optional[list[datetime.datetime]] = None

    def as_apply(self) -> PrimitiveNullableListedApply:
        """Convert this read version of primitive nullable listed to the writing version."""
        return PrimitiveNullableListedApply(
            space=self.space,
            external_id=self.external_id,
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


class PrimitiveNullableListedApply(DomainModelApply):
    """This represents the writing version of primitive nullable listed.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive nullable listed.
        boolean: The boolean field.
        date: The date field.
        float_32: The float 32 field.
        float_64: The float 64 field.
        int_32: The int 32 field.
        int_64: The int 64 field.
        json_: The json field.
        text: The text field.
        timestamp: The timestamp field.
        existing_version: Fail the ingestion request if the primitive nullable listed version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    boolean: Optional[list[bool]] = None
    date: Optional[list[datetime.date]] = None
    float_32: Optional[list[float]] = Field(None, alias="float32")
    float_64: Optional[list[float]] = Field(None, alias="float64")
    int_32: Optional[list[int]] = Field(None, alias="int32")
    int_64: Optional[list[int]] = Field(None, alias="int64")
    json_: Optional[list[dict]] = Field(None, alias="json")
    text: Optional[list[str]] = None
    timestamp: Optional[list[datetime.datetime]] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            PrimitiveNullableListed, dm.ViewId("pygen-models", "PrimitiveNullableListed", "1")
        )

        properties = {}

        if self.boolean is not None:
            properties["boolean"] = self.boolean

        if self.date is not None:
            properties["date"] = [date.isoformat() for date in self.date]

        if self.float_32 is not None:
            properties["float32"] = self.float_32

        if self.float_64 is not None:
            properties["float64"] = self.float_64

        if self.int_32 is not None:
            properties["int32"] = self.int_32

        if self.int_64 is not None:
            properties["int64"] = self.int_64

        if self.json_ is not None:
            properties["json"] = self.json_

        if self.text is not None:
            properties["text"] = self.text

        if self.timestamp is not None:
            properties["timestamp"] = [timestamp.isoformat(timespec="milliseconds") for timestamp in self.timestamp]

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
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


class PrimitiveNullableListedList(DomainModelList[PrimitiveNullableListed]):
    """List of primitive nullable listeds in the read version."""

    _INSTANCE = PrimitiveNullableListed

    def as_apply(self) -> PrimitiveNullableListedApplyList:
        """Convert these read versions of primitive nullable listed to the writing versions."""
        return PrimitiveNullableListedApplyList([node.as_apply() for node in self.data])


class PrimitiveNullableListedApplyList(DomainModelApplyList[PrimitiveNullableListedApply]):
    """List of primitive nullable listeds in the writing version."""

    _INSTANCE = PrimitiveNullableListedApply


def _create_primitive_nullable_listed_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
