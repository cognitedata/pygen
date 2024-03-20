from __future__ import annotations

import datetime
from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)


__all__ = [
    "PrimitiveRequiredList",
    "PrimitiveRequiredListApply",
    "PrimitiveRequiredListList",
    "PrimitiveRequiredListApplyList",
    "PrimitiveRequiredListFields",
    "PrimitiveRequiredListTextFields",
]


PrimitiveRequiredListTextFields = Literal["text"]
PrimitiveRequiredListFields = Literal[
    "boolean", "date", "float_32", "float_64", "int_32", "int_64", "json_", "text", "timestamp"
]

_PRIMITIVEREQUIREDLIST_PROPERTIES_BY_FIELD = {
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


class PrimitiveRequiredList(DomainModel):
    """This represents the reading version of primitive required list.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive required list.
        boolean: The boolean field.
        date: The date field.
        float_32: The float 32 field.
        float_64: The float 64 field.
        int_32: The int 32 field.
        int_64: The int 64 field.
        json_: The json field.
        text: The text field.
        timestamp: The timestamp field.
        created_time: The created time of the primitive required list node.
        last_updated_time: The last updated time of the primitive required list node.
        deleted_time: If present, the deleted time of the primitive required list node.
        version: The version of the primitive required list node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    boolean: Optional[list[bool]] = None
    date: Optional[list[datetime.date]] = None
    float_32: Optional[list[float]] = Field(None, alias="float32")
    float_64: Optional[list[float]] = Field(None, alias="float64")
    int_32: Optional[list[int]] = Field(None, alias="int32")
    int_64: Optional[list[int]] = Field(None, alias="int64")
    json_: Optional[list[dict]] = Field(None, alias="json")
    text: Optional[list[str]] = None
    timestamp: Optional[list[datetime.datetime]] = None

    def as_apply(self) -> PrimitiveRequiredListApply:
        """Convert this read version of primitive required list to the writing version."""
        return PrimitiveRequiredListApply(
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


class PrimitiveRequiredListApply(DomainModelApply):
    """This represents the writing version of primitive required list.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive required list.
        boolean: The boolean field.
        date: The date field.
        float_32: The float 32 field.
        float_64: The float 64 field.
        int_32: The int 32 field.
        int_64: The int 64 field.
        json_: The json field.
        text: The text field.
        timestamp: The timestamp field.
        existing_version: Fail the ingestion request if the primitive required list version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    boolean: list[bool]
    date: list[datetime.date]
    float_32: list[float] = Field(alias="float32")
    float_64: list[float] = Field(alias="float64")
    int_32: list[int] = Field(alias="int32")
    int_64: list[int] = Field(alias="int64")
    json_: list[dict] = Field(alias="json")
    text: list[str]
    timestamp: list[datetime.datetime]

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "pygen-models", "PrimitiveRequiredList", "1"
        )

        properties = {}

        if self.boolean is not None:
            properties["boolean"] = self.boolean

        if self.date is not None:
            properties["date"] = self.date.isoformat()

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
            properties["timestamp"] = self.timestamp.isoformat(timespec="milliseconds")

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


class PrimitiveRequiredListList(DomainModelList[PrimitiveRequiredList]):
    """List of primitive required lists in the read version."""

    _INSTANCE = PrimitiveRequiredList

    def as_apply(self) -> PrimitiveRequiredListApplyList:
        """Convert these read versions of primitive required list to the writing versions."""
        return PrimitiveRequiredListApplyList([node.as_write() for node in self.data])


class PrimitiveRequiredListApplyList(DomainModelApplyList[PrimitiveRequiredListApply]):
    """List of primitive required lists in the writing version."""

    _INSTANCE = PrimitiveRequiredListApply


def _create_primitive_required_list_filter(
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
