from __future__ import annotations

import datetime
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
    "PrimitiveRequiredListed",
    "PrimitiveRequiredListedApply",
    "PrimitiveRequiredListedList",
    "PrimitiveRequiredListedApplyList",
    "PrimitiveRequiredListedFields",
    "PrimitiveRequiredListedTextFields",
]


PrimitiveRequiredListedTextFields = Literal["text"]
PrimitiveRequiredListedFields = Literal[
    "boolean", "date", "float_32", "float_64", "int_32", "int_64", "json_", "text", "timestamp"
]

_PRIMITIVEREQUIREDLISTED_PROPERTIES_BY_FIELD = {
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


class PrimitiveRequiredListed(DomainModel):
    """This represents the reading version of primitive required listed.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive required listed.
        data_record: The data record of the primitive required listed node.
        boolean: The boolean field.
        date: The date field.
        float_32: The float 32 field.
        float_64: The float 64 field.
        int_32: The int 32 field.
        int_64: The int 64 field.
        json_: The json field.
        text: The text field.
        timestamp: The timestamp field.
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

    def as_apply(self) -> PrimitiveRequiredListedApply:
        """Convert this read version of primitive required listed to the writing version."""
        return PrimitiveRequiredListedApply(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
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


class PrimitiveRequiredListedApply(DomainModelApply):
    """This represents the writing version of primitive required listed.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive required listed.
        data_record: The data record of the primitive required listed node.
        boolean: The boolean field.
        date: The date field.
        float_32: The float 32 field.
        float_64: The float 64 field.
        int_32: The int 32 field.
        int_64: The int 64 field.
        json_: The json field.
        text: The text field.
        timestamp: The timestamp field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
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
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            PrimitiveRequiredListed, dm.ViewId("pygen-models", "PrimitiveRequiredListed", "1")
        )

        properties: dict[str, Any] = {}

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


class PrimitiveRequiredListedList(DomainModelList[PrimitiveRequiredListed]):
    """List of primitive required listeds in the read version."""

    _INSTANCE = PrimitiveRequiredListed

    def as_apply(self) -> PrimitiveRequiredListedApplyList:
        """Convert these read versions of primitive required listed to the writing versions."""
        return PrimitiveRequiredListedApplyList([node.as_apply() for node in self.data])


class PrimitiveRequiredListedApplyList(DomainModelApplyList[PrimitiveRequiredListedApply]):
    """List of primitive required listeds in the writing version."""

    _INSTANCE = PrimitiveRequiredListedApply


def _create_primitive_required_listed_filter(
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
