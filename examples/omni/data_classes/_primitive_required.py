from __future__ import annotations

import datetime
import warnings
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
)


__all__ = [
    "PrimitiveRequired",
    "PrimitiveRequiredWrite",
    "PrimitiveRequiredApply",
    "PrimitiveRequiredList",
    "PrimitiveRequiredWriteList",
    "PrimitiveRequiredApplyList",
    "PrimitiveRequiredFields",
    "PrimitiveRequiredTextFields",
    "PrimitiveRequiredGraphQL",
]


PrimitiveRequiredTextFields = Literal["text"]
PrimitiveRequiredFields = Literal[
    "boolean", "date", "float_32", "float_64", "int_32", "int_64", "json_", "text", "timestamp"
]

_PRIMITIVEREQUIRED_PROPERTIES_BY_FIELD = {
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


class PrimitiveRequiredGraphQL(GraphQLCore):
    """This represents the reading version of primitive required, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive required.
        data_record: The data record of the primitive required node.
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

    view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "PrimitiveRequired", "1")
    boolean: Optional[bool] = None
    date: Optional[datetime.date] = None
    float_32: Optional[float] = Field(None, alias="float32")
    float_64: Optional[float] = Field(None, alias="float64")
    int_32: Optional[int] = Field(None, alias="int32")
    int_64: Optional[int] = Field(None, alias="int64")
    json_: Optional[dict] = Field(None, alias="json")
    text: Optional[str] = None
    timestamp: Optional[datetime.datetime] = None

    @model_validator(mode="before")
    def parse_data_record(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        if "lastUpdatedTime" in values or "createdTime" in values:
            values["dataRecord"] = DataRecordGraphQL(
                created_time=values.pop("createdTime", None),
                last_updated_time=values.pop("lastUpdatedTime", None),
            )
        return values

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> PrimitiveRequired:
        """Convert this GraphQL format of primitive required to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return PrimitiveRequired(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> PrimitiveRequiredWrite:
        """Convert this GraphQL format of primitive required to the writing format."""
        return PrimitiveRequiredWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
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


class PrimitiveRequired(DomainModel):
    """This represents the reading version of primitive required.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive required.
        data_record: The data record of the primitive required node.
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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "PrimitiveRequired", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    boolean: bool
    date: datetime.date
    float_32: float = Field(alias="float32")
    float_64: float = Field(alias="float64")
    int_32: int = Field(alias="int32")
    int_64: int = Field(alias="int64")
    json_: dict = Field(alias="json")
    text: str
    timestamp: datetime.datetime

    def as_write(self) -> PrimitiveRequiredWrite:
        """Convert this read version of primitive required to the writing version."""
        return PrimitiveRequiredWrite(
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

    def as_apply(self) -> PrimitiveRequiredWrite:
        """Convert this read version of primitive required to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PrimitiveRequiredWrite(DomainModelWrite):
    """This represents the writing version of primitive required.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the primitive required.
        data_record: The data record of the primitive required node.
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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "PrimitiveRequired", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    boolean: bool
    date: datetime.date
    float_32: float = Field(alias="float32")
    float_64: float = Field(alias="float64")
    int_32: int = Field(alias="int32")
    int_64: int = Field(alias="int64")
    json_: dict = Field(alias="json")
    text: str
    timestamp: datetime.datetime

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        properties: dict[str, Any] = {}

        if self.boolean is not None:
            properties["boolean"] = self.boolean

        if self.date is not None:
            properties["date"] = self.date.isoformat() if self.date else None

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
            properties["timestamp"] = self.timestamp.isoformat(timespec="milliseconds") if self.timestamp else None

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        return resources


class PrimitiveRequiredApply(PrimitiveRequiredWrite):
    def __new__(cls, *args, **kwargs) -> PrimitiveRequiredApply:
        warnings.warn(
            "PrimitiveRequiredApply is deprecated and will be removed in v1.0. Use PrimitiveRequiredWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PrimitiveRequired.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class PrimitiveRequiredList(DomainModelList[PrimitiveRequired]):
    """List of primitive requireds in the read version."""

    _INSTANCE = PrimitiveRequired

    def as_write(self) -> PrimitiveRequiredWriteList:
        """Convert these read versions of primitive required to the writing versions."""
        return PrimitiveRequiredWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PrimitiveRequiredWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PrimitiveRequiredWriteList(DomainModelWriteList[PrimitiveRequiredWrite]):
    """List of primitive requireds in the writing version."""

    _INSTANCE = PrimitiveRequiredWrite


class PrimitiveRequiredApplyList(PrimitiveRequiredWriteList): ...


def _create_primitive_required_filter(
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
    filters: list[dm.Filter] = []
    if isinstance(boolean, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("boolean"), value=boolean))
    if min_date is not None or max_date is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("date"),
                gte=min_date.isoformat() if min_date else None,
                lte=max_date.isoformat() if max_date else None,
            )
        )
    if min_float_32 is not None or max_float_32 is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("float32"), gte=min_float_32, lte=max_float_32))
    if min_float_64 is not None or max_float_64 is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("float64"), gte=min_float_64, lte=max_float_64))
    if min_int_32 is not None or max_int_32 is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("int32"), gte=min_int_32, lte=max_int_32))
    if min_int_64 is not None or max_int_64 is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("int64"), gte=min_int_64, lte=max_int_64))
    if isinstance(text, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("text"), value=text))
    if text and isinstance(text, list):
        filters.append(dm.filters.In(view_id.as_property_ref("text"), values=text))
    if text_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("text"), value=text_prefix))
    if min_timestamp is not None or max_timestamp is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("timestamp"),
                gte=min_timestamp.isoformat(timespec="milliseconds") if min_timestamp else None,
                lte=max_timestamp.isoformat(timespec="milliseconds") if max_timestamp else None,
            )
        )
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
