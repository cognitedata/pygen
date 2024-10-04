from __future__ import annotations

import datetime
import warnings
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelation,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    T_DomainModelList,
    as_direct_relation_reference,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    select_best_node,
    QueryCore,
    NodeQueryCore,
)


__all__ = [
    "PrimitiveRequiredListed",
    "PrimitiveRequiredListedWrite",
    "PrimitiveRequiredListedApply",
    "PrimitiveRequiredListedList",
    "PrimitiveRequiredListedWriteList",
    "PrimitiveRequiredListedApplyList",
    "PrimitiveRequiredListedFields",
    "PrimitiveRequiredListedTextFields",
    "PrimitiveRequiredListedGraphQL",
]


PrimitiveRequiredListedTextFields = Literal["external_id", "text"]
PrimitiveRequiredListedFields = Literal[
    "external_id", "boolean", "date", "float_32", "float_64", "int_32", "int_64", "json_", "text", "timestamp"
]

_PRIMITIVEREQUIREDLISTED_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
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


class PrimitiveRequiredListedGraphQL(GraphQLCore):
    """This represents the reading version of primitive required listed, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

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

    view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "PrimitiveRequiredListed", "1")
    boolean: Optional[list[bool]] = None
    date: Optional[list[datetime.date]] = None
    float_32: Optional[list[float]] = Field(None, alias="float32")
    float_64: Optional[list[float]] = Field(None, alias="float64")
    int_32: Optional[list[int]] = Field(None, alias="int32")
    int_64: Optional[list[int]] = Field(None, alias="int64")
    json_: Optional[list[dict]] = Field(None, alias="json")
    text: Optional[list[str]] = None
    timestamp: Optional[list[datetime.datetime]] = None

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
    def as_read(self) -> PrimitiveRequiredListed:
        """Convert this GraphQL format of primitive required listed to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return PrimitiveRequiredListed(
            space=self.space,
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
    def as_write(self) -> PrimitiveRequiredListedWrite:
        """Convert this GraphQL format of primitive required listed to the writing format."""
        return PrimitiveRequiredListedWrite(
            space=self.space,
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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "PrimitiveRequiredListed", "1")

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

    def as_write(self) -> PrimitiveRequiredListedWrite:
        """Convert this read version of primitive required listed to the writing version."""
        return PrimitiveRequiredListedWrite(
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

    def as_apply(self) -> PrimitiveRequiredListedWrite:
        """Convert this read version of primitive required listed to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PrimitiveRequiredListedWrite(DomainModelWrite):
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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "PrimitiveRequiredListed", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    boolean: list[bool]
    date: list[datetime.date]
    float_32: list[float] = Field(alias="float32")
    float_64: list[float] = Field(alias="float64")
    int_32: list[int] = Field(alias="int32")
    int_64: list[int] = Field(alias="int64")
    json_: list[dict] = Field(alias="json")
    text: list[str]
    timestamp: list[datetime.datetime]

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
            properties["date"] = [date.isoformat() for date in self.date or []]

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
            properties["timestamp"] = [
                timestamp.isoformat(timespec="milliseconds") for timestamp in self.timestamp or []
            ]

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=as_direct_relation_reference(self.node_type),
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


class PrimitiveRequiredListedApply(PrimitiveRequiredListedWrite):
    def __new__(cls, *args, **kwargs) -> PrimitiveRequiredListedApply:
        warnings.warn(
            "PrimitiveRequiredListedApply is deprecated and will be removed in v1.0. Use PrimitiveRequiredListedWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PrimitiveRequiredListed.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class PrimitiveRequiredListedList(DomainModelList[PrimitiveRequiredListed]):
    """List of primitive required listeds in the read version."""

    _INSTANCE = PrimitiveRequiredListed

    def as_write(self) -> PrimitiveRequiredListedWriteList:
        """Convert these read versions of primitive required listed to the writing versions."""
        return PrimitiveRequiredListedWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PrimitiveRequiredListedWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PrimitiveRequiredListedWriteList(DomainModelWriteList[PrimitiveRequiredListedWrite]):
    """List of primitive required listeds in the writing version."""

    _INSTANCE = PrimitiveRequiredListedWrite


class PrimitiveRequiredListedApplyList(PrimitiveRequiredListedWriteList): ...


def _create_primitive_required_listed_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _PrimitiveRequiredListedQuery(NodeQueryCore[T_DomainModelList, PrimitiveRequiredListedList]):
    _view_id = PrimitiveRequiredListed._view_id
    _result_cls = PrimitiveRequiredListed
    _result_list_cls_end = PrimitiveRequiredListedList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
    ):

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
        )


class PrimitiveRequiredListedQuery(_PrimitiveRequiredListedQuery[PrimitiveRequiredListedList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, PrimitiveRequiredListedList)

    def list_primitive_required_listed(self, limit: int = DEFAULT_QUERY_LIMIT) -> PrimitiveRequiredListedList:
        return self._list(limit=limit)
