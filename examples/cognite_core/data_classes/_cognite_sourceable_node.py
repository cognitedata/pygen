from __future__ import annotations

import datetime
import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union, no_type_check

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from pydantic import Field, field_validator, model_validator

from cognite_core.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelList,
    DomainModelWrite,
    DomainModelWriteList,
    DomainRelation,
    GraphQLCore,
    NodeQueryCore,
    QueryCore,
    ResourcesWrite,
    StringFilter,
    T_DomainModelList,
    TimestampFilter,
    as_direct_relation_reference,
    as_instance_dict_id,
    is_tuple_id,
)

if TYPE_CHECKING:
    from cognite_core.data_classes._cognite_source_system import (
        CogniteSourceSystem,
        CogniteSourceSystemGraphQL,
        CogniteSourceSystemList,
        CogniteSourceSystemWrite,
        CogniteSourceSystemWriteList,
    )


__all__ = [
    "CogniteSourceableNode",
    "CogniteSourceableNodeWrite",
    "CogniteSourceableNodeApply",
    "CogniteSourceableNodeList",
    "CogniteSourceableNodeWriteList",
    "CogniteSourceableNodeApplyList",
    "CogniteSourceableNodeFields",
    "CogniteSourceableNodeTextFields",
    "CogniteSourceableNodeGraphQL",
]


CogniteSourceableNodeTextFields = Literal[
    "external_id", "source_context", "source_created_user", "source_id", "source_updated_user"
]
CogniteSourceableNodeFields = Literal[
    "external_id",
    "source_context",
    "source_created_time",
    "source_created_user",
    "source_id",
    "source_updated_time",
    "source_updated_user",
]

_COGNITESOURCEABLENODE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "source_context": "sourceContext",
    "source_created_time": "sourceCreatedTime",
    "source_created_user": "sourceCreatedUser",
    "source_id": "sourceId",
    "source_updated_time": "sourceUpdatedTime",
    "source_updated_user": "sourceUpdatedUser",
}


class CogniteSourceableNodeGraphQL(GraphQLCore):
    """This represents the reading version of Cognite sourceable node, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite sourceable node.
        data_record: The data record of the Cognite sourceable node node.
        source: Direct relation to a source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteSourceable", "v1")
    source: Optional[CogniteSourceSystemGraphQL] = Field(default=None, repr=False)
    source_context: Optional[str] = Field(None, alias="sourceContext")
    source_created_time: Optional[datetime.datetime] = Field(None, alias="sourceCreatedTime")
    source_created_user: Optional[str] = Field(None, alias="sourceCreatedUser")
    source_id: Optional[str] = Field(None, alias="sourceId")
    source_updated_time: Optional[datetime.datetime] = Field(None, alias="sourceUpdatedTime")
    source_updated_user: Optional[str] = Field(None, alias="sourceUpdatedUser")

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

    @field_validator("source", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> CogniteSourceableNode:
        """Convert this GraphQL format of Cognite sourceable node to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return CogniteSourceableNode(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            source=self.source.as_read() if isinstance(self.source, GraphQLCore) else self.source,
            source_context=self.source_context,
            source_created_time=self.source_created_time,
            source_created_user=self.source_created_user,
            source_id=self.source_id,
            source_updated_time=self.source_updated_time,
            source_updated_user=self.source_updated_user,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteSourceableNodeWrite:
        """Convert this GraphQL format of Cognite sourceable node to the writing format."""
        return CogniteSourceableNodeWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            source=self.source.as_write() if isinstance(self.source, GraphQLCore) else self.source,
            source_context=self.source_context,
            source_created_time=self.source_created_time,
            source_created_user=self.source_created_user,
            source_id=self.source_id,
            source_updated_time=self.source_updated_time,
            source_updated_user=self.source_updated_user,
        )


class CogniteSourceableNode(DomainModel):
    """This represents the reading version of Cognite sourceable node.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite sourceable node.
        data_record: The data record of the Cognite sourceable node node.
        source: Direct relation to a source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteSourceable", "v1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    source: Union[CogniteSourceSystem, str, dm.NodeId, None] = Field(default=None, repr=False)
    source_context: Optional[str] = Field(None, alias="sourceContext")
    source_created_time: Optional[datetime.datetime] = Field(None, alias="sourceCreatedTime")
    source_created_user: Optional[str] = Field(None, alias="sourceCreatedUser")
    source_id: Optional[str] = Field(None, alias="sourceId")
    source_updated_time: Optional[datetime.datetime] = Field(None, alias="sourceUpdatedTime")
    source_updated_user: Optional[str] = Field(None, alias="sourceUpdatedUser")

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteSourceableNodeWrite:
        """Convert this read version of Cognite sourceable node to the writing version."""
        return CogniteSourceableNodeWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            source=self.source.as_write() if isinstance(self.source, DomainModel) else self.source,
            source_context=self.source_context,
            source_created_time=self.source_created_time,
            source_created_user=self.source_created_user,
            source_id=self.source_id,
            source_updated_time=self.source_updated_time,
            source_updated_user=self.source_updated_user,
        )

    def as_apply(self) -> CogniteSourceableNodeWrite:
        """Convert this read version of Cognite sourceable node to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, CogniteSourceableNode],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._cognite_source_system import CogniteSourceSystem

        for instance in instances.values():
            if (
                isinstance(instance.source, dm.NodeId | str)
                and (source := nodes_by_id.get(instance.source))
                and isinstance(source, CogniteSourceSystem)
            ):
                instance.source = source


class CogniteSourceableNodeWrite(DomainModelWrite):
    """This represents the writing version of Cognite sourceable node.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite sourceable node.
        data_record: The data record of the Cognite sourceable node node.
        source: Direct relation to a source system
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext is expected to not be set.
        source_created_time: When the instance was created in source system (if available)
        source_created_user: User identifier from the source system on who created the source data. This identifier is not guaranteed to match the user identifiers in CDF
        source_id: Identifier from the source system
        source_updated_time: When the instance was last updated in the source system (if available)
        source_updated_user: User identifier from the source system on who last updated the source data. This identifier is not guaranteed to match the user identifiers in CDF
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteSourceable", "v1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    source: Union[CogniteSourceSystemWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    source_context: Optional[str] = Field(None, alias="sourceContext")
    source_created_time: Optional[datetime.datetime] = Field(None, alias="sourceCreatedTime")
    source_created_user: Optional[str] = Field(None, alias="sourceCreatedUser")
    source_id: Optional[str] = Field(None, alias="sourceId")
    source_updated_time: Optional[datetime.datetime] = Field(None, alias="sourceUpdatedTime")
    source_updated_user: Optional[str] = Field(None, alias="sourceUpdatedUser")

    @field_validator("source", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value

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

        if self.source is not None:
            properties["source"] = {
                "space": self.space if isinstance(self.source, str) else self.source.space,
                "externalId": self.source if isinstance(self.source, str) else self.source.external_id,
            }

        if self.source_context is not None or write_none:
            properties["sourceContext"] = self.source_context

        if self.source_created_time is not None or write_none:
            properties["sourceCreatedTime"] = (
                self.source_created_time.isoformat(timespec="milliseconds") if self.source_created_time else None
            )

        if self.source_created_user is not None or write_none:
            properties["sourceCreatedUser"] = self.source_created_user

        if self.source_id is not None or write_none:
            properties["sourceId"] = self.source_id

        if self.source_updated_time is not None or write_none:
            properties["sourceUpdatedTime"] = (
                self.source_updated_time.isoformat(timespec="milliseconds") if self.source_updated_time else None
            )

        if self.source_updated_user is not None or write_none:
            properties["sourceUpdatedUser"] = self.source_updated_user

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

        if isinstance(self.source, DomainModelWrite):
            other_resources = self.source._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class CogniteSourceableNodeApply(CogniteSourceableNodeWrite):
    def __new__(cls, *args, **kwargs) -> CogniteSourceableNodeApply:
        warnings.warn(
            "CogniteSourceableNodeApply is deprecated and will be removed in v1.0. Use CogniteSourceableNodeWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "CogniteSourceableNode.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class CogniteSourceableNodeList(DomainModelList[CogniteSourceableNode]):
    """List of Cognite sourceable nodes in the read version."""

    _INSTANCE = CogniteSourceableNode

    def as_write(self) -> CogniteSourceableNodeWriteList:
        """Convert these read versions of Cognite sourceable node to the writing versions."""
        return CogniteSourceableNodeWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> CogniteSourceableNodeWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def source(self) -> CogniteSourceSystemList:
        from ._cognite_source_system import CogniteSourceSystem, CogniteSourceSystemList

        return CogniteSourceSystemList(
            [item.source for item in self.data if isinstance(item.source, CogniteSourceSystem)]
        )


class CogniteSourceableNodeWriteList(DomainModelWriteList[CogniteSourceableNodeWrite]):
    """List of Cognite sourceable nodes in the writing version."""

    _INSTANCE = CogniteSourceableNodeWrite

    @property
    def source(self) -> CogniteSourceSystemWriteList:
        from ._cognite_source_system import CogniteSourceSystemWrite, CogniteSourceSystemWriteList

        return CogniteSourceSystemWriteList(
            [item.source for item in self.data if isinstance(item.source, CogniteSourceSystemWrite)]
        )


class CogniteSourceableNodeApplyList(CogniteSourceableNodeWriteList): ...


def _create_cognite_sourceable_node_filter(
    view_id: dm.ViewId,
    source: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    source_context: str | list[str] | None = None,
    source_context_prefix: str | None = None,
    min_source_created_time: datetime.datetime | None = None,
    max_source_created_time: datetime.datetime | None = None,
    source_created_user: str | list[str] | None = None,
    source_created_user_prefix: str | None = None,
    source_id: str | list[str] | None = None,
    source_id_prefix: str | None = None,
    min_source_updated_time: datetime.datetime | None = None,
    max_source_updated_time: datetime.datetime | None = None,
    source_updated_user: str | list[str] | None = None,
    source_updated_user_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(source, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(source):
        filters.append(dm.filters.Equals(view_id.as_property_ref("source"), value=as_instance_dict_id(source)))
    if source and isinstance(source, Sequence) and not isinstance(source, str) and not is_tuple_id(source):
        filters.append(
            dm.filters.In(view_id.as_property_ref("source"), values=[as_instance_dict_id(item) for item in source])
        )
    if isinstance(source_context, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("sourceContext"), value=source_context))
    if source_context and isinstance(source_context, list):
        filters.append(dm.filters.In(view_id.as_property_ref("sourceContext"), values=source_context))
    if source_context_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("sourceContext"), value=source_context_prefix))
    if min_source_created_time is not None or max_source_created_time is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("sourceCreatedTime"),
                gte=min_source_created_time.isoformat(timespec="milliseconds") if min_source_created_time else None,
                lte=max_source_created_time.isoformat(timespec="milliseconds") if max_source_created_time else None,
            )
        )
    if isinstance(source_created_user, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("sourceCreatedUser"), value=source_created_user))
    if source_created_user and isinstance(source_created_user, list):
        filters.append(dm.filters.In(view_id.as_property_ref("sourceCreatedUser"), values=source_created_user))
    if source_created_user_prefix is not None:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("sourceCreatedUser"), value=source_created_user_prefix)
        )
    if isinstance(source_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("sourceId"), value=source_id))
    if source_id and isinstance(source_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("sourceId"), values=source_id))
    if source_id_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("sourceId"), value=source_id_prefix))
    if min_source_updated_time is not None or max_source_updated_time is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("sourceUpdatedTime"),
                gte=min_source_updated_time.isoformat(timespec="milliseconds") if min_source_updated_time else None,
                lte=max_source_updated_time.isoformat(timespec="milliseconds") if max_source_updated_time else None,
            )
        )
    if isinstance(source_updated_user, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("sourceUpdatedUser"), value=source_updated_user))
    if source_updated_user and isinstance(source_updated_user, list):
        filters.append(dm.filters.In(view_id.as_property_ref("sourceUpdatedUser"), values=source_updated_user))
    if source_updated_user_prefix is not None:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("sourceUpdatedUser"), value=source_updated_user_prefix)
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


class _CogniteSourceableNodeQuery(NodeQueryCore[T_DomainModelList, CogniteSourceableNodeList]):
    _view_id = CogniteSourceableNode._view_id
    _result_cls = CogniteSourceableNode
    _result_list_cls_end = CogniteSourceableNodeList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
    ):
        from ._cognite_source_system import _CogniteSourceSystemQuery

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
            connection_type,
            reverse_expression,
        )

        if _CogniteSourceSystemQuery not in created_types:
            self.source = _CogniteSourceSystemQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("source"),
                    direction="outwards",
                ),
                connection_name="source",
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.source_context = StringFilter(self, self._view_id.as_property_ref("sourceContext"))
        self.source_created_time = TimestampFilter(self, self._view_id.as_property_ref("sourceCreatedTime"))
        self.source_created_user = StringFilter(self, self._view_id.as_property_ref("sourceCreatedUser"))
        self.source_id = StringFilter(self, self._view_id.as_property_ref("sourceId"))
        self.source_updated_time = TimestampFilter(self, self._view_id.as_property_ref("sourceUpdatedTime"))
        self.source_updated_user = StringFilter(self, self._view_id.as_property_ref("sourceUpdatedUser"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.source_context,
                self.source_created_time,
                self.source_created_user,
                self.source_id,
                self.source_updated_time,
                self.source_updated_user,
            ]
        )

    def list_cognite_sourceable_node(self, limit: int = DEFAULT_QUERY_LIMIT) -> CogniteSourceableNodeList:
        return self._list(limit=limit)


class CogniteSourceableNodeQuery(_CogniteSourceableNodeQuery[CogniteSourceableNodeList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CogniteSourceableNodeList)
