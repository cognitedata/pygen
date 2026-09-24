from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

from cognite_core.config import global_config
from cognite_core.data_classes._core import (
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
    as_node_id,
    as_read_args,
    as_write_args,
    is_tuple_id,
    as_instance_dict_id,
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    ViewPropertyId,
    DirectRelationFilter,
    TimestampFilter,
)
from cognite_core.data_classes._cognite_describable_node import CogniteDescribableNode, CogniteDescribableNodeWrite
from cognite_core.data_classes._cognite_sourceable_node import CogniteSourceableNode, CogniteSourceableNodeWrite

if TYPE_CHECKING:
    from cognite_core.data_classes._cognite_source_system import (
        CogniteSourceSystem,
        CogniteSourceSystemList,
        CogniteSourceSystemGraphQL,
        CogniteSourceSystemWrite,
        CogniteSourceSystemWriteList,
    )


__all__ = [
    "CogniteStateSet",
    "CogniteStateSetWrite",
    "CogniteStateSetList",
    "CogniteStateSetWriteList",
    "CogniteStateSetFields",
    "CogniteStateSetTextFields",
    "CogniteStateSetGraphQL",
]


CogniteStateSetTextFields = Literal[
    "external_id",
    "aliases",
    "description",
    "name",
    "source_context",
    "source_created_user",
    "source_id",
    "source_updated_user",
    "tags",
]
CogniteStateSetFields = Literal[
    "external_id",
    "aliases",
    "description",
    "name",
    "source_context",
    "source_created_time",
    "source_created_user",
    "source_id",
    "source_updated_time",
    "source_updated_user",
    "states",
    "tags",
]

_COGNITESTATESET_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aliases": "aliases",
    "description": "description",
    "name": "name",
    "source_context": "sourceContext",
    "source_created_time": "sourceCreatedTime",
    "source_created_user": "sourceCreatedUser",
    "source_id": "sourceId",
    "source_updated_time": "sourceUpdatedTime",
    "source_updated_user": "sourceUpdatedUser",
    "states": "states",
    "tags": "tags",
}


class CogniteStateSetGraphQL(GraphQLCore):
    """This represents the reading version of Cognite state set, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite state set.
        data_record: The data record of the Cognite state set node.
        aliases: Alternative names for the instance.
        description: The description of the instance.
        name: The name of the instance.
        source: A direct relation to a source system instance.
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext
            is expected to not be set.
        source_created_time: The time the instance was created in the source system (if available).
        source_created_user: The user identifier from the source system who created the source data. This identifier is
            not guaranteed to match user identifiers in Cognite Data Fusion.
        source_id: The identifier in the source system.
        source_updated_time: The time the instance was last updated in the source system (if available).
        source_updated_user: The user identifier from the source system who last updated the source data. This
            identifier is not guaranteed to match user identifiers in Cognite Data Fusion.
        states: A list of valid states. Each state contains a numeric value and a string value.
        tags: A list of tags for the instance. Max: 1000.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteStateSet", "v1")
    aliases: Optional[list[str]] = None
    description: Optional[str] = None
    name: Optional[str] = None
    source: Optional[CogniteSourceSystemGraphQL] = Field(default=None, repr=False)
    source_context: Optional[str] = Field(None, alias="sourceContext")
    source_created_time: Optional[datetime.datetime] = Field(None, alias="sourceCreatedTime")
    source_created_user: Optional[str] = Field(None, alias="sourceCreatedUser")
    source_id: Optional[str] = Field(None, alias="sourceId")
    source_updated_time: Optional[datetime.datetime] = Field(None, alias="sourceUpdatedTime")
    source_updated_user: Optional[str] = Field(None, alias="sourceUpdatedUser")
    states: Optional[list[dict]] = None
    tags: Optional[list[str]] = None

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

    def as_read(self) -> CogniteStateSet:
        """Convert this GraphQL format of Cognite state set to the reading format."""
        return CogniteStateSet.model_validate(as_read_args(self))

    def as_write(self) -> CogniteStateSetWrite:
        """Convert this GraphQL format of Cognite state set to the writing format."""
        return CogniteStateSetWrite.model_validate(as_write_args(self))


class CogniteStateSet(CogniteDescribableNode, CogniteSourceableNode):
    """This represents the reading version of Cognite state set.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite state set.
        data_record: The data record of the Cognite state set node.
        aliases: Alternative names for the instance.
        description: The description of the instance.
        name: The name of the instance.
        source: A direct relation to a source system instance.
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext
            is expected to not be set.
        source_created_time: The time the instance was created in the source system (if available).
        source_created_user: The user identifier from the source system who created the source data. This identifier is
            not guaranteed to match user identifiers in Cognite Data Fusion.
        source_id: The identifier in the source system.
        source_updated_time: The time the instance was last updated in the source system (if available).
        source_updated_user: The user identifier from the source system who last updated the source data. This
            identifier is not guaranteed to match user identifiers in Cognite Data Fusion.
        states: A list of valid states. Each state contains a numeric value and a string value.
        tags: A list of tags for the instance. Max: 1000.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteStateSet", "v1")

    node_type: Union[dm.DirectRelationReference, None] = None
    states: list[dict]

    @field_validator("source", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    def as_write(self) -> CogniteStateSetWrite:
        """Convert this read version of Cognite state set to the writing version."""
        return CogniteStateSetWrite.model_validate(as_write_args(self))


class CogniteStateSetWrite(CogniteDescribableNodeWrite, CogniteSourceableNodeWrite):
    """This represents the writing version of Cognite state set.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite state set.
        data_record: The data record of the Cognite state set node.
        aliases: Alternative names for the instance.
        description: The description of the instance.
        name: The name of the instance.
        source: A direct relation to a source system instance.
        source_context: Context of the source id. For systems where the sourceId is globally unique, the sourceContext
            is expected to not be set.
        source_created_time: The time the instance was created in the source system (if available).
        source_created_user: The user identifier from the source system who created the source data. This identifier is
            not guaranteed to match user identifiers in Cognite Data Fusion.
        source_id: The identifier in the source system.
        source_updated_time: The time the instance was last updated in the source system (if available).
        source_updated_user: The user identifier from the source system who last updated the source data. This
            identifier is not guaranteed to match user identifiers in Cognite Data Fusion.
        states: A list of valid states. Each state contains a numeric value and a string value.
        tags: A list of tags for the instance. Max: 1000.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "aliases",
        "description",
        "name",
        "source",
        "source_context",
        "source_created_time",
        "source_created_user",
        "source_id",
        "source_updated_time",
        "source_updated_user",
        "states",
        "tags",
    )
    _direct_relations: ClassVar[tuple[str, ...]] = ("source",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteStateSet", "v1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    states: list[dict]


class CogniteStateSetList(DomainModelList[CogniteStateSet]):
    """List of Cognite state sets in the read version."""

    _INSTANCE = CogniteStateSet

    def as_write(self) -> CogniteStateSetWriteList:
        """Convert these read versions of Cognite state set to the writing versions."""
        return CogniteStateSetWriteList([node.as_write() for node in self.data])

    @property
    def source(self) -> CogniteSourceSystemList:
        from ._cognite_source_system import CogniteSourceSystem, CogniteSourceSystemList

        return CogniteSourceSystemList(
            [item.source for item in self.data if isinstance(item.source, CogniteSourceSystem)]
        )


class CogniteStateSetWriteList(DomainModelWriteList[CogniteStateSetWrite]):
    """List of Cognite state sets in the writing version."""

    _INSTANCE = CogniteStateSetWrite

    @property
    def source(self) -> CogniteSourceSystemWriteList:
        from ._cognite_source_system import CogniteSourceSystemWrite, CogniteSourceSystemWriteList

        return CogniteSourceSystemWriteList(
            [item.source for item in self.data if isinstance(item.source, CogniteSourceSystemWrite)]
        )


def _create_cognite_state_set_filter(
    view_id: dm.ViewId,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
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
    if isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
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


class _CogniteStateSetQuery(NodeQueryCore[T_DomainModelList, CogniteStateSetList]):
    _view_id = CogniteStateSet._view_id
    _result_cls = CogniteStateSet
    _result_list_cls_end = CogniteStateSetList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
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
            connection_property,
            connection_type,
            reverse_expression,
        )

        if _CogniteSourceSystemQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "source"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.source_filter = DirectRelationFilter(self, self._view_id.as_property_ref("source"))
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
                self.description,
                self.name,
                self.source_filter,
                self.source_context,
                self.source_created_time,
                self.source_created_user,
                self.source_id,
                self.source_updated_time,
                self.source_updated_user,
            ]
        )

    def list_cognite_state_set(self, limit: int = DEFAULT_QUERY_LIMIT) -> CogniteStateSetList:
        return self._list(limit=limit)


class CogniteStateSetQuery(_CogniteStateSetQuery[CogniteStateSetList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CogniteStateSetList)
