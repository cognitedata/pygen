from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

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
)
from cognite_core.data_classes._cognite_describable_node import CogniteDescribableNode, CogniteDescribableNodeWrite


__all__ = [
    "Cognite360ImageStation",
    "Cognite360ImageStationWrite",
    "Cognite360ImageStationList",
    "Cognite360ImageStationWriteList",
    "Cognite360ImageStationFields",
    "Cognite360ImageStationTextFields",
    "Cognite360ImageStationGraphQL",
]


Cognite360ImageStationTextFields = Literal["external_id", "aliases", "description", "name", "tags"]
Cognite360ImageStationFields = Literal["external_id", "aliases", "description", "group_type", "name", "tags"]

_COGNITE360IMAGESTATION_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aliases": "aliases",
    "description": "description",
    "group_type": "groupType",
    "name": "name",
    "tags": "tags",
}


class Cognite360ImageStationGraphQL(GraphQLCore):
    """This represents the reading version of Cognite 360 image station, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image station.
        data_record: The data record of the Cognite 360 image station node.
        aliases: Alternative names for the node
        description: Description of the instance
        group_type: Type of group
        name: Name of the instance
        tags: Text based labels for generic use, limited to 1000
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite360ImageStation", "v1")
    aliases: Optional[list[str]] = None
    description: Optional[str] = None
    group_type: Optional[Literal["Station360"]] = Field(None, alias="groupType")
    name: Optional[str] = None
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

    def as_read(self) -> Cognite360ImageStation:
        """Convert this GraphQL format of Cognite 360 image station to the reading format."""
        return Cognite360ImageStation.model_validate(as_read_args(self))

    def as_write(self) -> Cognite360ImageStationWrite:
        """Convert this GraphQL format of Cognite 360 image station to the writing format."""
        return Cognite360ImageStationWrite.model_validate(as_write_args(self))


class Cognite360ImageStation(CogniteDescribableNode):
    """This represents the reading version of Cognite 360 image station.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image station.
        data_record: The data record of the Cognite 360 image station node.
        aliases: Alternative names for the node
        description: Description of the instance
        group_type: Type of group
        name: Name of the instance
        tags: Text based labels for generic use, limited to 1000
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite360ImageStation", "v1")

    node_type: Union[dm.DirectRelationReference, None] = None
    group_type: Optional[Literal["Station360"]] | str = Field(None, alias="groupType")

    def as_write(self) -> Cognite360ImageStationWrite:
        """Convert this read version of Cognite 360 image station to the writing version."""
        return Cognite360ImageStationWrite.model_validate(as_write_args(self))


class Cognite360ImageStationWrite(CogniteDescribableNodeWrite):
    """This represents the writing version of Cognite 360 image station.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite 360 image station.
        data_record: The data record of the Cognite 360 image station node.
        aliases: Alternative names for the node
        description: Description of the instance
        group_type: Type of group
        name: Name of the instance
        tags: Text based labels for generic use, limited to 1000
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "aliases",
        "description",
        "group_type",
        "name",
        "tags",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite360ImageStation", "v1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    group_type: Optional[Literal["Station360"]] = Field(None, alias="groupType")


class Cognite360ImageStationList(DomainModelList[Cognite360ImageStation]):
    """List of Cognite 360 image stations in the read version."""

    _INSTANCE = Cognite360ImageStation

    def as_write(self) -> Cognite360ImageStationWriteList:
        """Convert these read versions of Cognite 360 image station to the writing versions."""
        return Cognite360ImageStationWriteList([node.as_write() for node in self.data])


class Cognite360ImageStationWriteList(DomainModelWriteList[Cognite360ImageStationWrite]):
    """List of Cognite 360 image stations in the writing version."""

    _INSTANCE = Cognite360ImageStationWrite


def _create_cognite_360_image_station_filter(
    view_id: dm.ViewId,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    group_type: Literal["Station360"] | list[Literal["Station360"]] | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
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
    if isinstance(group_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("groupType"), value=group_type))
    if group_type and isinstance(group_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("groupType"), values=group_type))
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _Cognite360ImageStationQuery(NodeQueryCore[T_DomainModelList, Cognite360ImageStationList]):
    _view_id = Cognite360ImageStation._view_id
    _result_cls = Cognite360ImageStation
    _result_list_cls_end = Cognite360ImageStationList

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

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.description,
                self.name,
            ]
        )

    def list_cognite_360_image_station(self, limit: int = DEFAULT_QUERY_LIMIT) -> Cognite360ImageStationList:
        return self._list(limit=limit)


class Cognite360ImageStationQuery(_Cognite360ImageStationQuery[Cognite360ImageStationList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, Cognite360ImageStationList)
