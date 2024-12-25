from __future__ import annotations

import warnings
from typing import Any, ClassVar, Literal, Optional, Union, no_type_check

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from pydantic import Field, model_validator

from cognite_core.data_classes._cognite_describable_node import CogniteDescribableNode, CogniteDescribableNodeWrite
from cognite_core.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModelList,
    DomainModelWriteList,
    GraphQLCore,
    NodeQueryCore,
    QueryCore,
    ResourcesWrite,
    StringFilter,
    T_DomainModelList,
    as_direct_relation_reference,
)

__all__ = [
    "Cognite360ImageStation",
    "Cognite360ImageStationWrite",
    "Cognite360ImageStationApply",
    "Cognite360ImageStationList",
    "Cognite360ImageStationWriteList",
    "Cognite360ImageStationApplyList",
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> Cognite360ImageStation:
        """Convert this GraphQL format of Cognite 360 image station to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Cognite360ImageStation(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            aliases=self.aliases,
            description=self.description,
            group_type=self.group_type,
            name=self.name,
            tags=self.tags,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> Cognite360ImageStationWrite:
        """Convert this GraphQL format of Cognite 360 image station to the writing format."""
        return Cognite360ImageStationWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            aliases=self.aliases,
            description=self.description,
            group_type=self.group_type,
            name=self.name,
            tags=self.tags,
        )


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
    group_type: Optional[Literal["Station360"]] = Field(None, alias="groupType")

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> Cognite360ImageStationWrite:
        """Convert this read version of Cognite 360 image station to the writing version."""
        return Cognite360ImageStationWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            aliases=self.aliases,
            description=self.description,
            group_type=self.group_type,
            name=self.name,
            tags=self.tags,
        )

    def as_apply(self) -> Cognite360ImageStationWrite:
        """Convert this read version of Cognite 360 image station to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "Cognite360ImageStation", "v1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    group_type: Optional[Literal["Station360"]] = Field(None, alias="groupType")

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

        if self.aliases is not None or write_none:
            properties["aliases"] = self.aliases

        if self.description is not None or write_none:
            properties["description"] = self.description

        if self.group_type is not None or write_none:
            properties["groupType"] = self.group_type

        if self.name is not None or write_none:
            properties["name"] = self.name

        if self.tags is not None or write_none:
            properties["tags"] = self.tags

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


class Cognite360ImageStationApply(Cognite360ImageStationWrite):
    def __new__(cls, *args, **kwargs) -> Cognite360ImageStationApply:
        warnings.warn(
            "Cognite360ImageStationApply is deprecated and will be removed in v1.0. Use Cognite360ImageStationWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Cognite360ImageStation.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class Cognite360ImageStationList(DomainModelList[Cognite360ImageStation]):
    """List of Cognite 360 image stations in the read version."""

    _INSTANCE = Cognite360ImageStation

    def as_write(self) -> Cognite360ImageStationWriteList:
        """Convert these read versions of Cognite 360 image station to the writing versions."""
        return Cognite360ImageStationWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> Cognite360ImageStationWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class Cognite360ImageStationWriteList(DomainModelWriteList[Cognite360ImageStationWrite]):
    """List of Cognite 360 image stations in the writing version."""

    _INSTANCE = Cognite360ImageStationWrite


class Cognite360ImageStationApplyList(Cognite360ImageStationWriteList): ...


def _create_cognite_360_image_station_filter(
    view_id: dm.ViewId,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
    ):
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
