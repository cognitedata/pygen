from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

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

if TYPE_CHECKING:
    from ._connection_edge_a import ConnectionEdgeA, ConnectionEdgeAGraphQL, ConnectionEdgeAWrite
    from ._connection_item_d import ConnectionItemD, ConnectionItemDGraphQL, ConnectionItemDWrite


__all__ = [
    "ConnectionItemF",
    "ConnectionItemFWrite",
    "ConnectionItemFApply",
    "ConnectionItemFList",
    "ConnectionItemFWriteList",
    "ConnectionItemFApplyList",
    "ConnectionItemFFields",
    "ConnectionItemFTextFields",
    "ConnectionItemFGraphQL",
]


ConnectionItemFTextFields = Literal["name"]
ConnectionItemFFields = Literal["name"]

_CONNECTIONITEMF_PROPERTIES_BY_FIELD = {
    "name": "name",
}


class ConnectionItemFGraphQL(GraphQLCore):
    """This represents the reading version of connection item f, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item f.
        data_record: The data record of the connection item f node.
        direct_list: The direct list field.
        name: The name field.
        outwards_multi: The outwards multi field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "ConnectionItemF", "1")
    direct_list: Optional[list[ConnectionItemDGraphQL]] = Field(default=None, repr=False, alias="directList")
    name: Optional[str] = None
    outwards_multi: Optional[list[ConnectionEdgeAGraphQL]] = Field(default=None, repr=False, alias="outwardsMulti")

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

    @field_validator("direct_list", "outwards_multi", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> ConnectionItemF:
        """Convert this GraphQL format of connection item f to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ConnectionItemF(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            direct_list=[direct_list.as_read() for direct_list in self.direct_list or []],
            name=self.name,
            outwards_multi=[outwards_multi.as_read() for outwards_multi in self.outwards_multi or []],
        )

    def as_write(self) -> ConnectionItemFWrite:
        """Convert this GraphQL format of connection item f to the writing format."""
        return ConnectionItemFWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            direct_list=[direct_list.as_write() for direct_list in self.direct_list or []],
            name=self.name,
            outwards_multi=[outwards_multi.as_write() for outwards_multi in self.outwards_multi or []],
        )


class ConnectionItemF(DomainModel):
    """This represents the reading version of connection item f.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item f.
        data_record: The data record of the connection item f node.
        direct_list: The direct list field.
        name: The name field.
        outwards_multi: The outwards multi field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "ConnectionItemF", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "ConnectionItemF")
    direct_list: Union[list[ConnectionItemD], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="directList"
    )
    name: Optional[str] = None
    outwards_multi: Optional[list[ConnectionEdgeA]] = Field(default=None, repr=False, alias="outwardsMulti")

    def as_write(self) -> ConnectionItemFWrite:
        """Convert this read version of connection item f to the writing version."""
        return ConnectionItemFWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            direct_list=[
                direct_list.as_write() if isinstance(direct_list, DomainModel) else direct_list
                for direct_list in self.direct_list or []
            ],
            name=self.name,
            outwards_multi=[outwards_multi.as_write() for outwards_multi in self.outwards_multi or []],
        )

    def as_apply(self) -> ConnectionItemFWrite:
        """Convert this read version of connection item f to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ConnectionItemFWrite(DomainModelWrite):
    """This represents the writing version of connection item f.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the connection item f.
        data_record: The data record of the connection item f node.
        direct_list: The direct list field.
        name: The name field.
        outwards_multi: The outwards multi field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "ConnectionItemF", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "ConnectionItemF")
    direct_list: Union[list[ConnectionItemDWrite], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="directList"
    )
    name: Optional[str] = None
    outwards_multi: Optional[list[ConnectionEdgeAWrite]] = Field(default=None, repr=False, alias="outwardsMulti")

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

        if self.direct_list is not None:
            properties["directList"] = [
                {
                    "space": self.space if isinstance(direct_list, str) else direct_list.space,
                    "externalId": direct_list if isinstance(direct_list, str) else direct_list.external_id,
                }
                for direct_list in self.direct_list or []
            ]

        if self.name is not None or write_none:
            properties["name"] = self.name

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

        for outwards_multi in self.outwards_multi or []:
            if isinstance(outwards_multi, DomainRelationWrite):
                other_resources = outwards_multi._to_instances_write(
                    cache,
                    self,
                    dm.DirectRelationReference("pygen-models", "multiProperty"),
                )
                resources.extend(other_resources)

        for direct_list in self.direct_list or []:
            if isinstance(direct_list, DomainModelWrite):
                other_resources = direct_list._to_instances_write(cache)
                resources.extend(other_resources)

        return resources


class ConnectionItemFApply(ConnectionItemFWrite):
    def __new__(cls, *args, **kwargs) -> ConnectionItemFApply:
        warnings.warn(
            "ConnectionItemFApply is deprecated and will be removed in v1.0. Use ConnectionItemFWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ConnectionItemF.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ConnectionItemFList(DomainModelList[ConnectionItemF]):
    """List of connection item fs in the read version."""

    _INSTANCE = ConnectionItemF

    def as_write(self) -> ConnectionItemFWriteList:
        """Convert these read versions of connection item f to the writing versions."""
        return ConnectionItemFWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ConnectionItemFWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ConnectionItemFWriteList(DomainModelWriteList[ConnectionItemFWrite]):
    """List of connection item fs in the writing version."""

    _INSTANCE = ConnectionItemFWrite


class ConnectionItemFApplyList(ConnectionItemFWriteList): ...


def _create_connection_item_f_filter(
    view_id: dm.ViewId,
    direct_list: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if direct_list and isinstance(direct_list, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("directList"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": direct_list},
            )
        )
    if direct_list and isinstance(direct_list, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("directList"), value={"space": direct_list[0], "externalId": direct_list[1]}
            )
        )
    if direct_list and isinstance(direct_list, list) and isinstance(direct_list[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("directList"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in direct_list],
            )
        )
    if direct_list and isinstance(direct_list, list) and isinstance(direct_list[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("directList"),
                values=[{"space": item[0], "externalId": item[1]} for item in direct_list],
            )
        )
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
