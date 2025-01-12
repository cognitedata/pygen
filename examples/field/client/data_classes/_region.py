from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

from field.client.data_classes._core import (
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
)

if TYPE_CHECKING:
    from field.client.data_classes._area import Area, AreaList, AreaGraphQL, AreaWrite, AreaWriteList


__all__ = [
    "Region",
    "RegionWrite",
    "RegionApply",
    "RegionList",
    "RegionWriteList",
    "RegionApplyList",
    "RegionGraphQL",
]


RegionTextFields = Literal["external_id",]
RegionFields = Literal["external_id",]

_REGION_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
}


class RegionGraphQL(GraphQLCore):
    """This represents the reading version of region, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the region.
        data_record: The data record of the region node.
        area: The area field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("fields-space", "Region", "841accf72d9b06")
    area: Optional[AreaGraphQL] = Field(default=None, repr=False)

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

    @field_validator("area", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> Region:
        """Convert this GraphQL format of region to the reading format."""
        return Region.model_validate(as_read_args(self))

    def as_write(self) -> RegionWrite:
        """Convert this GraphQL format of region to the writing format."""
        return RegionWrite.model_validate(as_write_args(self))


class Region(DomainModel):
    """This represents the reading version of region.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the region.
        data_record: The data record of the region node.
        area: The area field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("fields-space", "Region", "841accf72d9b06")

    space: str
    node_type: Union[dm.DirectRelationReference, None] = None
    area: Union[Area, dm.NodeId, None] = Field(default=None, repr=False)

    @field_validator("area", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    def as_write(self) -> RegionWrite:
        """Convert this read version of region to the writing version."""
        return RegionWrite.model_validate(as_write_args(self))

    def as_apply(self) -> RegionWrite:
        """Convert this read version of region to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class RegionWrite(DomainModelWrite):
    """This represents the writing version of region.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the region.
        data_record: The data record of the region node.
        area: The area field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = ("area",)
    _direct_relations: ClassVar[tuple[str, ...]] = ("area",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("fields-space", "Region", "841accf72d9b06")

    space: str
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    area: Union[AreaWrite, dm.NodeId, None] = Field(default=None, repr=False)

    @field_validator("area", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class RegionApply(RegionWrite):
    def __new__(cls, *args, **kwargs) -> RegionApply:
        warnings.warn(
            "RegionApply is deprecated and will be removed in v1.0. "
            "Use RegionWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Region.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class RegionList(DomainModelList[Region]):
    """List of regions in the read version."""

    _INSTANCE = Region

    def as_write(self) -> RegionWriteList:
        """Convert these read versions of region to the writing versions."""
        return RegionWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> RegionWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def area(self) -> AreaList:
        from ._area import Area, AreaList

        return AreaList([item.area for item in self.data if isinstance(item.area, Area)])


class RegionWriteList(DomainModelWriteList[RegionWrite]):
    """List of regions in the writing version."""

    _INSTANCE = RegionWrite

    @property
    def area(self) -> AreaWriteList:
        from ._area import AreaWrite, AreaWriteList

        return AreaWriteList([item.area for item in self.data if isinstance(item.area, AreaWrite)])


class RegionApplyList(RegionWriteList): ...


def _create_region_filter(
    view_id: dm.ViewId,
    area: (
        tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(area, dm.NodeId | dm.DirectRelationReference) or is_tuple_id(area):
        filters.append(dm.filters.Equals(view_id.as_property_ref("area"), value=as_instance_dict_id(area)))
    if area and isinstance(area, Sequence) and not isinstance(area, str) and not is_tuple_id(area):
        filters.append(
            dm.filters.In(view_id.as_property_ref("area"), values=[as_instance_dict_id(item) for item in area])
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


class _RegionQuery(NodeQueryCore[T_DomainModelList, RegionList]):
    _view_id = Region._view_id
    _result_cls = Region
    _result_list_cls_end = RegionList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
    ):
        from ._area import _AreaQuery

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

        if _AreaQuery not in created_types:
            self.area = _AreaQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("area"),
                    direction="outwards",
                ),
                connection_name="area",
                connection_property=ViewPropertyId(self._view_id, "area"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.area_filter = DirectRelationFilter(self, self._view_id.as_property_ref("area"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.area_filter,
            ]
        )

    def list_region(self, limit: int = DEFAULT_QUERY_LIMIT) -> RegionList:
        return self._list(limit=limit)


class RegionQuery(_RegionQuery[RegionList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, RegionList)
