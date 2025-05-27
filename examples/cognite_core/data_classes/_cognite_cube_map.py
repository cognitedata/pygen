from __future__ import annotations

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
)

if TYPE_CHECKING:
    from cognite_core.data_classes._cognite_file import (
        CogniteFile,
        CogniteFileList,
        CogniteFileGraphQL,
        CogniteFileWrite,
        CogniteFileWriteList,
    )


__all__ = [
    "CogniteCubeMap",
    "CogniteCubeMapWrite",
    "CogniteCubeMapList",
    "CogniteCubeMapWriteList",
    "CogniteCubeMapGraphQL",
]


CogniteCubeMapTextFields = Literal["external_id",]
CogniteCubeMapFields = Literal["external_id",]

_COGNITECUBEMAP_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
}


class CogniteCubeMapGraphQL(GraphQLCore):
    """This represents the reading version of Cognite cube map, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite cube map.
        data_record: The data record of the Cognite cube map node.
        back: Direct relation to a file holding the back projection of the cube map
        bottom: Direct relation to a file holding the bottom projection of the cube map
        front: Direct relation to a file holding the front projection of the cube map
        left: Direct relation to a file holding the left projection of the cube map
        right: Direct relation to a file holding the right projection of the cube map
        top: Direct relation to a file holding the top projection of the cube map
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteCubeMap", "v1")
    back: Optional[CogniteFileGraphQL] = Field(default=None, repr=False)
    bottom: Optional[CogniteFileGraphQL] = Field(default=None, repr=False)
    front: Optional[CogniteFileGraphQL] = Field(default=None, repr=False)
    left: Optional[CogniteFileGraphQL] = Field(default=None, repr=False)
    right: Optional[CogniteFileGraphQL] = Field(default=None, repr=False)
    top: Optional[CogniteFileGraphQL] = Field(default=None, repr=False)

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

    @field_validator("back", "bottom", "front", "left", "right", "top", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> CogniteCubeMap:
        """Convert this GraphQL format of Cognite cube map to the reading format."""
        return CogniteCubeMap.model_validate(as_read_args(self))

    def as_write(self) -> CogniteCubeMapWrite:
        """Convert this GraphQL format of Cognite cube map to the writing format."""
        return CogniteCubeMapWrite.model_validate(as_write_args(self))


class CogniteCubeMap(DomainModel):
    """This represents the reading version of Cognite cube map.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite cube map.
        data_record: The data record of the Cognite cube map node.
        back: Direct relation to a file holding the back projection of the cube map
        bottom: Direct relation to a file holding the bottom projection of the cube map
        front: Direct relation to a file holding the front projection of the cube map
        left: Direct relation to a file holding the left projection of the cube map
        right: Direct relation to a file holding the right projection of the cube map
        top: Direct relation to a file holding the top projection of the cube map
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteCubeMap", "v1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    back: Union[CogniteFile, str, dm.NodeId, None] = Field(default=None, repr=False)
    bottom: Union[CogniteFile, str, dm.NodeId, None] = Field(default=None, repr=False)
    front: Union[CogniteFile, str, dm.NodeId, None] = Field(default=None, repr=False)
    left: Union[CogniteFile, str, dm.NodeId, None] = Field(default=None, repr=False)
    right: Union[CogniteFile, str, dm.NodeId, None] = Field(default=None, repr=False)
    top: Union[CogniteFile, str, dm.NodeId, None] = Field(default=None, repr=False)

    @field_validator("back", "bottom", "front", "left", "right", "top", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    def as_write(self) -> CogniteCubeMapWrite:
        """Convert this read version of Cognite cube map to the writing version."""
        return CogniteCubeMapWrite.model_validate(as_write_args(self))


class CogniteCubeMapWrite(DomainModelWrite):
    """This represents the writing version of Cognite cube map.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite cube map.
        data_record: The data record of the Cognite cube map node.
        back: Direct relation to a file holding the back projection of the cube map
        bottom: Direct relation to a file holding the bottom projection of the cube map
        front: Direct relation to a file holding the front projection of the cube map
        left: Direct relation to a file holding the left projection of the cube map
        right: Direct relation to a file holding the right projection of the cube map
        top: Direct relation to a file holding the top projection of the cube map
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "back",
        "bottom",
        "front",
        "left",
        "right",
        "top",
    )
    _direct_relations: ClassVar[tuple[str, ...]] = (
        "back",
        "bottom",
        "front",
        "left",
        "right",
        "top",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteCubeMap", "v1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    back: Union[CogniteFileWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    bottom: Union[CogniteFileWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    front: Union[CogniteFileWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    left: Union[CogniteFileWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    right: Union[CogniteFileWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    top: Union[CogniteFileWrite, str, dm.NodeId, None] = Field(default=None, repr=False)

    @field_validator("back", "bottom", "front", "left", "right", "top", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class CogniteCubeMapList(DomainModelList[CogniteCubeMap]):
    """List of Cognite cube maps in the read version."""

    _INSTANCE = CogniteCubeMap

    def as_write(self) -> CogniteCubeMapWriteList:
        """Convert these read versions of Cognite cube map to the writing versions."""
        return CogniteCubeMapWriteList([node.as_write() for node in self.data])

    @property
    def back(self) -> CogniteFileList:
        from ._cognite_file import CogniteFile, CogniteFileList

        return CogniteFileList([item.back for item in self.data if isinstance(item.back, CogniteFile)])

    @property
    def bottom(self) -> CogniteFileList:
        from ._cognite_file import CogniteFile, CogniteFileList

        return CogniteFileList([item.bottom for item in self.data if isinstance(item.bottom, CogniteFile)])

    @property
    def front(self) -> CogniteFileList:
        from ._cognite_file import CogniteFile, CogniteFileList

        return CogniteFileList([item.front for item in self.data if isinstance(item.front, CogniteFile)])

    @property
    def left(self) -> CogniteFileList:
        from ._cognite_file import CogniteFile, CogniteFileList

        return CogniteFileList([item.left for item in self.data if isinstance(item.left, CogniteFile)])

    @property
    def right(self) -> CogniteFileList:
        from ._cognite_file import CogniteFile, CogniteFileList

        return CogniteFileList([item.right for item in self.data if isinstance(item.right, CogniteFile)])

    @property
    def top(self) -> CogniteFileList:
        from ._cognite_file import CogniteFile, CogniteFileList

        return CogniteFileList([item.top for item in self.data if isinstance(item.top, CogniteFile)])


class CogniteCubeMapWriteList(DomainModelWriteList[CogniteCubeMapWrite]):
    """List of Cognite cube maps in the writing version."""

    _INSTANCE = CogniteCubeMapWrite

    @property
    def back(self) -> CogniteFileWriteList:
        from ._cognite_file import CogniteFileWrite, CogniteFileWriteList

        return CogniteFileWriteList([item.back for item in self.data if isinstance(item.back, CogniteFileWrite)])

    @property
    def bottom(self) -> CogniteFileWriteList:
        from ._cognite_file import CogniteFileWrite, CogniteFileWriteList

        return CogniteFileWriteList([item.bottom for item in self.data if isinstance(item.bottom, CogniteFileWrite)])

    @property
    def front(self) -> CogniteFileWriteList:
        from ._cognite_file import CogniteFileWrite, CogniteFileWriteList

        return CogniteFileWriteList([item.front for item in self.data if isinstance(item.front, CogniteFileWrite)])

    @property
    def left(self) -> CogniteFileWriteList:
        from ._cognite_file import CogniteFileWrite, CogniteFileWriteList

        return CogniteFileWriteList([item.left for item in self.data if isinstance(item.left, CogniteFileWrite)])

    @property
    def right(self) -> CogniteFileWriteList:
        from ._cognite_file import CogniteFileWrite, CogniteFileWriteList

        return CogniteFileWriteList([item.right for item in self.data if isinstance(item.right, CogniteFileWrite)])

    @property
    def top(self) -> CogniteFileWriteList:
        from ._cognite_file import CogniteFileWrite, CogniteFileWriteList

        return CogniteFileWriteList([item.top for item in self.data if isinstance(item.top, CogniteFileWrite)])


def _create_cognite_cube_map_filter(
    view_id: dm.ViewId,
    back: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    bottom: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    front: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    left: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    right: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    top: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(back, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(back):
        filters.append(dm.filters.Equals(view_id.as_property_ref("back"), value=as_instance_dict_id(back)))
    if back and isinstance(back, Sequence) and not isinstance(back, str) and not is_tuple_id(back):
        filters.append(
            dm.filters.In(view_id.as_property_ref("back"), values=[as_instance_dict_id(item) for item in back])
        )
    if isinstance(bottom, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(bottom):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bottom"), value=as_instance_dict_id(bottom)))
    if bottom and isinstance(bottom, Sequence) and not isinstance(bottom, str) and not is_tuple_id(bottom):
        filters.append(
            dm.filters.In(view_id.as_property_ref("bottom"), values=[as_instance_dict_id(item) for item in bottom])
        )
    if isinstance(front, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(front):
        filters.append(dm.filters.Equals(view_id.as_property_ref("front"), value=as_instance_dict_id(front)))
    if front and isinstance(front, Sequence) and not isinstance(front, str) and not is_tuple_id(front):
        filters.append(
            dm.filters.In(view_id.as_property_ref("front"), values=[as_instance_dict_id(item) for item in front])
        )
    if isinstance(left, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(left):
        filters.append(dm.filters.Equals(view_id.as_property_ref("left"), value=as_instance_dict_id(left)))
    if left and isinstance(left, Sequence) and not isinstance(left, str) and not is_tuple_id(left):
        filters.append(
            dm.filters.In(view_id.as_property_ref("left"), values=[as_instance_dict_id(item) for item in left])
        )
    if isinstance(right, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(right):
        filters.append(dm.filters.Equals(view_id.as_property_ref("right"), value=as_instance_dict_id(right)))
    if right and isinstance(right, Sequence) and not isinstance(right, str) and not is_tuple_id(right):
        filters.append(
            dm.filters.In(view_id.as_property_ref("right"), values=[as_instance_dict_id(item) for item in right])
        )
    if isinstance(top, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(top):
        filters.append(dm.filters.Equals(view_id.as_property_ref("top"), value=as_instance_dict_id(top)))
    if top and isinstance(top, Sequence) and not isinstance(top, str) and not is_tuple_id(top):
        filters.append(
            dm.filters.In(view_id.as_property_ref("top"), values=[as_instance_dict_id(item) for item in top])
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


class _CogniteCubeMapQuery(NodeQueryCore[T_DomainModelList, CogniteCubeMapList]):
    _view_id = CogniteCubeMap._view_id
    _result_cls = CogniteCubeMap
    _result_list_cls_end = CogniteCubeMapList

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
        from ._cognite_file import _CogniteFileQuery

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

        if _CogniteFileQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.back = _CogniteFileQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("back"),
                    direction="outwards",
                ),
                connection_name="back",
                connection_property=ViewPropertyId(self._view_id, "back"),
            )

        if _CogniteFileQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.bottom = _CogniteFileQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("bottom"),
                    direction="outwards",
                ),
                connection_name="bottom",
                connection_property=ViewPropertyId(self._view_id, "bottom"),
            )

        if _CogniteFileQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.front = _CogniteFileQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("front"),
                    direction="outwards",
                ),
                connection_name="front",
                connection_property=ViewPropertyId(self._view_id, "front"),
            )

        if _CogniteFileQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.left = _CogniteFileQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("left"),
                    direction="outwards",
                ),
                connection_name="left",
                connection_property=ViewPropertyId(self._view_id, "left"),
            )

        if _CogniteFileQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.right = _CogniteFileQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("right"),
                    direction="outwards",
                ),
                connection_name="right",
                connection_property=ViewPropertyId(self._view_id, "right"),
            )

        if _CogniteFileQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.top = _CogniteFileQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("top"),
                    direction="outwards",
                ),
                connection_name="top",
                connection_property=ViewPropertyId(self._view_id, "top"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.back_filter = DirectRelationFilter(self, self._view_id.as_property_ref("back"))
        self.bottom_filter = DirectRelationFilter(self, self._view_id.as_property_ref("bottom"))
        self.front_filter = DirectRelationFilter(self, self._view_id.as_property_ref("front"))
        self.left_filter = DirectRelationFilter(self, self._view_id.as_property_ref("left"))
        self.right_filter = DirectRelationFilter(self, self._view_id.as_property_ref("right"))
        self.top_filter = DirectRelationFilter(self, self._view_id.as_property_ref("top"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.back_filter,
                self.bottom_filter,
                self.front_filter,
                self.left_filter,
                self.right_filter,
                self.top_filter,
            ]
        )

    def list_cognite_cube_map(self, limit: int = DEFAULT_QUERY_LIMIT) -> CogniteCubeMapList:
        return self._list(limit=limit)


class CogniteCubeMapQuery(_CogniteCubeMapQuery[CogniteCubeMapList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CogniteCubeMapList)
