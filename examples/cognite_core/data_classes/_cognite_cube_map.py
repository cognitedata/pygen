from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

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
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    is_tuple_id,
    select_best_node,
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    ViewPropertyId,
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
    "CogniteCubeMapApply",
    "CogniteCubeMapList",
    "CogniteCubeMapWriteList",
    "CogniteCubeMapApplyList",
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> CogniteCubeMap:
        """Convert this GraphQL format of Cognite cube map to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return CogniteCubeMap(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            back=self.back.as_read() if isinstance(self.back, GraphQLCore) else self.back,
            bottom=self.bottom.as_read() if isinstance(self.bottom, GraphQLCore) else self.bottom,
            front=self.front.as_read() if isinstance(self.front, GraphQLCore) else self.front,
            left=self.left.as_read() if isinstance(self.left, GraphQLCore) else self.left,
            right=self.right.as_read() if isinstance(self.right, GraphQLCore) else self.right,
            top=self.top.as_read() if isinstance(self.top, GraphQLCore) else self.top,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteCubeMapWrite:
        """Convert this GraphQL format of Cognite cube map to the writing format."""
        return CogniteCubeMapWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            back=self.back.as_write() if isinstance(self.back, GraphQLCore) else self.back,
            bottom=self.bottom.as_write() if isinstance(self.bottom, GraphQLCore) else self.bottom,
            front=self.front.as_write() if isinstance(self.front, GraphQLCore) else self.front,
            left=self.left.as_write() if isinstance(self.left, GraphQLCore) else self.left,
            right=self.right.as_write() if isinstance(self.right, GraphQLCore) else self.right,
            top=self.top.as_write() if isinstance(self.top, GraphQLCore) else self.top,
        )


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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteCubeMapWrite:
        """Convert this read version of Cognite cube map to the writing version."""
        return CogniteCubeMapWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            back=self.back.as_write() if isinstance(self.back, DomainModel) else self.back,
            bottom=self.bottom.as_write() if isinstance(self.bottom, DomainModel) else self.bottom,
            front=self.front.as_write() if isinstance(self.front, DomainModel) else self.front,
            left=self.left.as_write() if isinstance(self.left, DomainModel) else self.left,
            right=self.right.as_write() if isinstance(self.right, DomainModel) else self.right,
            top=self.top.as_write() if isinstance(self.top, DomainModel) else self.top,
        )

    def as_apply(self) -> CogniteCubeMapWrite:
        """Convert this read version of Cognite cube map to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


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

        if self.back is not None:
            properties["back"] = {
                "space": self.space if isinstance(self.back, str) else self.back.space,
                "externalId": self.back if isinstance(self.back, str) else self.back.external_id,
            }

        if self.bottom is not None:
            properties["bottom"] = {
                "space": self.space if isinstance(self.bottom, str) else self.bottom.space,
                "externalId": self.bottom if isinstance(self.bottom, str) else self.bottom.external_id,
            }

        if self.front is not None:
            properties["front"] = {
                "space": self.space if isinstance(self.front, str) else self.front.space,
                "externalId": self.front if isinstance(self.front, str) else self.front.external_id,
            }

        if self.left is not None:
            properties["left"] = {
                "space": self.space if isinstance(self.left, str) else self.left.space,
                "externalId": self.left if isinstance(self.left, str) else self.left.external_id,
            }

        if self.right is not None:
            properties["right"] = {
                "space": self.space if isinstance(self.right, str) else self.right.space,
                "externalId": self.right if isinstance(self.right, str) else self.right.external_id,
            }

        if self.top is not None:
            properties["top"] = {
                "space": self.space if isinstance(self.top, str) else self.top.space,
                "externalId": self.top if isinstance(self.top, str) else self.top.external_id,
            }

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

        if isinstance(self.back, DomainModelWrite):
            other_resources = self.back._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.bottom, DomainModelWrite):
            other_resources = self.bottom._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.front, DomainModelWrite):
            other_resources = self.front._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.left, DomainModelWrite):
            other_resources = self.left._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.right, DomainModelWrite):
            other_resources = self.right._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.top, DomainModelWrite):
            other_resources = self.top._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class CogniteCubeMapApply(CogniteCubeMapWrite):
    def __new__(cls, *args, **kwargs) -> CogniteCubeMapApply:
        warnings.warn(
            "CogniteCubeMapApply is deprecated and will be removed in v1.0. "
            "Use CogniteCubeMapWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "CogniteCubeMap.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class CogniteCubeMapList(DomainModelList[CogniteCubeMap]):
    """List of Cognite cube maps in the read version."""

    _INSTANCE = CogniteCubeMap

    def as_write(self) -> CogniteCubeMapWriteList:
        """Convert these read versions of Cognite cube map to the writing versions."""
        return CogniteCubeMapWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> CogniteCubeMapWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

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


class CogniteCubeMapApplyList(CogniteCubeMapWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
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

        if _CogniteFileQuery not in created_types:
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

        if _CogniteFileQuery not in created_types:
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

        if _CogniteFileQuery not in created_types:
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

        if _CogniteFileQuery not in created_types:
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

        if _CogniteFileQuery not in created_types:
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

        if _CogniteFileQuery not in created_types:
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

    def list_cognite_cube_map(self, limit: int = DEFAULT_QUERY_LIMIT) -> CogniteCubeMapList:
        return self._list(limit=limit)


class CogniteCubeMapQuery(_CogniteCubeMapQuery[CogniteCubeMapList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CogniteCubeMapList)
