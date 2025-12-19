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
from cognite_core.data_classes._cognite_describable_node import CogniteDescribableNode, CogniteDescribableNodeWrite

if TYPE_CHECKING:
    from cognite_core.data_classes._cognite_asset_class import (
        CogniteAssetClass,
        CogniteAssetClassList,
        CogniteAssetClassGraphQL,
        CogniteAssetClassWrite,
        CogniteAssetClassWriteList,
    )


__all__ = [
    "CogniteAssetType",
    "CogniteAssetTypeWrite",
    "CogniteAssetTypeList",
    "CogniteAssetTypeWriteList",
    "CogniteAssetTypeFields",
    "CogniteAssetTypeTextFields",
    "CogniteAssetTypeGraphQL",
]


CogniteAssetTypeTextFields = Literal["external_id", "aliases", "code", "description", "name", "standard", "tags"]
CogniteAssetTypeFields = Literal["external_id", "aliases", "code", "description", "name", "standard", "tags"]

_COGNITEASSETTYPE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aliases": "aliases",
    "code": "code",
    "description": "description",
    "name": "name",
    "standard": "standard",
    "tags": "tags",
}


class CogniteAssetTypeGraphQL(GraphQLCore):
    """This represents the reading version of Cognite asset type, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite asset type.
        data_record: The data record of the Cognite asset type node.
        aliases: Alternative names for the node
        asset_class: Specifies the class the type belongs to. It's a direct relation to CogniteAssetClass.
        code: A unique identifier for the type of asset.
        description: Description of the instance
        name: Name of the instance
        standard: A text string to specify which standard the type is from.
        tags: Text based labels for generic use, limited to 1000
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteAssetType", "v1")
    aliases: Optional[list[str]] = None
    asset_class: Optional[CogniteAssetClassGraphQL] = Field(default=None, repr=False, alias="assetClass")
    code: Optional[str] = None
    description: Optional[str] = None
    name: Optional[str] = None
    standard: Optional[str] = None
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

    @field_validator("asset_class", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> CogniteAssetType:
        """Convert this GraphQL format of Cognite asset type to the reading format."""
        return CogniteAssetType.model_validate(as_read_args(self))

    def as_write(self) -> CogniteAssetTypeWrite:
        """Convert this GraphQL format of Cognite asset type to the writing format."""
        return CogniteAssetTypeWrite.model_validate(as_write_args(self))


class CogniteAssetType(CogniteDescribableNode):
    """This represents the reading version of Cognite asset type.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite asset type.
        data_record: The data record of the Cognite asset type node.
        aliases: Alternative names for the node
        asset_class: Specifies the class the type belongs to. It's a direct relation to CogniteAssetClass.
        code: A unique identifier for the type of asset.
        description: Description of the instance
        name: Name of the instance
        standard: A text string to specify which standard the type is from.
        tags: Text based labels for generic use, limited to 1000
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteAssetType", "v1")

    node_type: Union[dm.DirectRelationReference, None] = None
    asset_class: Union[CogniteAssetClass, str, dm.NodeId, None] = Field(default=None, repr=False, alias="assetClass")
    code: Optional[str] = None
    standard: Optional[str] = None

    @field_validator("asset_class", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    def as_write(self) -> CogniteAssetTypeWrite:
        """Convert this read version of Cognite asset type to the writing version."""
        return CogniteAssetTypeWrite.model_validate(as_write_args(self))


class CogniteAssetTypeWrite(CogniteDescribableNodeWrite):
    """This represents the writing version of Cognite asset type.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite asset type.
        data_record: The data record of the Cognite asset type node.
        aliases: Alternative names for the node
        asset_class: Specifies the class the type belongs to. It's a direct relation to CogniteAssetClass.
        code: A unique identifier for the type of asset.
        description: Description of the instance
        name: Name of the instance
        standard: A text string to specify which standard the type is from.
        tags: Text based labels for generic use, limited to 1000
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "aliases",
        "asset_class",
        "code",
        "description",
        "name",
        "standard",
        "tags",
    )
    _direct_relations: ClassVar[tuple[str, ...]] = ("asset_class",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteAssetType", "v1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    asset_class: Union[CogniteAssetClassWrite, str, dm.NodeId, None] = Field(
        default=None, repr=False, alias="assetClass"
    )
    code: Optional[str] = None
    standard: Optional[str] = None

    @field_validator("asset_class", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class CogniteAssetTypeList(DomainModelList[CogniteAssetType]):
    """List of Cognite asset types in the read version."""

    _INSTANCE = CogniteAssetType

    def as_write(self) -> CogniteAssetTypeWriteList:
        """Convert these read versions of Cognite asset type to the writing versions."""
        return CogniteAssetTypeWriteList([node.as_write() for node in self.data])

    @property
    def asset_class(self) -> CogniteAssetClassList:
        from ._cognite_asset_class import CogniteAssetClass, CogniteAssetClassList

        return CogniteAssetClassList(
            [item.asset_class for item in self.data if isinstance(item.asset_class, CogniteAssetClass)]
        )


class CogniteAssetTypeWriteList(DomainModelWriteList[CogniteAssetTypeWrite]):
    """List of Cognite asset types in the writing version."""

    _INSTANCE = CogniteAssetTypeWrite

    @property
    def asset_class(self) -> CogniteAssetClassWriteList:
        from ._cognite_asset_class import CogniteAssetClassWrite, CogniteAssetClassWriteList

        return CogniteAssetClassWriteList(
            [item.asset_class for item in self.data if isinstance(item.asset_class, CogniteAssetClassWrite)]
        )


def _create_cognite_asset_type_filter(
    view_id: dm.ViewId,
    asset_class: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    code: str | list[str] | None = None,
    code_prefix: str | None = None,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    standard: str | list[str] | None = None,
    standard_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(asset_class, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(asset_class):
        filters.append(dm.filters.Equals(view_id.as_property_ref("assetClass"), value=as_instance_dict_id(asset_class)))
    if (
        asset_class
        and isinstance(asset_class, Sequence)
        and not isinstance(asset_class, str)
        and not is_tuple_id(asset_class)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("assetClass"), values=[as_instance_dict_id(item) for item in asset_class]
            )
        )
    if isinstance(code, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("code"), value=code))
    if code and isinstance(code, list):
        filters.append(dm.filters.In(view_id.as_property_ref("code"), values=code))
    if code_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("code"), value=code_prefix))
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
    if isinstance(standard, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("standard"), value=standard))
    if standard and isinstance(standard, list):
        filters.append(dm.filters.In(view_id.as_property_ref("standard"), values=standard))
    if standard_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("standard"), value=standard_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _CogniteAssetTypeQuery(NodeQueryCore[T_DomainModelList, CogniteAssetTypeList]):
    _view_id = CogniteAssetType._view_id
    _result_cls = CogniteAssetType
    _result_list_cls_end = CogniteAssetTypeList

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
        from ._cognite_asset_class import _CogniteAssetClassQuery

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

        if _CogniteAssetClassQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.asset_class = _CogniteAssetClassQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("assetClass"),
                    direction="outwards",
                ),
                connection_name="asset_class",
                connection_property=ViewPropertyId(self._view_id, "assetClass"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.asset_class_filter = DirectRelationFilter(self, self._view_id.as_property_ref("assetClass"))
        self.code = StringFilter(self, self._view_id.as_property_ref("code"))
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.standard = StringFilter(self, self._view_id.as_property_ref("standard"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.asset_class_filter,
                self.code,
                self.description,
                self.name,
                self.standard,
            ]
        )

    def list_cognite_asset_type(self, limit: int = DEFAULT_QUERY_LIMIT) -> CogniteAssetTypeList:
        return self._list(limit=limit)


class CogniteAssetTypeQuery(_CogniteAssetTypeQuery[CogniteAssetTypeList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CogniteAssetTypeList)
