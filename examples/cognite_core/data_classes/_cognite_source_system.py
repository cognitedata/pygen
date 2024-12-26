from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

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
)
from cognite_core.data_classes._cognite_describable_node import CogniteDescribableNode, CogniteDescribableNodeWrite


__all__ = [
    "CogniteSourceSystem",
    "CogniteSourceSystemWrite",
    "CogniteSourceSystemApply",
    "CogniteSourceSystemList",
    "CogniteSourceSystemWriteList",
    "CogniteSourceSystemApplyList",
    "CogniteSourceSystemFields",
    "CogniteSourceSystemTextFields",
    "CogniteSourceSystemGraphQL",
]


CogniteSourceSystemTextFields = Literal[
    "external_id", "aliases", "description", "manufacturer", "name", "tags", "version_"
]
CogniteSourceSystemFields = Literal["external_id", "aliases", "description", "manufacturer", "name", "tags", "version_"]

_COGNITESOURCESYSTEM_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aliases": "aliases",
    "description": "description",
    "manufacturer": "manufacturer",
    "name": "name",
    "tags": "tags",
    "version_": "version",
}


class CogniteSourceSystemGraphQL(GraphQLCore):
    """This represents the reading version of Cognite source system, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite source system.
        data_record: The data record of the Cognite source system node.
        aliases: Alternative names for the node
        description: Description of the instance
        manufacturer: Manufacturer of the source system
        name: Name of the instance
        tags: Text based labels for generic use, limited to 1000
        version_: Version identifier for the source system
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteSourceSystem", "v1")
    aliases: Optional[list[str]] = None
    description: Optional[str] = None
    manufacturer: Optional[str] = None
    name: Optional[str] = None
    tags: Optional[list[str]] = None
    version_: Optional[str] = Field(None, alias="version")

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
    def as_read(self) -> CogniteSourceSystem:
        """Convert this GraphQL format of Cognite source system to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return CogniteSourceSystem(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            aliases=self.aliases,
            description=self.description,
            manufacturer=self.manufacturer,
            name=self.name,
            tags=self.tags,
            version_=self.version_,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteSourceSystemWrite:
        """Convert this GraphQL format of Cognite source system to the writing format."""
        return CogniteSourceSystemWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            aliases=self.aliases,
            description=self.description,
            manufacturer=self.manufacturer,
            name=self.name,
            tags=self.tags,
            version_=self.version_,
        )


class CogniteSourceSystem(CogniteDescribableNode):
    """This represents the reading version of Cognite source system.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite source system.
        data_record: The data record of the Cognite source system node.
        aliases: Alternative names for the node
        description: Description of the instance
        manufacturer: Manufacturer of the source system
        name: Name of the instance
        tags: Text based labels for generic use, limited to 1000
        version_: Version identifier for the source system
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteSourceSystem", "v1")

    node_type: Union[dm.DirectRelationReference, None] = None
    manufacturer: Optional[str] = None
    version_: Optional[str] = Field(None, alias="version")

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CogniteSourceSystemWrite:
        """Convert this read version of Cognite source system to the writing version."""
        return CogniteSourceSystemWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            aliases=self.aliases,
            description=self.description,
            manufacturer=self.manufacturer,
            name=self.name,
            tags=self.tags,
            version_=self.version_,
        )

    def as_apply(self) -> CogniteSourceSystemWrite:
        """Convert this read version of Cognite source system to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class CogniteSourceSystemWrite(CogniteDescribableNodeWrite):
    """This represents the writing version of Cognite source system.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite source system.
        data_record: The data record of the Cognite source system node.
        aliases: Alternative names for the node
        description: Description of the instance
        manufacturer: Manufacturer of the source system
        name: Name of the instance
        tags: Text based labels for generic use, limited to 1000
        version_: Version identifier for the source system
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("cdf_cdm", "CogniteSourceSystem", "v1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    manufacturer: Optional[str] = None
    version_: Optional[str] = Field(None, alias="version")

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

        if self.manufacturer is not None or write_none:
            properties["manufacturer"] = self.manufacturer

        if self.name is not None or write_none:
            properties["name"] = self.name

        if self.tags is not None or write_none:
            properties["tags"] = self.tags

        if self.version_ is not None or write_none:
            properties["version"] = self.version_

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


class CogniteSourceSystemApply(CogniteSourceSystemWrite):
    def __new__(cls, *args, **kwargs) -> CogniteSourceSystemApply:
        warnings.warn(
            "CogniteSourceSystemApply is deprecated and will be removed in v1.0. "
            "Use CogniteSourceSystemWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "CogniteSourceSystem.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class CogniteSourceSystemList(DomainModelList[CogniteSourceSystem]):
    """List of Cognite source systems in the read version."""

    _INSTANCE = CogniteSourceSystem

    def as_write(self) -> CogniteSourceSystemWriteList:
        """Convert these read versions of Cognite source system to the writing versions."""
        return CogniteSourceSystemWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> CogniteSourceSystemWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class CogniteSourceSystemWriteList(DomainModelWriteList[CogniteSourceSystemWrite]):
    """List of Cognite source systems in the writing version."""

    _INSTANCE = CogniteSourceSystemWrite


class CogniteSourceSystemApplyList(CogniteSourceSystemWriteList): ...


def _create_cognite_source_system_filter(
    view_id: dm.ViewId,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    manufacturer: str | list[str] | None = None,
    manufacturer_prefix: str | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    version_: str | list[str] | None = None,
    version_prefix: str | None = None,
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
    if isinstance(manufacturer, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("manufacturer"), value=manufacturer))
    if manufacturer and isinstance(manufacturer, list):
        filters.append(dm.filters.In(view_id.as_property_ref("manufacturer"), values=manufacturer))
    if manufacturer_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("manufacturer"), value=manufacturer_prefix))
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if isinstance(version_, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("version"), value=version_))
    if version_ and isinstance(version_, list):
        filters.append(dm.filters.In(view_id.as_property_ref("version"), values=version_))
    if version_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("version"), value=version_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _CogniteSourceSystemQuery(NodeQueryCore[T_DomainModelList, CogniteSourceSystemList]):
    _view_id = CogniteSourceSystem._view_id
    _result_cls = CogniteSourceSystem
    _result_list_cls_end = CogniteSourceSystemList

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
        self.manufacturer = StringFilter(self, self._view_id.as_property_ref("manufacturer"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.version_ = StringFilter(self, self._view_id.as_property_ref("version"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.description,
                self.manufacturer,
                self.name,
                self.version_,
            ]
        )

    def list_cognite_source_system(self, limit: int = DEFAULT_QUERY_LIMIT) -> CogniteSourceSystemList:
        return self._list(limit=limit)


class CogniteSourceSystemQuery(_CogniteSourceSystemQuery[CogniteSourceSystemList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CogniteSourceSystemList)
