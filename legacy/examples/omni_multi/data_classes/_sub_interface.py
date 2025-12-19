from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

from omni_multi.config import global_config
from omni_multi.data_classes._core import (
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
from omni_multi.data_classes._main_interface import MainInterface, MainInterfaceWrite


__all__ = [
    "SubInterface",
    "SubInterfaceWrite",
    "SubInterfaceList",
    "SubInterfaceWriteList",
    "SubInterfaceFields",
    "SubInterfaceTextFields",
    "SubInterfaceGraphQL",
]


SubInterfaceTextFields = Literal["external_id", "main_value", "sub_value"]
SubInterfaceFields = Literal["external_id", "main_value", "sub_value"]

_SUBINTERFACE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "main_value": "mainValue",
    "sub_value": "subValue",
}


class SubInterfaceGraphQL(GraphQLCore):
    """This represents the reading version of sub interface, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the sub interface.
        data_record: The data record of the sub interface node.
        main_value: The main value field.
        sub_value: The sub value field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "SubInterface", "1")
    main_value: Optional[str] = Field(None, alias="mainValue")
    sub_value: Optional[str] = Field(None, alias="subValue")

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

    def as_read(self) -> SubInterface:
        """Convert this GraphQL format of sub interface to the reading format."""
        return SubInterface.model_validate(as_read_args(self))

    def as_write(self) -> SubInterfaceWrite:
        """Convert this GraphQL format of sub interface to the writing format."""
        return SubInterfaceWrite.model_validate(as_write_args(self))


class SubInterface(MainInterface):
    """This represents the reading version of sub interface.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the sub interface.
        data_record: The data record of the sub interface node.
        main_value: The main value field.
        sub_value: The sub value field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "SubInterface", "1")

    node_type: Union[dm.DirectRelationReference, None] = None
    sub_value: Optional[str] = Field(None, alias="subValue")

    def as_write(self) -> SubInterfaceWrite:
        """Convert this read version of sub interface to the writing version."""
        return SubInterfaceWrite.model_validate(as_write_args(self))


class SubInterfaceWrite(MainInterfaceWrite):
    """This represents the writing version of sub interface.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the sub interface.
        data_record: The data record of the sub interface node.
        main_value: The main value field.
        sub_value: The sub value field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "main_value",
        "sub_value",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "SubInterface", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    sub_value: Optional[str] = Field(None, alias="subValue")


class SubInterfaceList(DomainModelList[SubInterface]):
    """List of sub interfaces in the read version."""

    _INSTANCE = SubInterface

    def as_write(self) -> SubInterfaceWriteList:
        """Convert these read versions of sub interface to the writing versions."""
        return SubInterfaceWriteList([node.as_write() for node in self.data])


class SubInterfaceWriteList(DomainModelWriteList[SubInterfaceWrite]):
    """List of sub interfaces in the writing version."""

    _INSTANCE = SubInterfaceWrite


def _create_sub_interface_filter(
    view_id: dm.ViewId,
    main_value: str | list[str] | None = None,
    main_value_prefix: str | None = None,
    sub_value: str | list[str] | None = None,
    sub_value_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(main_value, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("mainValue"), value=main_value))
    if main_value and isinstance(main_value, list):
        filters.append(dm.filters.In(view_id.as_property_ref("mainValue"), values=main_value))
    if main_value_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("mainValue"), value=main_value_prefix))
    if isinstance(sub_value, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("subValue"), value=sub_value))
    if sub_value and isinstance(sub_value, list):
        filters.append(dm.filters.In(view_id.as_property_ref("subValue"), values=sub_value))
    if sub_value_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("subValue"), value=sub_value_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _SubInterfaceQuery(NodeQueryCore[T_DomainModelList, SubInterfaceList]):
    _view_id = SubInterface._view_id
    _result_cls = SubInterface
    _result_list_cls_end = SubInterfaceList

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
        self.main_value = StringFilter(self, self._view_id.as_property_ref("mainValue"))
        self.sub_value = StringFilter(self, self._view_id.as_property_ref("subValue"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.main_value,
                self.sub_value,
            ]
        )

    def list_sub_interface(self, limit: int = DEFAULT_QUERY_LIMIT) -> SubInterfaceList:
        return self._list(limit=limit)


class SubInterfaceQuery(_SubInterfaceQuery[SubInterfaceList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, SubInterfaceList)
