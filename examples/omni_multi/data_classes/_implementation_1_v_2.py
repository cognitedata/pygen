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
from omni_multi.data_classes._sub_interface import SubInterface, SubInterfaceWrite


__all__ = [
    "Implementation1v2",
    "Implementation1v2Write",
    "Implementation1v2List",
    "Implementation1v2WriteList",
    "Implementation1v2Fields",
    "Implementation1v2TextFields",
    "Implementation1v2GraphQL",
]


Implementation1v2TextFields = Literal["external_id", "main_value", "sub_value", "value_2"]
Implementation1v2Fields = Literal["external_id", "main_value", "sub_value", "value_2"]

_IMPLEMENTATION1V2_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "main_value": "mainValue",
    "sub_value": "subValue",
    "value_2": "value2",
}


class Implementation1v2GraphQL(GraphQLCore):
    """This represents the reading version of implementation 1 v 2, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 v 2.
        data_record: The data record of the implementation 1 v 2 node.
        main_value: The main value field.
        sub_value: The sub value field.
        value_2: The value 2 field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "Implementation1", "2")
    main_value: Optional[str] = Field(None, alias="mainValue")
    sub_value: Optional[str] = Field(None, alias="subValue")
    value_2: Optional[str] = Field(None, alias="value2")

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

    def as_read(self) -> Implementation1v2:
        """Convert this GraphQL format of implementation 1 v 2 to the reading format."""
        return Implementation1v2.model_validate(as_read_args(self))

    def as_write(self) -> Implementation1v2Write:
        """Convert this GraphQL format of implementation 1 v 2 to the writing format."""
        return Implementation1v2Write.model_validate(as_write_args(self))


class Implementation1v2(SubInterface):
    """This represents the reading version of implementation 1 v 2.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 v 2.
        data_record: The data record of the implementation 1 v 2 node.
        main_value: The main value field.
        sub_value: The sub value field.
        value_2: The value 2 field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "Implementation1", "2")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("pygen-models", "Implementation1")
    value_2: str = Field(alias="value2")

    def as_write(self) -> Implementation1v2Write:
        """Convert this read version of implementation 1 v 2 to the writing version."""
        return Implementation1v2Write.model_validate(as_write_args(self))


class Implementation1v2Write(SubInterfaceWrite):
    """This represents the writing version of implementation 1 v 2.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the implementation 1 v 2.
        data_record: The data record of the implementation 1 v 2 node.
        main_value: The main value field.
        sub_value: The sub value field.
        value_2: The value 2 field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "main_value",
        "sub_value",
        "value_2",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "Implementation1", "2")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference(
        "pygen-models", "Implementation1"
    )
    value_2: str = Field(alias="value2")


class Implementation1v2List(DomainModelList[Implementation1v2]):
    """List of implementation 1 v 2 in the read version."""

    _INSTANCE = Implementation1v2

    def as_write(self) -> Implementation1v2WriteList:
        """Convert these read versions of implementation 1 v 2 to the writing versions."""
        return Implementation1v2WriteList([node.as_write() for node in self.data])


class Implementation1v2WriteList(DomainModelWriteList[Implementation1v2Write]):
    """List of implementation 1 v 2 in the writing version."""

    _INSTANCE = Implementation1v2Write


def _create_implementation_1_v_2_filter(
    view_id: dm.ViewId,
    main_value: str | list[str] | None = None,
    main_value_prefix: str | None = None,
    sub_value: str | list[str] | None = None,
    sub_value_prefix: str | None = None,
    value_2: str | list[str] | None = None,
    value_2_prefix: str | None = None,
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
    if isinstance(value_2, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("value2"), value=value_2))
    if value_2 and isinstance(value_2, list):
        filters.append(dm.filters.In(view_id.as_property_ref("value2"), values=value_2))
    if value_2_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("value2"), value=value_2_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _Implementation1v2Query(NodeQueryCore[T_DomainModelList, Implementation1v2List]):
    _view_id = Implementation1v2._view_id
    _result_cls = Implementation1v2
    _result_list_cls_end = Implementation1v2List

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
        self.value_2 = StringFilter(self, self._view_id.as_property_ref("value2"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.main_value,
                self.sub_value,
                self.value_2,
            ]
        )

    def list_implementation_1_v_2(self, limit: int = DEFAULT_QUERY_LIMIT) -> Implementation1v2List:
        return self._list(limit=limit)


class Implementation1v2Query(_Implementation1v2Query[Implementation1v2List]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, Implementation1v2List)
