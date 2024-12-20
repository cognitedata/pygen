from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator

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
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    is_tuple_id,
    select_best_node,
    QueryCore,
    NodeQueryCore,
    StringFilter,
)


__all__ = [
    "MainInterface",
    "MainInterfaceWrite",
    "MainInterfaceApply",
    "MainInterfaceList",
    "MainInterfaceWriteList",
    "MainInterfaceApplyList",
    "MainInterfaceFields",
    "MainInterfaceTextFields",
    "MainInterfaceGraphQL",
]


MainInterfaceTextFields = Literal["external_id", "main_value"]
MainInterfaceFields = Literal["external_id", "main_value"]

_MAININTERFACE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "main_value": "mainValue",
}


class MainInterfaceGraphQL(GraphQLCore):
    """This represents the reading version of main interface, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the main interface.
        data_record: The data record of the main interface node.
        main_value: The main value field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "MainInterface", "1")
    main_value: Optional[str] = Field(None, alias="mainValue")

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
    def as_read(self) -> MainInterface:
        """Convert this GraphQL format of main interface to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return MainInterface(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            main_value=self.main_value,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> MainInterfaceWrite:
        """Convert this GraphQL format of main interface to the writing format."""
        return MainInterfaceWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            main_value=self.main_value,
        )


class MainInterface(DomainModel):
    """This represents the reading version of main interface.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the main interface.
        data_record: The data record of the main interface node.
        main_value: The main value field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "MainInterface", "1")

    space: str
    node_type: Union[dm.DirectRelationReference, None] = None
    main_value: Optional[str] = Field(None, alias="mainValue")

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> MainInterfaceWrite:
        """Convert this read version of main interface to the writing version."""
        return MainInterfaceWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            main_value=self.main_value,
        )

    def as_apply(self) -> MainInterfaceWrite:
        """Convert this read version of main interface to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class MainInterfaceWrite(DomainModelWrite):
    """This represents the writing version of main interface.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the main interface.
        data_record: The data record of the main interface node.
        main_value: The main value field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "MainInterface", "1")

    space: str
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    main_value: Optional[str] = Field(None, alias="mainValue")

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

        if self.main_value is not None or write_none:
            properties["mainValue"] = self.main_value

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


class MainInterfaceApply(MainInterfaceWrite):
    def __new__(cls, *args, **kwargs) -> MainInterfaceApply:
        warnings.warn(
            "MainInterfaceApply is deprecated and will be removed in v1.0. Use MainInterfaceWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "MainInterface.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class MainInterfaceList(DomainModelList[MainInterface]):
    """List of main interfaces in the read version."""

    _INSTANCE = MainInterface

    def as_write(self) -> MainInterfaceWriteList:
        """Convert these read versions of main interface to the writing versions."""
        return MainInterfaceWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> MainInterfaceWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class MainInterfaceWriteList(DomainModelWriteList[MainInterfaceWrite]):
    """List of main interfaces in the writing version."""

    _INSTANCE = MainInterfaceWrite


class MainInterfaceApplyList(MainInterfaceWriteList): ...


def _create_main_interface_filter(
    view_id: dm.ViewId,
    main_value: str | list[str] | None = None,
    main_value_prefix: str | None = None,
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
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _MainInterfaceQuery(NodeQueryCore[T_DomainModelList, MainInterfaceList]):
    _view_id = MainInterface._view_id
    _result_cls = MainInterface
    _result_list_cls_end = MainInterfaceList

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
        self.main_value = StringFilter(self, self._view_id.as_property_ref("mainValue"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.main_value,
            ]
        )

    def list_main_interface(self, limit: int = DEFAULT_QUERY_LIMIT) -> MainInterfaceList:
        return self._list(limit=limit)


class MainInterfaceQuery(_MainInterfaceQuery[MainInterfaceList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, MainInterfaceList)
