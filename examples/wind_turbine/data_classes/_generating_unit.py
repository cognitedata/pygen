from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import field_validator, model_validator, ValidationInfo

from wind_turbine.config import global_config
from wind_turbine.data_classes._core import (
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
    FloatFilter,
)


__all__ = [
    "GeneratingUnit",
    "GeneratingUnitWrite",
    "GeneratingUnitList",
    "GeneratingUnitWriteList",
    "GeneratingUnitFields",
    "GeneratingUnitTextFields",
    "GeneratingUnitGraphQL",
]


GeneratingUnitTextFields = Literal["external_id", "description", "name"]
GeneratingUnitFields = Literal["external_id", "capacity", "description", "name"]

_GENERATINGUNIT_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "capacity": "capacity",
    "description": "description",
    "name": "name",
}


class GeneratingUnitGraphQL(GraphQLCore):
    """This represents the reading version of generating unit, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generating unit.
        data_record: The data record of the generating unit node.
        capacity: The capacity field.
        description: Description of the instance
        name: Name of the instance
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "GeneratingUnit", "1")
    capacity: Optional[float] = None
    description: Optional[str] = None
    name: Optional[str] = None

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

    def as_read(self) -> GeneratingUnit:
        """Convert this GraphQL format of generating unit to the reading format."""
        return GeneratingUnit.model_validate(as_read_args(self))

    def as_write(self) -> GeneratingUnitWrite:
        """Convert this GraphQL format of generating unit to the writing format."""
        return GeneratingUnitWrite.model_validate(as_write_args(self))


class GeneratingUnit(DomainModel):
    """This represents the reading version of generating unit.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generating unit.
        data_record: The data record of the generating unit node.
        capacity: The capacity field.
        description: Description of the instance
        name: Name of the instance
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "GeneratingUnit", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    capacity: Optional[float] = None
    description: Optional[str] = None
    name: Optional[str] = None

    def as_write(self) -> GeneratingUnitWrite:
        """Convert this read version of generating unit to the writing version."""
        return GeneratingUnitWrite.model_validate(as_write_args(self))


class GeneratingUnitWrite(DomainModelWrite):
    """This represents the writing version of generating unit.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generating unit.
        data_record: The data record of the generating unit node.
        capacity: The capacity field.
        description: Description of the instance
        name: Name of the instance
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "capacity",
        "description",
        "name",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "GeneratingUnit", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    capacity: Optional[float] = None
    description: Optional[str] = None
    name: Optional[str] = None


class GeneratingUnitList(DomainModelList[GeneratingUnit]):
    """List of generating units in the read version."""

    _INSTANCE = GeneratingUnit

    def as_write(self) -> GeneratingUnitWriteList:
        """Convert these read versions of generating unit to the writing versions."""
        return GeneratingUnitWriteList([node.as_write() for node in self.data])


class GeneratingUnitWriteList(DomainModelWriteList[GeneratingUnitWrite]):
    """List of generating units in the writing version."""

    _INSTANCE = GeneratingUnitWrite


def _create_generating_unit_filter(
    view_id: dm.ViewId,
    min_capacity: float | None = None,
    max_capacity: float | None = None,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if min_capacity is not None or max_capacity is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("capacity"), gte=min_capacity, lte=max_capacity))
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


class _GeneratingUnitQuery(NodeQueryCore[T_DomainModelList, GeneratingUnitList]):
    _view_id = GeneratingUnit._view_id
    _result_cls = GeneratingUnit
    _result_list_cls_end = GeneratingUnitList

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
        self.capacity = FloatFilter(self, self._view_id.as_property_ref("capacity"))
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.capacity,
                self.description,
                self.name,
            ]
        )

    def list_generating_unit(self, limit: int = DEFAULT_QUERY_LIMIT) -> GeneratingUnitList:
        return self._list(limit=limit)


class GeneratingUnitQuery(_GeneratingUnitQuery[GeneratingUnitList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, GeneratingUnitList)
