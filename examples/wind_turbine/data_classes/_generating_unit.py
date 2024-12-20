from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import field_validator, model_validator

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
    FloatFilter,
)


__all__ = [
    "GeneratingUnit",
    "GeneratingUnitWrite",
    "GeneratingUnitApply",
    "GeneratingUnitList",
    "GeneratingUnitWriteList",
    "GeneratingUnitApplyList",
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> GeneratingUnit:
        """Convert this GraphQL format of generating unit to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return GeneratingUnit(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            capacity=self.capacity,
            description=self.description,
            name=self.name,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> GeneratingUnitWrite:
        """Convert this GraphQL format of generating unit to the writing format."""
        return GeneratingUnitWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            capacity=self.capacity,
            description=self.description,
            name=self.name,
        )


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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> GeneratingUnitWrite:
        """Convert this read version of generating unit to the writing version."""
        return GeneratingUnitWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            capacity=self.capacity,
            description=self.description,
            name=self.name,
        )

    def as_apply(self) -> GeneratingUnitWrite:
        """Convert this read version of generating unit to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "GeneratingUnit", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    capacity: Optional[float] = None
    description: Optional[str] = None
    name: Optional[str] = None

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

        if self.capacity is not None or write_none:
            properties["capacity"] = self.capacity

        if self.description is not None or write_none:
            properties["description"] = self.description

        if self.name is not None or write_none:
            properties["name"] = self.name

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


class GeneratingUnitApply(GeneratingUnitWrite):
    def __new__(cls, *args, **kwargs) -> GeneratingUnitApply:
        warnings.warn(
            "GeneratingUnitApply is deprecated and will be removed in v1.0. Use GeneratingUnitWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "GeneratingUnit.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class GeneratingUnitList(DomainModelList[GeneratingUnit]):
    """List of generating units in the read version."""

    _INSTANCE = GeneratingUnit

    def as_write(self) -> GeneratingUnitWriteList:
        """Convert these read versions of generating unit to the writing versions."""
        return GeneratingUnitWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> GeneratingUnitWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class GeneratingUnitWriteList(DomainModelWriteList[GeneratingUnitWrite]):
    """List of generating units in the writing version."""

    _INSTANCE = GeneratingUnitWrite


class GeneratingUnitApplyList(GeneratingUnitWriteList): ...


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
