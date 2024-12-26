from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union, no_type_check

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from wind_turbine.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainRelation,
    DomainRelationList,
    DomainRelationWrite,
    DomainRelationWriteList,
    EdgeQueryCore,
    FloatFilter,
    GraphQLCore,
    NodeQueryCore,
    QueryCore,
    ResourcesWrite,
    StringFilter,
    T_DomainList,
)
from wind_turbine.data_classes._metmast import Metmast, MetmastGraphQL, MetmastWrite
from wind_turbine.data_classes._wind_turbine import WindTurbineWrite

if TYPE_CHECKING:
    from wind_turbine.data_classes._metmast import Metmast, MetmastGraphQL, MetmastWrite
    from wind_turbine.data_classes._wind_turbine import WindTurbine, WindTurbineGraphQL, WindTurbineWrite


__all__ = [
    "Distance",
    "DistanceWrite",
    "DistanceApply",
    "DistanceList",
    "DistanceWriteList",
    "DistanceApplyList",
    "DistanceFields",
]


DistanceTextFields = Literal["external_id",]
DistanceFields = Literal["external_id", "distance"]
_DISTANCE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "distance": "distance",
}


class DistanceGraphQL(GraphQLCore):
    """This represents the reading version of distance, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the distance.
        data_record: The data record of the distance node.
        end_node: The end node of this edge.
        distance: The distance field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Distance", "1")
    end_node: Union[MetmastGraphQL, WindTurbineGraphQL, None] = None
    distance: Optional[float] = None

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> Distance:
        """Convert this GraphQL format of distance to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Distance(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            end_node=self.end_node.as_read() if isinstance(self.end_node, GraphQLCore) else self.end_node,
            distance=self.distance,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> DistanceWrite:
        """Convert this GraphQL format of distance to the writing format."""
        return DistanceWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            end_node=self.end_node.as_write() if isinstance(self.end_node, DomainModel) else self.end_node,
            distance=self.distance,
        )


class Distance(DomainRelation):
    """This represents the reading version of distance.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the distance.
        data_record: The data record of the distance edge.
        end_node: The end node of this edge.
        distance: The distance field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Distance", "1")
    space: str = DEFAULT_INSTANCE_SPACE
    end_node: Union[Metmast, WindTurbine, str, dm.NodeId]
    distance: Optional[float] = None

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> DistanceWrite:
        """Convert this read version of distance to the writing version."""
        return DistanceWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            end_node=self.end_node.as_write() if isinstance(self.end_node, DomainModel) else self.end_node,
            distance=self.distance,
        )

    def as_apply(self) -> DistanceWrite:
        """Convert this read version of distance to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class DistanceWrite(DomainRelationWrite):
    """This represents the writing version of distance.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the distance.
        data_record: The data record of the distance edge.
        end_node: The end node of this edge.
        distance: The distance field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Distance", "1")
    space: str = DEFAULT_INSTANCE_SPACE
    end_node: Union[MetmastWrite, WindTurbineWrite, str, dm.NodeId]
    distance: Optional[float] = None

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        start_node: DomainModelWrite,
        edge_type: dm.DirectRelationReference,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.external_id and (self.space, self.external_id) in cache:
            return resources

        _validate_end_node(start_node, self.end_node)

        if isinstance(self.end_node, DomainModelWrite):
            end_node = self.end_node.as_direct_reference()
        elif isinstance(self.end_node, str):
            end_node = dm.DirectRelationReference(self.space, self.end_node)
        elif isinstance(self.end_node, dm.NodeId):
            end_node = dm.DirectRelationReference(self.end_node.space, self.end_node.external_id)
        else:
            raise ValueError(f"Invalid type for equipment_module: {type(self.end_node)}")

        external_id = self.external_id or DomainRelationWrite.external_id_factory(start_node, self.end_node, edge_type)

        properties: dict[str, Any] = {}

        if self.distance is not None or write_none:
            properties["distance"] = self.distance

        if properties:
            this_edge = dm.EdgeApply(
                space=self.space,
                external_id=external_id,
                type=edge_type,
                start_node=start_node.as_direct_reference(),
                end_node=end_node,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                    )
                ],
            )
            resources.edges.append(this_edge)
            cache.add((self.space, external_id))

        if isinstance(self.end_node, DomainModelWrite):
            other_resources = self.end_node._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class DistanceApply(DistanceWrite):
    def __new__(cls, *args, **kwargs) -> DistanceApply:
        warnings.warn(
            "DistanceApply is deprecated and will be removed in v1.0. "
            "Use DistanceWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Distance.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class DistanceList(DomainRelationList[Distance]):
    """List of distances in the reading version."""

    _INSTANCE = Distance

    def as_write(self) -> DistanceWriteList:
        """Convert this read version of distance list to the writing version."""
        return DistanceWriteList([edge.as_write() for edge in self])

    def as_apply(self) -> DistanceWriteList:
        """Convert these read versions of distance list to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class DistanceWriteList(DomainRelationWriteList[DistanceWrite]):
    """List of distances in the writing version."""

    _INSTANCE = DistanceWrite


class DistanceApplyList(DistanceWriteList): ...


def _create_distance_filter(
    edge_type: dm.DirectRelationReference,
    view_id: dm.ViewId,
    start_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    start_node_space: str = DEFAULT_INSTANCE_SPACE,
    end_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    space_end_node: str = DEFAULT_INSTANCE_SPACE,
    min_distance: float | None = None,
    max_distance: float | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter:
    filters: list[dm.Filter] = [
        dm.filters.Equals(
            ["edge", "type"],
            {"space": edge_type.space, "externalId": edge_type.external_id},
        )
    ]
    if start_node and isinstance(start_node, str):
        filters.append(
            dm.filters.Equals(["edge", "startNode"], value={"space": start_node_space, "externalId": start_node})
        )
    if start_node and isinstance(start_node, dm.NodeId):
        filters.append(
            dm.filters.Equals(
                ["edge", "startNode"], value=start_node.dump(camel_case=True, include_instance_type=False)
            )
        )
    if start_node and isinstance(start_node, list):
        filters.append(
            dm.filters.In(
                ["edge", "startNode"],
                values=[
                    (
                        {"space": start_node_space, "externalId": ext_id}
                        if isinstance(ext_id, str)
                        else ext_id.dump(camel_case=True, include_instance_type=False)
                    )
                    for ext_id in start_node
                ],
            )
        )
    if end_node and isinstance(end_node, str):
        filters.append(dm.filters.Equals(["edge", "endNode"], value={"space": space_end_node, "externalId": end_node}))
    if end_node and isinstance(end_node, dm.NodeId):
        filters.append(
            dm.filters.Equals(["edge", "endNode"], value=end_node.dump(camel_case=True, include_instance_type=False))
        )
    if end_node and isinstance(end_node, list):
        filters.append(
            dm.filters.In(
                ["edge", "endNode"],
                values=[
                    (
                        {"space": space_end_node, "externalId": ext_id}
                        if isinstance(ext_id, str)
                        else ext_id.dump(camel_case=True, include_instance_type=False)
                    )
                    for ext_id in end_node
                ],
            )
        )
    if min_distance is not None or max_distance is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("distance"), gte=min_distance, lte=max_distance))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["edge", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["edge", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["edge", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters)


_EXPECTED_START_NODES_BY_END_NODE: dict[type[DomainModelWrite], set[type[DomainModelWrite]]] = {
    MetmastWrite: {WindTurbineWrite},
}


def _validate_end_node(
    start_node: DomainModelWrite, end_node: Union[MetmastWrite, WindTurbineWrite, str, dm.NodeId]
) -> None:
    if isinstance(end_node, str | dm.NodeId):
        # Nothing to validate
        return
    if type(end_node) not in _EXPECTED_START_NODES_BY_END_NODE:
        raise ValueError(
            f"Invalid end node type: {type(end_node)}. "
            f"Should be one of {[t.__name__ for t in _EXPECTED_START_NODES_BY_END_NODE.keys()]}"
        )
    if type(start_node) not in _EXPECTED_START_NODES_BY_END_NODE[type(end_node)]:
        raise ValueError(
            f"Invalid end node type: {type(end_node)}. "
            f"Expected one of: {_EXPECTED_START_NODES_BY_END_NODE[type(end_node)]}"
        )


class _DistanceQuery(EdgeQueryCore[T_DomainList, DistanceList]):
    _view_id = Distance._view_id
    _result_cls = Distance
    _result_list_cls_end = DistanceList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainList],
        end_node_cls: type[NodeQueryCore],
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
    ):
        super().__init__(created_types, creation_path, client, result_list_cls, expression, None, connection_name)
        if end_node_cls not in created_types:
            self.end_node = end_node_cls(
                created_types=created_types.copy(),
                creation_path=self._creation_path,
                client=client,
                result_list_cls=result_list_cls,  # type: ignore[type-var]
                expression=dm.query.NodeResultSetExpression(),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.distance = FloatFilter(self, self._view_id.as_property_ref("distance"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.distance,
            ]
        )
