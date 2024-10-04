from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator

from ._core import (
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
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    select_best_node,
    QueryCore,
    NodeQueryCore,
    FloatFilter,
    StringFilter,
)

if TYPE_CHECKING:
    from ._blade import Blade, BladeGraphQL, BladeWrite
    from ._metmast import Metmast, MetmastGraphQL, MetmastWrite
    from ._nacelle import Nacelle, NacelleGraphQL, NacelleWrite
    from ._rotor import Rotor, RotorGraphQL, RotorWrite


__all__ = [
    "Windmill",
    "WindmillWrite",
    "WindmillApply",
    "WindmillList",
    "WindmillWriteList",
    "WindmillApplyList",
    "WindmillFields",
    "WindmillTextFields",
    "WindmillGraphQL",
]


WindmillTextFields = Literal["external_id", "name", "windfarm"]
WindmillFields = Literal["external_id", "capacity", "name", "windfarm"]

_WINDMILL_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "capacity": "capacity",
    "name": "name",
    "windfarm": "windfarm",
}


class WindmillGraphQL(GraphQLCore):
    """This represents the reading version of windmill, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the windmill.
        data_record: The data record of the windmill node.
        blades: The blade field.
        capacity: The capacity field.
        metmast: The metmast field.
        nacelle: The nacelle field.
        name: The name field.
        rotor: The rotor field.
        windfarm: The windfarm field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power-models", "Windmill", "1")
    blades: Optional[list[BladeGraphQL]] = Field(default=None, repr=False)
    capacity: Optional[float] = None
    metmast: Optional[list[MetmastGraphQL]] = Field(default=None, repr=False)
    nacelle: Optional[NacelleGraphQL] = Field(default=None, repr=False)
    name: Optional[str] = None
    rotor: Optional[RotorGraphQL] = Field(default=None, repr=False)
    windfarm: Optional[str] = None

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

    @field_validator("blades", "metmast", "nacelle", "rotor", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> Windmill:
        """Convert this GraphQL format of windmill to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Windmill(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            blades=[blade.as_read() for blade in self.blades or []],
            capacity=self.capacity,
            metmast=[metmast.as_read() for metmast in self.metmast or []],
            nacelle=self.nacelle.as_read() if isinstance(self.nacelle, GraphQLCore) else self.nacelle,
            name=self.name,
            rotor=self.rotor.as_read() if isinstance(self.rotor, GraphQLCore) else self.rotor,
            windfarm=self.windfarm,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> WindmillWrite:
        """Convert this GraphQL format of windmill to the writing format."""
        return WindmillWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            blades=[blade.as_write() for blade in self.blades or []],
            capacity=self.capacity,
            metmast=[metmast.as_write() for metmast in self.metmast or []],
            nacelle=self.nacelle.as_write() if isinstance(self.nacelle, GraphQLCore) else self.nacelle,
            name=self.name,
            rotor=self.rotor.as_write() if isinstance(self.rotor, GraphQLCore) else self.rotor,
            windfarm=self.windfarm,
        )


class Windmill(DomainModel):
    """This represents the reading version of windmill.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the windmill.
        data_record: The data record of the windmill node.
        blades: The blade field.
        capacity: The capacity field.
        metmast: The metmast field.
        nacelle: The nacelle field.
        name: The name field.
        rotor: The rotor field.
        windfarm: The windfarm field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power-models", "Windmill", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    blades: Optional[list[Union[Blade, str, dm.NodeId]]] = Field(default=None, repr=False)
    capacity: Optional[float] = None
    metmast: Optional[list[Union[Metmast, str, dm.NodeId]]] = Field(default=None, repr=False)
    nacelle: Union[Nacelle, str, dm.NodeId, None] = Field(default=None, repr=False)
    name: Optional[str] = None
    rotor: Union[Rotor, str, dm.NodeId, None] = Field(default=None, repr=False)
    windfarm: Optional[str] = None

    def as_write(self) -> WindmillWrite:
        """Convert this read version of windmill to the writing version."""
        return WindmillWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            blades=[blade.as_write() if isinstance(blade, DomainModel) else blade for blade in self.blades or []],
            capacity=self.capacity,
            metmast=[
                metmast.as_write() if isinstance(metmast, DomainModel) else metmast for metmast in self.metmast or []
            ],
            nacelle=self.nacelle.as_write() if isinstance(self.nacelle, DomainModel) else self.nacelle,
            name=self.name,
            rotor=self.rotor.as_write() if isinstance(self.rotor, DomainModel) else self.rotor,
            windfarm=self.windfarm,
        )

    def as_apply(self) -> WindmillWrite:
        """Convert this read version of windmill to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, Windmill],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._blade import Blade
        from ._metmast import Metmast
        from ._nacelle import Nacelle
        from ._rotor import Rotor

        for instance in instances.values():
            if (
                isinstance(instance.nacelle, (dm.NodeId, str))
                and (nacelle := nodes_by_id.get(instance.nacelle))
                and isinstance(nacelle, Nacelle)
            ):
                instance.nacelle = nacelle
            if (
                isinstance(instance.rotor, (dm.NodeId, str))
                and (rotor := nodes_by_id.get(instance.rotor))
                and isinstance(rotor, Rotor)
            ):
                instance.rotor = rotor
            if edges := edges_by_source_node.get(instance.as_id()):
                blades: list[Blade | str | dm.NodeId] = []
                metmast: list[Metmast | str | dm.NodeId] = []
                for edge in edges:
                    value: DomainModel | DomainRelation | str | dm.NodeId
                    if isinstance(edge, DomainRelation):
                        value = edge
                    else:
                        other_end: dm.DirectRelationReference = (
                            edge.end_node
                            if edge.start_node.space == instance.space
                            and edge.start_node.external_id == instance.external_id
                            else edge.start_node
                        )
                        destination: dm.NodeId | str = (
                            as_node_id(other_end)
                            if other_end.space != DEFAULT_INSTANCE_SPACE
                            else other_end.external_id
                        )
                        if destination in nodes_by_id:
                            value = nodes_by_id[destination]
                        else:
                            value = destination
                    edge_type = edge.edge_type if isinstance(edge, DomainRelation) else edge.type

                    if edge_type == dm.DirectRelationReference("power-models", "Windmill.blades") and isinstance(
                        value, (Blade, str, dm.NodeId)
                    ):
                        blades.append(value)
                    if edge_type == dm.DirectRelationReference("power-models", "Windmill.metmast") and isinstance(
                        value, (Metmast, str, dm.NodeId)
                    ):
                        metmast.append(value)

                instance.blades = blades or None
                instance.metmast = metmast or None


class WindmillWrite(DomainModelWrite):
    """This represents the writing version of windmill.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the windmill.
        data_record: The data record of the windmill node.
        blades: The blade field.
        capacity: The capacity field.
        metmast: The metmast field.
        nacelle: The nacelle field.
        name: The name field.
        rotor: The rotor field.
        windfarm: The windfarm field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power-models", "Windmill", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    blades: Optional[list[Union[BladeWrite, str, dm.NodeId]]] = Field(default=None, repr=False)
    capacity: Optional[float] = None
    metmast: Optional[list[Union[MetmastWrite, str, dm.NodeId]]] = Field(default=None, repr=False)
    nacelle: Union[NacelleWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    name: Optional[str] = None
    rotor: Union[RotorWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    windfarm: Optional[str] = None

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

        if self.nacelle is not None:
            properties["nacelle"] = {
                "space": self.space if isinstance(self.nacelle, str) else self.nacelle.space,
                "externalId": self.nacelle if isinstance(self.nacelle, str) else self.nacelle.external_id,
            }

        if self.name is not None or write_none:
            properties["name"] = self.name

        if self.rotor is not None:
            properties["rotor"] = {
                "space": self.space if isinstance(self.rotor, str) else self.rotor.space,
                "externalId": self.rotor if isinstance(self.rotor, str) else self.rotor.external_id,
            }

        if self.windfarm is not None or write_none:
            properties["windfarm"] = self.windfarm

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

        edge_type = dm.DirectRelationReference("power-models", "Windmill.blades")
        for blade in self.blades or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=blade,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("power-models", "Windmill.metmast")
        for metmast in self.metmast or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=metmast,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.nacelle, DomainModelWrite):
            other_resources = self.nacelle._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.rotor, DomainModelWrite):
            other_resources = self.rotor._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class WindmillApply(WindmillWrite):
    def __new__(cls, *args, **kwargs) -> WindmillApply:
        warnings.warn(
            "WindmillApply is deprecated and will be removed in v1.0. Use WindmillWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Windmill.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class WindmillList(DomainModelList[Windmill]):
    """List of windmills in the read version."""

    _INSTANCE = Windmill

    def as_write(self) -> WindmillWriteList:
        """Convert these read versions of windmill to the writing versions."""
        return WindmillWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> WindmillWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class WindmillWriteList(DomainModelWriteList[WindmillWrite]):
    """List of windmills in the writing version."""

    _INSTANCE = WindmillWrite


class WindmillApplyList(WindmillWriteList): ...


def _create_windmill_filter(
    view_id: dm.ViewId,
    min_capacity: float | None = None,
    max_capacity: float | None = None,
    nacelle: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    rotor: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    windfarm: str | list[str] | None = None,
    windfarm_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if min_capacity is not None or max_capacity is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("capacity"), gte=min_capacity, lte=max_capacity))
    if nacelle and isinstance(nacelle, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("nacelle"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": nacelle}
            )
        )
    if nacelle and isinstance(nacelle, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("nacelle"), value={"space": nacelle[0], "externalId": nacelle[1]})
        )
    if nacelle and isinstance(nacelle, list) and isinstance(nacelle[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("nacelle"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in nacelle],
            )
        )
    if nacelle and isinstance(nacelle, list) and isinstance(nacelle[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("nacelle"),
                values=[{"space": item[0], "externalId": item[1]} for item in nacelle],
            )
        )
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if rotor and isinstance(rotor, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("rotor"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": rotor}
            )
        )
    if rotor and isinstance(rotor, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("rotor"), value={"space": rotor[0], "externalId": rotor[1]})
        )
    if rotor and isinstance(rotor, list) and isinstance(rotor[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("rotor"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in rotor],
            )
        )
    if rotor and isinstance(rotor, list) and isinstance(rotor[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("rotor"), values=[{"space": item[0], "externalId": item[1]} for item in rotor]
            )
        )
    if isinstance(windfarm, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("windfarm"), value=windfarm))
    if windfarm and isinstance(windfarm, list):
        filters.append(dm.filters.In(view_id.as_property_ref("windfarm"), values=windfarm))
    if windfarm_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("windfarm"), value=windfarm_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _WindmillQuery(NodeQueryCore[T_DomainModelList, WindmillList]):
    _view_id = Windmill._view_id
    _result_cls = Windmill
    _result_list_cls_end = WindmillList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
    ):
        from ._blade import _BladeQuery
        from ._metmast import _MetmastQuery
        from ._nacelle import _NacelleQuery
        from ._rotor import _RotorQuery

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
        )

        if _BladeQuery not in created_types:
            self.blades = _BladeQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                "blades",
            )

        if _MetmastQuery not in created_types:
            self.metmast = _MetmastQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                "metmast",
            )

        if _NacelleQuery not in created_types:
            self.nacelle = _NacelleQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("nacelle"),
                    direction="outwards",
                ),
                "nacelle",
            )

        if _RotorQuery not in created_types:
            self.rotor = _RotorQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("rotor"),
                    direction="outwards",
                ),
                "rotor",
            )

        self.capacity = FloatFilter(self, self._view_id.as_property_ref("capacity"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.windfarm = StringFilter(self, self._view_id.as_property_ref("windfarm"))
        self._filter_classes.extend(
            [
                self.capacity,
                self.name,
                self.windfarm,
            ]
        )

    def list_windmill(self, limit: int = DEFAULT_QUERY_LIMIT) -> WindmillList:
        return self._list(limit=limit)


class WindmillQuery(_WindmillQuery[WindmillList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, WindmillList)
