from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelCore,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)

if TYPE_CHECKING:
    from ._blade import Blade, BladeApply
    from ._metmast import Metmast, MetmastApply
    from ._nacelle import Nacelle, NacelleApply
    from ._rotor import Rotor, RotorApply


__all__ = ["Windmill", "WindmillApply", "WindmillList", "WindmillApplyList", "WindmillFields", "WindmillTextFields"]


WindmillTextFields = Literal["name", "windfarm"]
WindmillFields = Literal["capacity", "name", "windfarm"]

_WINDMILL_PROPERTIES_BY_FIELD = {
    "capacity": "capacity",
    "name": "name",
    "windfarm": "windfarm",
}


class Windmill(DomainModel):
    """This represents the reading version of windmill.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the windmill.
        blades: The blade field.
        capacity: The capacity field.
        metmast: The metmast field.
        nacelle: The nacelle field.
        name: The name field.
        rotor: The rotor field.
        windfarm: The windfarm field.
        created_time: The created time of the windmill node.
        last_updated_time: The last updated time of the windmill node.
        deleted_time: If present, the deleted time of the windmill node.
        version: The version of the windmill node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    blades: Union[list[Blade], list[str], None] = Field(default=None, repr=False)
    capacity: Optional[float] = None
    metmast: Union[list[Metmast], list[str], None] = Field(default=None, repr=False)
    nacelle: Union[Nacelle, str, dm.NodeId, None] = Field(None, repr=False)
    name: Optional[str] = None
    rotor: Union[Rotor, str, dm.NodeId, None] = Field(None, repr=False)
    windfarm: Optional[str] = None

    def as_apply(self) -> WindmillApply:
        """Convert this read version of windmill to the writing version."""
        return WindmillApply(
            space=self.space,
            external_id=self.external_id,
            blades=[blade.as_apply() if isinstance(blade, DomainModel) else blade for blade in self.blades or []],
            capacity=self.capacity,
            metmast=[
                metmast.as_apply() if isinstance(metmast, DomainModel) else metmast for metmast in self.metmast or []
            ],
            nacelle=self.nacelle.as_apply() if isinstance(self.nacelle, DomainModel) else self.nacelle,
            name=self.name,
            rotor=self.rotor.as_apply() if isinstance(self.rotor, DomainModel) else self.rotor,
            windfarm=self.windfarm,
        )


class WindmillApply(DomainModelApply):
    """This represents the writing version of windmill.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the windmill.
        blades: The blade field.
        capacity: The capacity field.
        metmast: The metmast field.
        nacelle: The nacelle field.
        name: The name field.
        rotor: The rotor field.
        windfarm: The windfarm field.
        existing_version: Fail the ingestion request if the windmill version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    blades: Union[list[BladeApply], list[str], None] = Field(default=None, repr=False)
    capacity: Optional[float] = None
    metmast: Union[list[MetmastApply], list[str], None] = Field(default=None, repr=False)
    nacelle: Union[NacelleApply, str, dm.NodeId, None] = Field(None, repr=False)
    name: Optional[str] = None
    rotor: Union[RotorApply, str, dm.NodeId, None] = Field(None, repr=False)
    windfarm: Optional[str] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(Windmill, dm.ViewId("power-models", "Windmill", "1"))

        properties = {}

        if self.capacity is not None:
            properties["capacity"] = self.capacity

        if self.nacelle is not None:
            properties["nacelle"] = {
                "space": self.space if isinstance(self.nacelle, str) else self.nacelle.space,
                "externalId": self.nacelle if isinstance(self.nacelle, str) else self.nacelle.external_id,
            }

        if self.name is not None:
            properties["name"] = self.name

        if self.rotor is not None:
            properties["rotor"] = {
                "space": self.space if isinstance(self.rotor, str) else self.rotor.space,
                "externalId": self.rotor if isinstance(self.rotor, str) else self.rotor.external_id,
            }

        if self.windfarm is not None:
            properties["windfarm"] = self.windfarm

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        edge_type = dm.DirectRelationReference("power-models", "Windmill.blades")
        for blade in self.blades or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, start_node=self, end_node=blade, edge_type=edge_type, view_by_read_class=view_by_read_class
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("power-models", "Windmill.metmast")
        for metmast in self.metmast or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, start_node=self, end_node=metmast, edge_type=edge_type, view_by_read_class=view_by_read_class
            )
            resources.extend(other_resources)

        if isinstance(self.nacelle, DomainModelApply):
            other_resources = self.nacelle._to_instances_apply(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.rotor, DomainModelApply):
            other_resources = self.rotor._to_instances_apply(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class WindmillList(DomainModelList[Windmill]):
    """List of windmills in the read version."""

    _INSTANCE = Windmill

    def as_apply(self) -> WindmillApplyList:
        """Convert these read versions of windmill to the writing versions."""
        return WindmillApplyList([node.as_apply() for node in self.data])


class WindmillApplyList(DomainModelApplyList[WindmillApply]):
    """List of windmills in the writing version."""

    _INSTANCE = WindmillApply


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
    filters = []
    if min_capacity or max_capacity:
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
    if name is not None and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
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
    if windfarm is not None and isinstance(windfarm, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("windfarm"), value=windfarm))
    if windfarm and isinstance(windfarm, list):
        filters.append(dm.filters.In(view_id.as_property_ref("windfarm"), values=windfarm))
    if windfarm_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("windfarm"), value=windfarm_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
