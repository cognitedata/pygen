from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "GeoContexts",
    "GeoContextsApply",
    "GeoContextsList",
    "GeoContextsApplyList",
    "GeoContextsFields",
    "GeoContextsTextFields",
]


GeoContextsTextFields = Literal[
    "basin_id", "field_id", "geo_political_entity_id", "geo_type_id", "play_id", "prospect_id"
]
GeoContextsFields = Literal["basin_id", "field_id", "geo_political_entity_id", "geo_type_id", "play_id", "prospect_id"]

_GEOCONTEXTS_PROPERTIES_BY_FIELD = {
    "basin_id": "BasinID",
    "field_id": "FieldID",
    "geo_political_entity_id": "GeoPoliticalEntityID",
    "geo_type_id": "GeoTypeID",
    "play_id": "PlayID",
    "prospect_id": "ProspectID",
}


class GeoContexts(DomainModel):
    """This represent a read version of geo context.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the geo context.
        basin_id: The basin id field.
        field_id: The field id field.
        geo_political_entity_id: The geo political entity id field.
        geo_type_id: The geo type id field.
        play_id: The play id field.
        prospect_id: The prospect id field.
        created_time: The created time of the geo context node.
        last_updated_time: The last updated time of the geo context node.
        deleted_time: If present, the deleted time of the geo context node.
        version: The version of the geo context node.
    """

    space: str = "IntegrationTestsImmutable"
    basin_id: Optional[str] = Field(None, alias="BasinID")
    field_id: Optional[str] = Field(None, alias="FieldID")
    geo_political_entity_id: Optional[str] = Field(None, alias="GeoPoliticalEntityID")
    geo_type_id: Optional[str] = Field(None, alias="GeoTypeID")
    play_id: Optional[str] = Field(None, alias="PlayID")
    prospect_id: Optional[str] = Field(None, alias="ProspectID")

    def as_apply(self) -> GeoContextsApply:
        """Convert this read version of geo context to a write version."""
        return GeoContextsApply(
            space=self.space,
            external_id=self.external_id,
            basin_id=self.basin_id,
            field_id=self.field_id,
            geo_political_entity_id=self.geo_political_entity_id,
            geo_type_id=self.geo_type_id,
            play_id=self.play_id,
            prospect_id=self.prospect_id,
        )


class GeoContextsApply(DomainModelApply):
    """This represent a write version of geo context.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the geo context.
        basin_id: The basin id field.
        field_id: The field id field.
        geo_political_entity_id: The geo political entity id field.
        geo_type_id: The geo type id field.
        play_id: The play id field.
        prospect_id: The prospect id field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    basin_id: Optional[str] = Field(None, alias="BasinID")
    field_id: Optional[str] = Field(None, alias="FieldID")
    geo_political_entity_id: Optional[str] = Field(None, alias="GeoPoliticalEntityID")
    geo_type_id: Optional[str] = Field(None, alias="GeoTypeID")
    play_id: Optional[str] = Field(None, alias="PlayID")
    prospect_id: Optional[str] = Field(None, alias="ProspectID")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.basin_id is not None:
            properties["BasinID"] = self.basin_id
        if self.field_id is not None:
            properties["FieldID"] = self.field_id
        if self.geo_political_entity_id is not None:
            properties["GeoPoliticalEntityID"] = self.geo_political_entity_id
        if self.geo_type_id is not None:
            properties["GeoTypeID"] = self.geo_type_id
        if self.play_id is not None:
            properties["PlayID"] = self.play_id
        if self.prospect_id is not None:
            properties["ProspectID"] = self.prospect_id
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "GeoContexts", "cec36d5139aade"),
                properties=properties,
            )
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[source],
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class GeoContextsList(TypeList[GeoContexts]):
    """List of geo contexts in read version."""

    _NODE = GeoContexts

    def as_apply(self) -> GeoContextsApplyList:
        """Convert this read version of geo context to a write version."""
        return GeoContextsApplyList([node.as_apply() for node in self.data])


class GeoContextsApplyList(TypeApplyList[GeoContextsApply]):
    """List of geo contexts in write version."""

    _NODE = GeoContextsApply
