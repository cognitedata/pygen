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
    space: str = "IntegrationTestsImmutable"
    basin_id: Optional[str] = Field(None, alias="BasinID")
    field_id: Optional[str] = Field(None, alias="FieldID")
    geo_political_entity_id: Optional[str] = Field(None, alias="GeoPoliticalEntityID")
    geo_type_id: Optional[str] = Field(None, alias="GeoTypeID")
    play_id: Optional[str] = Field(None, alias="PlayID")
    prospect_id: Optional[str] = Field(None, alias="ProspectID")

    def as_apply(self) -> GeoContextsApply:
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
    space: str = "IntegrationTestsImmutable"
    basin_id: Optional[str] = Field(None, alias="BasinID")
    field_id: Optional[str] = Field(None, alias="FieldID")
    geo_political_entity_id: Optional[str] = Field(None, alias="GeoPoliticalEntityID")
    geo_type_id: Optional[str] = Field(None, alias="GeoTypeID")
    play_id: Optional[str] = Field(None, alias="PlayID")
    prospect_id: Optional[str] = Field(None, alias="ProspectID")

    def _to_instances_apply(self, cache: set[str], write_view: dm.ViewId | None) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

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
    _NODE = GeoContexts

    def as_apply(self) -> GeoContextsApplyList:
        return GeoContextsApplyList([node.as_apply() for node in self.data])


class GeoContextsApplyList(TypeApplyList[GeoContextsApply]):
    _NODE = GeoContextsApply
