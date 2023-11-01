from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = ["Geometry", "GeometryApply", "GeometryList", "GeometryApplyList", "GeometryFields", "GeometryTextFields"]


GeometryTextFields = Literal["type"]
GeometryFields = Literal["bbox", "coordinates", "type"]

_GEOMETRY_PROPERTIES_BY_FIELD = {
    "bbox": "bbox",
    "coordinates": "coordinates",
    "type": "type",
}


class Geometry(DomainModel):
    space: str = "IntegrationTestsImmutable"
    bbox: Optional[list[float]] = None
    coordinates: Optional[list[float]] = None
    type: Optional[str] = None

    def as_apply(self) -> GeometryApply:
        return GeometryApply(
            space=self.space,
            external_id=self.external_id,
            bbox=self.bbox,
            coordinates=self.coordinates,
            type=self.type,
        )


class GeometryApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    bbox: Optional[list[float]] = None
    coordinates: Optional[list[float]] = None
    type: Optional[str] = None

    def _to_instances_apply(self, cache: set[str], write_view: dm.ViewId | None) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        properties = {}
        if self.bbox is not None:
            properties["bbox"] = self.bbox
        if self.coordinates is not None:
            properties["coordinates"] = self.coordinates
        if self.type is not None:
            properties["type"] = self.type
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "Geometry", "fc702ec6877c79"),
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


class GeometryList(TypeList[Geometry]):
    _NODE = Geometry

    def as_apply(self) -> GeometryApplyList:
        return GeometryApplyList([node.as_apply() for node in self.data])


class GeometryApplyList(TypeApplyList[GeometryApply]):
    _NODE = GeometryApply
