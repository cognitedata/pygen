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
    """This represent a read version of geometry.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the geometry.
        bbox: The bbox field.
        coordinates: The coordinate field.
        type: The type field.
        created_time: The created time of the geometry node.
        last_updated_time: The last updated time of the geometry node.
        deleted_time: If present, the deleted time of the geometry node.
        version: The version of the geometry node.
    """

    space: str = "IntegrationTestsImmutable"
    bbox: Optional[list[float]] = None
    coordinates: Optional[list[float]] = None
    type: Optional[str] = None

    def as_apply(self) -> GeometryApply:
        """Convert this read version of geometry to a write version."""
        return GeometryApply(
            space=self.space,
            external_id=self.external_id,
            bbox=self.bbox,
            coordinates=self.coordinates,
            type=self.type,
        )


class GeometryApply(DomainModelApply):
    """This represent a write version of geometry.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the geometry.
        bbox: The bbox field.
        coordinates: The coordinate field.
        type: The type field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    bbox: Optional[list[float]] = None
    coordinates: Optional[list[float]] = None
    type: Optional[str] = None

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

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
    """List of geometries in read version."""

    _NODE = Geometry

    def as_apply(self) -> GeometryApplyList:
        """Convert this read version of geometry to a write version."""
        return GeometryApplyList([node.as_apply() for node in self.data])


class GeometryApplyList(TypeApplyList[GeometryApply]):
    """List of geometries in write version."""

    _NODE = GeometryApply
