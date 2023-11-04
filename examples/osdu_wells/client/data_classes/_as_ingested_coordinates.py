from __future__ import annotations

from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._features import FeaturesApply

__all__ = [
    "AsIngestedCoordinates",
    "AsIngestedCoordinatesApply",
    "AsIngestedCoordinatesList",
    "AsIngestedCoordinatesApplyList",
    "AsIngestedCoordinatesFields",
    "AsIngestedCoordinatesTextFields",
]


AsIngestedCoordinatesTextFields = Literal[
    "coordinate_reference_system_id",
    "vertical_coordinate_reference_system_id",
    "vertical_unit_id",
    "persistable_reference_crs",
    "persistable_reference_unit_z",
    "persistable_reference_vertical_crs",
    "type",
]
AsIngestedCoordinatesFields = Literal[
    "coordinate_reference_system_id",
    "vertical_coordinate_reference_system_id",
    "vertical_unit_id",
    "bbox",
    "persistable_reference_crs",
    "persistable_reference_unit_z",
    "persistable_reference_vertical_crs",
    "type",
]

_ASINGESTEDCOORDINATES_PROPERTIES_BY_FIELD = {
    "coordinate_reference_system_id": "CoordinateReferenceSystemID",
    "vertical_coordinate_reference_system_id": "VerticalCoordinateReferenceSystemID",
    "vertical_unit_id": "VerticalUnitID",
    "bbox": "bbox",
    "persistable_reference_crs": "persistableReferenceCrs",
    "persistable_reference_unit_z": "persistableReferenceUnitZ",
    "persistable_reference_vertical_crs": "persistableReferenceVerticalCrs",
    "type": "type",
}


class AsIngestedCoordinates(DomainModel):
    """This represent a read version of as ingested coordinate.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the as ingested coordinate.
        coordinate_reference_system_id: The coordinate reference system id field.
        vertical_coordinate_reference_system_id: The vertical coordinate reference system id field.
        vertical_unit_id: The vertical unit id field.
        bbox: The bbox field.
        features: The feature field.
        persistable_reference_crs: The persistable reference cr field.
        persistable_reference_unit_z: The persistable reference unit z field.
        persistable_reference_vertical_crs: The persistable reference vertical cr field.
        type: The type field.
        created_time: The created time of the as ingested coordinate node.
        last_updated_time: The last updated time of the as ingested coordinate node.
        deleted_time: If present, the deleted time of the as ingested coordinate node.
        version: The version of the as ingested coordinate node.
    """

    space: str = "IntegrationTestsImmutable"
    coordinate_reference_system_id: Optional[str] = Field(None, alias="CoordinateReferenceSystemID")
    vertical_coordinate_reference_system_id: Optional[str] = Field(None, alias="VerticalCoordinateReferenceSystemID")
    vertical_unit_id: Optional[str] = Field(None, alias="VerticalUnitID")
    bbox: Optional[list[float]] = None
    features: Optional[list[str]] = None
    persistable_reference_crs: Optional[str] = Field(None, alias="persistableReferenceCrs")
    persistable_reference_unit_z: Optional[str] = Field(None, alias="persistableReferenceUnitZ")
    persistable_reference_vertical_crs: Optional[str] = Field(None, alias="persistableReferenceVerticalCrs")
    type: Optional[str] = None

    def as_apply(self) -> AsIngestedCoordinatesApply:
        """Convert this read version of as ingested coordinate to a write version."""
        return AsIngestedCoordinatesApply(
            space=self.space,
            external_id=self.external_id,
            coordinate_reference_system_id=self.coordinate_reference_system_id,
            vertical_coordinate_reference_system_id=self.vertical_coordinate_reference_system_id,
            vertical_unit_id=self.vertical_unit_id,
            bbox=self.bbox,
            features=self.features,
            persistable_reference_crs=self.persistable_reference_crs,
            persistable_reference_unit_z=self.persistable_reference_unit_z,
            persistable_reference_vertical_crs=self.persistable_reference_vertical_crs,
            type=self.type,
        )


class AsIngestedCoordinatesApply(DomainModelApply):
    """This represent a write version of as ingested coordinate.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the as ingested coordinate.
        coordinate_reference_system_id: The coordinate reference system id field.
        vertical_coordinate_reference_system_id: The vertical coordinate reference system id field.
        vertical_unit_id: The vertical unit id field.
        bbox: The bbox field.
        features: The feature field.
        persistable_reference_crs: The persistable reference cr field.
        persistable_reference_unit_z: The persistable reference unit z field.
        persistable_reference_vertical_crs: The persistable reference vertical cr field.
        type: The type field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    coordinate_reference_system_id: Optional[str] = Field(None, alias="CoordinateReferenceSystemID")
    vertical_coordinate_reference_system_id: Optional[str] = Field(None, alias="VerticalCoordinateReferenceSystemID")
    vertical_unit_id: Optional[str] = Field(None, alias="VerticalUnitID")
    bbox: Optional[list[float]] = None
    features: Union[list[FeaturesApply], list[str], None] = Field(default=None, repr=False)
    persistable_reference_crs: Optional[str] = Field(None, alias="persistableReferenceCrs")
    persistable_reference_unit_z: Optional[str] = Field(None, alias="persistableReferenceUnitZ")
    persistable_reference_vertical_crs: Optional[str] = Field(None, alias="persistableReferenceVerticalCrs")
    type: Optional[str] = None

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.coordinate_reference_system_id is not None:
            properties["CoordinateReferenceSystemID"] = self.coordinate_reference_system_id
        if self.vertical_coordinate_reference_system_id is not None:
            properties["VerticalCoordinateReferenceSystemID"] = self.vertical_coordinate_reference_system_id
        if self.vertical_unit_id is not None:
            properties["VerticalUnitID"] = self.vertical_unit_id
        if self.bbox is not None:
            properties["bbox"] = self.bbox
        if self.persistable_reference_crs is not None:
            properties["persistableReferenceCrs"] = self.persistable_reference_crs
        if self.persistable_reference_unit_z is not None:
            properties["persistableReferenceUnitZ"] = self.persistable_reference_unit_z
        if self.persistable_reference_vertical_crs is not None:
            properties["persistableReferenceVerticalCrs"] = self.persistable_reference_vertical_crs
        if self.type is not None:
            properties["type"] = self.type
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "AsIngestedCoordinates", "da1e4eb90494da"),
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

        for feature in self.features or []:
            edge = self._create_feature_edge(feature)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(feature, DomainModelApply):
                instances = feature._to_instances_apply(cache, view_by_write_class)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_feature_edge(self, feature: Union[str, FeaturesApply]) -> dm.EdgeApply:
        if isinstance(feature, str):
            end_node_ext_id = feature
        elif isinstance(feature, DomainModelApply):
            end_node_ext_id = feature.external_id
        else:
            raise TypeError(f"Expected str or FeaturesApply, got {type(feature)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "AsIngestedCoordinates.features"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )


class AsIngestedCoordinatesList(TypeList[AsIngestedCoordinates]):
    """List of as ingested coordinates in read version."""

    _NODE = AsIngestedCoordinates

    def as_apply(self) -> AsIngestedCoordinatesApplyList:
        """Convert this read version of as ingested coordinate to a write version."""
        return AsIngestedCoordinatesApplyList([node.as_apply() for node in self.data])


class AsIngestedCoordinatesApplyList(TypeApplyList[AsIngestedCoordinatesApply]):
    """List of as ingested coordinates in write version."""

    _NODE = AsIngestedCoordinatesApply
