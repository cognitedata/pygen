from __future__ import annotations

from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._geometry import GeometryApply

__all__ = ["Features", "FeaturesApply", "FeaturesList", "FeaturesApplyList", "FeaturesFields", "FeaturesTextFields"]


FeaturesTextFields = Literal["type"]
FeaturesFields = Literal["bbox", "type"]

_FEATURES_PROPERTIES_BY_FIELD = {
    "bbox": "bbox",
    "type": "type",
}


class Features(DomainModel):
    """This represent a read version of feature.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the feature.
        bbox: The bbox field.
        geometry: The geometry field.
        type: The type field.
        created_time: The created time of the feature node.
        last_updated_time: The last updated time of the feature node.
        deleted_time: If present, the deleted time of the feature node.
        version: The version of the feature node.
    """

    space: str = "IntegrationTestsImmutable"
    bbox: Optional[list[float]] = None
    geometry: Optional[str] = None
    type: Optional[str] = None

    def as_apply(self) -> FeaturesApply:
        """Convert this read version of feature to a write version."""
        return FeaturesApply(
            space=self.space,
            external_id=self.external_id,
            bbox=self.bbox,
            geometry=self.geometry,
            type=self.type,
        )


class FeaturesApply(DomainModelApply):
    """This represent a write version of feature.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the feature.
        bbox: The bbox field.
        geometry: The geometry field.
        type: The type field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    bbox: Optional[list[float]] = None
    geometry: Union[GeometryApply, str, None] = Field(None, repr=False)
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
        if self.geometry is not None:
            properties["geometry"] = {
                "space": self.space if isinstance(self.geometry, str) else self.geometry.space,
                "externalId": self.geometry if isinstance(self.geometry, str) else self.geometry.external_id,
            }
        if self.type is not None:
            properties["type"] = self.type
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "Features", "df91e0a3bad68c"),
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

        if isinstance(self.geometry, DomainModelApply):
            instances = self.geometry._to_instances_apply(cache, view_by_write_class)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class FeaturesList(TypeList[Features]):
    """List of features in read version."""

    _NODE = Features

    def as_apply(self) -> FeaturesApplyList:
        """Convert this read version of feature to a write version."""
        return FeaturesApplyList([node.as_apply() for node in self.data])


class FeaturesApplyList(TypeApplyList[FeaturesApply]):
    """List of features in write version."""

    _NODE = FeaturesApply
