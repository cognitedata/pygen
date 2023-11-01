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
    space: str = "IntegrationTestsImmutable"
    bbox: Optional[list[float]] = None
    geometry: Optional[str] = None
    type: Optional[str] = None

    def as_apply(self) -> FeaturesApply:
        return FeaturesApply(
            space=self.space,
            external_id=self.external_id,
            bbox=self.bbox,
            geometry=self.geometry,
            type=self.type,
        )


class FeaturesApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    bbox: Optional[list[float]] = None
    geometry: Union[GeometryApply, str, None] = Field(None, repr=False)
    type: Optional[str] = None

    def _to_instances_apply(self, cache: set[str], write_view: dm.ViewId | None) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        properties = {}
        if self.bbox is not None:
            properties["bbox"] = self.bbox
        if self.geometry is not None:
            properties["geometry"] = {
                "space": "IntegrationTestsImmutable",
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
            instances = self.geometry._to_instances_apply(cache, write_view)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class FeaturesList(TypeList[Features]):
    _NODE = Features

    def as_apply(self) -> FeaturesApplyList:
        return FeaturesApplyList([node.as_apply() for node in self.data])


class FeaturesApplyList(TypeApplyList[FeaturesApply]):
    _NODE = FeaturesApply
