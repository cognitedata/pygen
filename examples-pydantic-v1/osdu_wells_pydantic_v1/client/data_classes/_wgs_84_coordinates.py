from __future__ import annotations

from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._features import FeaturesApply

__all__ = [
    "WgsCoordinates",
    "WgsCoordinatesApply",
    "WgsCoordinatesList",
    "WgsCoordinatesApplyList",
    "WgsCoordinatesFields",
    "WgsCoordinatesTextFields",
]


WgsCoordinatesTextFields = Literal["type"]
WgsCoordinatesFields = Literal["bbox", "type"]

_WGSCOORDINATES_PROPERTIES_BY_FIELD = {
    "bbox": "bbox",
    "type": "type",
}


class WgsCoordinates(DomainModel):
    space: str = "IntegrationTestsImmutable"
    bbox: Optional[list[float]] = None
    features: Optional[list[str]] = None
    type: Optional[str] = None

    def as_apply(self) -> WgsCoordinatesApply:
        return WgsCoordinatesApply(
            space=self.space,
            external_id=self.external_id,
            bbox=self.bbox,
            features=self.features,
            type=self.type,
        )


class WgsCoordinatesApply(DomainModelApply):
    space: str = "IntegrationTestsImmutable"
    bbox: Optional[list[float]] = None
    features: Union[list[FeaturesApply], list[str], None] = Field(default=None, repr=False)
    type: Optional[str] = None

    def _to_instances_apply(self, cache: set[str], write_view: dm.ViewId | None) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        properties = {}
        if self.bbox is not None:
            properties["bbox"] = self.bbox
        if self.type is not None:
            properties["type"] = self.type
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "Wgs84Coordinates", "d6030081373896"),
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
                instances = feature._to_instances_apply(cache, write_view)
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
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "Wgs84Coordinates.features"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )


class WgsCoordinatesList(TypeList[WgsCoordinates]):
    _NODE = WgsCoordinates

    def as_apply(self) -> WgsCoordinatesApplyList:
        return WgsCoordinatesApplyList([node.as_apply() for node in self.data])


class WgsCoordinatesApplyList(TypeApplyList[WgsCoordinatesApply]):
    _NODE = WgsCoordinatesApply
