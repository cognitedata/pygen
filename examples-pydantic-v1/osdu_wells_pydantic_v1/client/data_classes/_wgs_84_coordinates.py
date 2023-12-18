from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)

if TYPE_CHECKING:
    from ._features import Features, FeaturesApply


__all__ = [
    "WgsCoordinates",
    "WgsCoordinatesApply",
    "WgsCoordinatesList",
    "WgsCoordinatesApplyList",
    "WgsCoordinatesFields",
    "WgsCoordinatesTextFields",
]


WgsCoordinatesTextFields = Literal["type_"]
WgsCoordinatesFields = Literal["bbox", "type_"]

_WGSCOORDINATES_PROPERTIES_BY_FIELD = {
    "bbox": "bbox",
    "type_": "type",
}


class WgsCoordinates(DomainModel):
    """This represents the reading version of wgs 84 coordinate.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the wgs 84 coordinate.
        bbox: The bbox field.
        features: The feature field.
        type_: The type field.
        created_time: The created time of the wgs 84 coordinate node.
        last_updated_time: The last updated time of the wgs 84 coordinate node.
        deleted_time: If present, the deleted time of the wgs 84 coordinate node.
        version: The version of the wgs 84 coordinate node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    bbox: Optional[list[float]] = None
    features: Union[list[Features], list[str], None] = Field(default=None, repr=False)
    type_: Optional[str] = Field(None, alias="type")

    def as_apply(self) -> WgsCoordinatesApply:
        """Convert this read version of wgs 84 coordinate to the writing version."""
        return WgsCoordinatesApply(
            space=self.space,
            external_id=self.external_id,
            bbox=self.bbox,
            features=[
                feature.as_apply() if isinstance(feature, DomainModel) else feature for feature in self.features or []
            ],
            type_=self.type_,
        )


class WgsCoordinatesApply(DomainModelApply):
    """This represents the writing version of wgs 84 coordinate.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the wgs 84 coordinate.
        bbox: The bbox field.
        features: The feature field.
        type_: The type field.
        existing_version: Fail the ingestion request if the wgs 84 coordinate version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    bbox: Optional[list[float]] = None
    features: Union[list[FeaturesApply], list[str], None] = Field(default=None, repr=False)
    type_: Optional[str] = Field(None, alias="type")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "Wgs84Coordinates", "d6030081373896"
        )

        properties = {}
        if self.bbox is not None:
            properties["bbox"] = self.bbox
        if self.type_ is not None:
            properties["type"] = self.type_

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "Wgs84Coordinates.features")
        for feature in self.features or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, start_node=self, end_node=feature, edge_type=edge_type, view_by_write_class=view_by_write_class
            )
            resources.extend(other_resources)

        return resources


class WgsCoordinatesList(DomainModelList[WgsCoordinates]):
    """List of wgs 84 coordinates in the read version."""

    _INSTANCE = WgsCoordinates

    def as_apply(self) -> WgsCoordinatesApplyList:
        """Convert these read versions of wgs 84 coordinate to the writing versions."""
        return WgsCoordinatesApplyList([node.as_apply() for node in self.data])


class WgsCoordinatesApplyList(DomainModelApplyList[WgsCoordinatesApply]):
    """List of wgs 84 coordinates in the writing version."""

    _INSTANCE = WgsCoordinatesApply


def _create_wgs_84_coordinate_filter(
    view_id: dm.ViewId,
    type_: str | list[str] | None = None,
    type_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if type_ is not None and isinstance(type_, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("type"), value=type_))
    if type_ and isinstance(type_, list):
        filters.append(dm.filters.In(view_id.as_property_ref("type"), values=type_))
    if type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("type"), value=type_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
