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
    from ._geometry import Geometry, GeometryApply


__all__ = ["Features", "FeaturesApply", "FeaturesList", "FeaturesApplyList", "FeaturesFields", "FeaturesTextFields"]


FeaturesTextFields = Literal["type_"]
FeaturesFields = Literal["bbox", "type_"]

_FEATURES_PROPERTIES_BY_FIELD = {
    "bbox": "bbox",
    "type_": "type",
}


class Features(DomainModel):
    """This represents the reading version of feature.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the feature.
        bbox: The bbox field.
        geometry: The geometry field.
        type_: The type field.
        created_time: The created time of the feature node.
        last_updated_time: The last updated time of the feature node.
        deleted_time: If present, the deleted time of the feature node.
        version: The version of the feature node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    bbox: Optional[list[float]] = None
    geometry: Union[Geometry, str, dm.NodeId, None] = Field(None, repr=False)
    type_: Optional[str] = Field(None, alias="type")

    def as_apply(self) -> FeaturesApply:
        """Convert this read version of feature to the writing version."""
        return FeaturesApply(
            space=self.space,
            external_id=self.external_id,
            bbox=self.bbox,
            geometry=self.geometry.as_apply() if isinstance(self.geometry, DomainModel) else self.geometry,
            type_=self.type_,
        )


class FeaturesApply(DomainModelApply):
    """This represents the writing version of feature.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the feature.
        bbox: The bbox field.
        geometry: The geometry field.
        type_: The type field.
        existing_version: Fail the ingestion request if the feature version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    bbox: Optional[list[float]] = None
    geometry: Union[GeometryApply, str, dm.NodeId, None] = Field(None, repr=False)
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
            "IntegrationTestsImmutable", "Features", "df91e0a3bad68c"
        )

        properties = {}
        if self.bbox is not None:
            properties["bbox"] = self.bbox
        if self.geometry is not None:
            properties["geometry"] = {
                "space": self.space if isinstance(self.geometry, str) else self.geometry.space,
                "externalId": self.geometry if isinstance(self.geometry, str) else self.geometry.external_id,
            }
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

        if isinstance(self.geometry, DomainModelApply):
            other_resources = self.geometry._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        return resources


class FeaturesList(DomainModelList[Features]):
    """List of features in the read version."""

    _INSTANCE = Features

    def as_apply(self) -> FeaturesApplyList:
        """Convert these read versions of feature to the writing versions."""
        return FeaturesApplyList([node.as_apply() for node in self.data])


class FeaturesApplyList(DomainModelApplyList[FeaturesApply]):
    """List of features in the writing version."""

    _INSTANCE = FeaturesApply


def _create_feature_filter(
    view_id: dm.ViewId,
    geometry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    type_: str | list[str] | None = None,
    type_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if geometry and isinstance(geometry, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("geometry"),
                value={"space": "IntegrationTestsImmutable", "externalId": geometry},
            )
        )
    if geometry and isinstance(geometry, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("geometry"), value={"space": geometry[0], "externalId": geometry[1]}
            )
        )
    if geometry and isinstance(geometry, list) and isinstance(geometry[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("geometry"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in geometry],
            )
        )
    if geometry and isinstance(geometry, list) and isinstance(geometry[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("geometry"),
                values=[{"space": item[0], "externalId": item[1]} for item in geometry],
            )
        )
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
