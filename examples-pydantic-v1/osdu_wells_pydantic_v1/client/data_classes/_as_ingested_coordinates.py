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
    "type_",
]
AsIngestedCoordinatesFields = Literal[
    "coordinate_reference_system_id",
    "vertical_coordinate_reference_system_id",
    "vertical_unit_id",
    "bbox",
    "persistable_reference_crs",
    "persistable_reference_unit_z",
    "persistable_reference_vertical_crs",
    "type_",
]

_ASINGESTEDCOORDINATES_PROPERTIES_BY_FIELD = {
    "coordinate_reference_system_id": "CoordinateReferenceSystemID",
    "vertical_coordinate_reference_system_id": "VerticalCoordinateReferenceSystemID",
    "vertical_unit_id": "VerticalUnitID",
    "bbox": "bbox",
    "persistable_reference_crs": "persistableReferenceCrs",
    "persistable_reference_unit_z": "persistableReferenceUnitZ",
    "persistable_reference_vertical_crs": "persistableReferenceVerticalCrs",
    "type_": "type",
}


class AsIngestedCoordinates(DomainModel):
    """This represents the reading version of as ingested coordinate.

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
        type_: The type field.
        created_time: The created time of the as ingested coordinate node.
        last_updated_time: The last updated time of the as ingested coordinate node.
        deleted_time: If present, the deleted time of the as ingested coordinate node.
        version: The version of the as ingested coordinate node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    coordinate_reference_system_id: Optional[str] = Field(None, alias="CoordinateReferenceSystemID")
    vertical_coordinate_reference_system_id: Optional[str] = Field(None, alias="VerticalCoordinateReferenceSystemID")
    vertical_unit_id: Optional[str] = Field(None, alias="VerticalUnitID")
    bbox: Optional[list[float]] = None
    features: Union[list[Features], list[str], None] = Field(default=None, repr=False)
    persistable_reference_crs: Optional[str] = Field(None, alias="persistableReferenceCrs")
    persistable_reference_unit_z: Optional[str] = Field(None, alias="persistableReferenceUnitZ")
    persistable_reference_vertical_crs: Optional[str] = Field(None, alias="persistableReferenceVerticalCrs")
    type_: Optional[str] = Field(None, alias="type")

    def as_apply(self) -> AsIngestedCoordinatesApply:
        """Convert this read version of as ingested coordinate to the writing version."""
        return AsIngestedCoordinatesApply(
            space=self.space,
            external_id=self.external_id,
            coordinate_reference_system_id=self.coordinate_reference_system_id,
            vertical_coordinate_reference_system_id=self.vertical_coordinate_reference_system_id,
            vertical_unit_id=self.vertical_unit_id,
            bbox=self.bbox,
            features=[
                feature.as_apply() if isinstance(feature, DomainModel) else feature for feature in self.features or []
            ],
            persistable_reference_crs=self.persistable_reference_crs,
            persistable_reference_unit_z=self.persistable_reference_unit_z,
            persistable_reference_vertical_crs=self.persistable_reference_vertical_crs,
            type_=self.type_,
        )


class AsIngestedCoordinatesApply(DomainModelApply):
    """This represents the writing version of as ingested coordinate.

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
        type_: The type field.
        existing_version: Fail the ingestion request if the as ingested coordinate version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    coordinate_reference_system_id: Optional[str] = Field(None, alias="CoordinateReferenceSystemID")
    vertical_coordinate_reference_system_id: Optional[str] = Field(None, alias="VerticalCoordinateReferenceSystemID")
    vertical_unit_id: Optional[str] = Field(None, alias="VerticalUnitID")
    bbox: Optional[list[float]] = None
    features: Union[list[FeaturesApply], list[str], None] = Field(default=None, repr=False)
    persistable_reference_crs: Optional[str] = Field(None, alias="persistableReferenceCrs")
    persistable_reference_unit_z: Optional[str] = Field(None, alias="persistableReferenceUnitZ")
    persistable_reference_vertical_crs: Optional[str] = Field(None, alias="persistableReferenceVerticalCrs")
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
            "IntegrationTestsImmutable", "AsIngestedCoordinates", "da1e4eb90494da"
        )

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

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "AsIngestedCoordinates.features")
        for feature in self.features or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, feature, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        return resources


class AsIngestedCoordinatesList(DomainModelList[AsIngestedCoordinates]):
    """List of as ingested coordinates in the read version."""

    _INSTANCE = AsIngestedCoordinates

    def as_apply(self) -> AsIngestedCoordinatesApplyList:
        """Convert these read versions of as ingested coordinate to the writing versions."""
        return AsIngestedCoordinatesApplyList([node.as_apply() for node in self.data])


class AsIngestedCoordinatesApplyList(DomainModelApplyList[AsIngestedCoordinatesApply]):
    """List of as ingested coordinates in the writing version."""

    _INSTANCE = AsIngestedCoordinatesApply


def _create_as_ingested_coordinate_filter(
    view_id: dm.ViewId,
    coordinate_reference_system_id: str | list[str] | None = None,
    coordinate_reference_system_id_prefix: str | None = None,
    vertical_coordinate_reference_system_id: str | list[str] | None = None,
    vertical_coordinate_reference_system_id_prefix: str | None = None,
    vertical_unit_id: str | list[str] | None = None,
    vertical_unit_id_prefix: str | None = None,
    persistable_reference_crs: str | list[str] | None = None,
    persistable_reference_crs_prefix: str | None = None,
    persistable_reference_unit_z: str | list[str] | None = None,
    persistable_reference_unit_z_prefix: str | None = None,
    persistable_reference_vertical_crs: str | list[str] | None = None,
    persistable_reference_vertical_crs_prefix: str | None = None,
    type_: str | list[str] | None = None,
    type_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if coordinate_reference_system_id and isinstance(coordinate_reference_system_id, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("CoordinateReferenceSystemID"), value=coordinate_reference_system_id
            )
        )
    if coordinate_reference_system_id and isinstance(coordinate_reference_system_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("CoordinateReferenceSystemID"), values=coordinate_reference_system_id)
        )
    if coordinate_reference_system_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("CoordinateReferenceSystemID"), value=coordinate_reference_system_id_prefix
            )
        )
    if vertical_coordinate_reference_system_id and isinstance(vertical_coordinate_reference_system_id, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("VerticalCoordinateReferenceSystemID"),
                value=vertical_coordinate_reference_system_id,
            )
        )
    if vertical_coordinate_reference_system_id and isinstance(vertical_coordinate_reference_system_id, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("VerticalCoordinateReferenceSystemID"),
                values=vertical_coordinate_reference_system_id,
            )
        )
    if vertical_coordinate_reference_system_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("VerticalCoordinateReferenceSystemID"),
                value=vertical_coordinate_reference_system_id_prefix,
            )
        )
    if vertical_unit_id and isinstance(vertical_unit_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("VerticalUnitID"), value=vertical_unit_id))
    if vertical_unit_id and isinstance(vertical_unit_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("VerticalUnitID"), values=vertical_unit_id))
    if vertical_unit_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("VerticalUnitID"), value=vertical_unit_id_prefix))
    if persistable_reference_crs and isinstance(persistable_reference_crs, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("persistableReferenceCrs"), value=persistable_reference_crs)
        )
    if persistable_reference_crs and isinstance(persistable_reference_crs, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("persistableReferenceCrs"), values=persistable_reference_crs)
        )
    if persistable_reference_crs_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("persistableReferenceCrs"), value=persistable_reference_crs_prefix
            )
        )
    if persistable_reference_unit_z and isinstance(persistable_reference_unit_z, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("persistableReferenceUnitZ"), value=persistable_reference_unit_z)
        )
    if persistable_reference_unit_z and isinstance(persistable_reference_unit_z, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("persistableReferenceUnitZ"), values=persistable_reference_unit_z)
        )
    if persistable_reference_unit_z_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("persistableReferenceUnitZ"), value=persistable_reference_unit_z_prefix
            )
        )
    if persistable_reference_vertical_crs and isinstance(persistable_reference_vertical_crs, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("persistableReferenceVerticalCrs"), value=persistable_reference_vertical_crs
            )
        )
    if persistable_reference_vertical_crs and isinstance(persistable_reference_vertical_crs, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("persistableReferenceVerticalCrs"), values=persistable_reference_vertical_crs
            )
        )
    if persistable_reference_vertical_crs_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("persistableReferenceVerticalCrs"),
                value=persistable_reference_vertical_crs_prefix,
            )
        )
    if type_ and isinstance(type_, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("type"), value=type_))
    if type_ and isinstance(type_, list):
        filters.append(dm.filters.In(view_id.as_property_ref("type"), values=type_))
    if type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("type"), value=type_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
