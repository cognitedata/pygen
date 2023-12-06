from __future__ import annotations

from typing import Literal, Optional

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


__all__ = ["Geometry", "GeometryApply", "GeometryList", "GeometryApplyList", "GeometryFields", "GeometryTextFields"]


GeometryTextFields = Literal["type_"]
GeometryFields = Literal["bbox", "coordinates", "type_"]

_GEOMETRY_PROPERTIES_BY_FIELD = {
    "bbox": "bbox",
    "coordinates": "coordinates",
    "type_": "type",
}


class Geometry(DomainModel):
    """This represents the reading version of geometry.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the geometry.
        bbox: The bbox field.
        coordinates: The coordinate field.
        type_: The type field.
        created_time: The created time of the geometry node.
        last_updated_time: The last updated time of the geometry node.
        deleted_time: If present, the deleted time of the geometry node.
        version: The version of the geometry node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    bbox: Optional[list[float]] = None
    coordinates: Optional[list[float]] = None
    type_: Optional[str] = Field(None, alias="type")

    def as_apply(self) -> GeometryApply:
        """Convert this read version of geometry to the writing version."""
        return GeometryApply(
            space=self.space,
            external_id=self.external_id,
            bbox=self.bbox,
            coordinates=self.coordinates,
            type_=self.type_,
        )


class GeometryApply(DomainModelApply):
    """This represents the writing version of geometry.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the geometry.
        bbox: The bbox field.
        coordinates: The coordinate field.
        type_: The type field.
        existing_version: Fail the ingestion request if the geometry version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    bbox: Optional[list[float]] = None
    coordinates: Optional[list[float]] = None
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
            "IntegrationTestsImmutable", "Geometry", "fc702ec6877c79"
        )

        properties = {}
        if self.bbox is not None:
            properties["bbox"] = self.bbox
        if self.coordinates is not None:
            properties["coordinates"] = self.coordinates
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

        return resources


class GeometryList(DomainModelList[Geometry]):
    """List of geometries in the read version."""

    _INSTANCE = Geometry

    def as_apply(self) -> GeometryApplyList:
        """Convert these read versions of geometry to the writing versions."""
        return GeometryApplyList([node.as_apply() for node in self.data])


class GeometryApplyList(DomainModelApplyList[GeometryApply]):
    """List of geometries in the writing version."""

    _INSTANCE = GeometryApply


def _create_geometry_filter(
    view_id: dm.ViewId,
    type_: str | list[str] | None = None,
    type_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
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
