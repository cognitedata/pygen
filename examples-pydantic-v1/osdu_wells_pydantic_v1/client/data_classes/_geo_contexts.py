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


__all__ = [
    "GeoContexts",
    "GeoContextsApply",
    "GeoContextsList",
    "GeoContextsApplyList",
    "GeoContextsFields",
    "GeoContextsTextFields",
]


GeoContextsTextFields = Literal[
    "basin_id", "field_id", "geo_political_entity_id", "geo_type_id", "play_id", "prospect_id"
]
GeoContextsFields = Literal["basin_id", "field_id", "geo_political_entity_id", "geo_type_id", "play_id", "prospect_id"]

_GEOCONTEXTS_PROPERTIES_BY_FIELD = {
    "basin_id": "BasinID",
    "field_id": "FieldID",
    "geo_political_entity_id": "GeoPoliticalEntityID",
    "geo_type_id": "GeoTypeID",
    "play_id": "PlayID",
    "prospect_id": "ProspectID",
}


class GeoContexts(DomainModel):
    """This represents the reading version of geo context.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the geo context.
        basin_id: The basin id field.
        field_id: The field id field.
        geo_political_entity_id: The geo political entity id field.
        geo_type_id: The geo type id field.
        play_id: The play id field.
        prospect_id: The prospect id field.
        created_time: The created time of the geo context node.
        last_updated_time: The last updated time of the geo context node.
        deleted_time: If present, the deleted time of the geo context node.
        version: The version of the geo context node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    basin_id: Optional[str] = Field(None, alias="BasinID")
    field_id: Optional[str] = Field(None, alias="FieldID")
    geo_political_entity_id: Optional[str] = Field(None, alias="GeoPoliticalEntityID")
    geo_type_id: Optional[str] = Field(None, alias="GeoTypeID")
    play_id: Optional[str] = Field(None, alias="PlayID")
    prospect_id: Optional[str] = Field(None, alias="ProspectID")

    def as_apply(self) -> GeoContextsApply:
        """Convert this read version of geo context to the writing version."""
        return GeoContextsApply(
            space=self.space,
            external_id=self.external_id,
            basin_id=self.basin_id,
            field_id=self.field_id,
            geo_political_entity_id=self.geo_political_entity_id,
            geo_type_id=self.geo_type_id,
            play_id=self.play_id,
            prospect_id=self.prospect_id,
        )


class GeoContextsApply(DomainModelApply):
    """This represents the writing version of geo context.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the geo context.
        basin_id: The basin id field.
        field_id: The field id field.
        geo_political_entity_id: The geo political entity id field.
        geo_type_id: The geo type id field.
        play_id: The play id field.
        prospect_id: The prospect id field.
        existing_version: Fail the ingestion request if the geo context version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    basin_id: Optional[str] = Field(None, alias="BasinID")
    field_id: Optional[str] = Field(None, alias="FieldID")
    geo_political_entity_id: Optional[str] = Field(None, alias="GeoPoliticalEntityID")
    geo_type_id: Optional[str] = Field(None, alias="GeoTypeID")
    play_id: Optional[str] = Field(None, alias="PlayID")
    prospect_id: Optional[str] = Field(None, alias="ProspectID")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "GeoContexts", "cec36d5139aade"
        )

        properties = {}
        if self.basin_id is not None:
            properties["BasinID"] = self.basin_id
        if self.field_id is not None:
            properties["FieldID"] = self.field_id
        if self.geo_political_entity_id is not None:
            properties["GeoPoliticalEntityID"] = self.geo_political_entity_id
        if self.geo_type_id is not None:
            properties["GeoTypeID"] = self.geo_type_id
        if self.play_id is not None:
            properties["PlayID"] = self.play_id
        if self.prospect_id is not None:
            properties["ProspectID"] = self.prospect_id

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=dm.DirectRelationReference("IntegrationTestsImmutable", "GeoContexts"),
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


class GeoContextsList(DomainModelList[GeoContexts]):
    """List of geo contexts in the read version."""

    _INSTANCE = GeoContexts

    def as_apply(self) -> GeoContextsApplyList:
        """Convert these read versions of geo context to the writing versions."""
        return GeoContextsApplyList([node.as_apply() for node in self.data])


class GeoContextsApplyList(DomainModelApplyList[GeoContextsApply]):
    """List of geo contexts in the writing version."""

    _INSTANCE = GeoContextsApply


def _create_geo_context_filter(
    view_id: dm.ViewId,
    basin_id: str | list[str] | None = None,
    basin_id_prefix: str | None = None,
    field_id: str | list[str] | None = None,
    field_id_prefix: str | None = None,
    geo_political_entity_id: str | list[str] | None = None,
    geo_political_entity_id_prefix: str | None = None,
    geo_type_id: str | list[str] | None = None,
    geo_type_id_prefix: str | None = None,
    play_id: str | list[str] | None = None,
    play_id_prefix: str | None = None,
    prospect_id: str | list[str] | None = None,
    prospect_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if basin_id is not None and isinstance(basin_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("BasinID"), value=basin_id))
    if basin_id and isinstance(basin_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("BasinID"), values=basin_id))
    if basin_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("BasinID"), value=basin_id_prefix))
    if field_id is not None and isinstance(field_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("FieldID"), value=field_id))
    if field_id and isinstance(field_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("FieldID"), values=field_id))
    if field_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("FieldID"), value=field_id_prefix))
    if geo_political_entity_id is not None and isinstance(geo_political_entity_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("GeoPoliticalEntityID"), value=geo_political_entity_id)
        )
    if geo_political_entity_id and isinstance(geo_political_entity_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("GeoPoliticalEntityID"), values=geo_political_entity_id))
    if geo_political_entity_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("GeoPoliticalEntityID"), value=geo_political_entity_id_prefix)
        )
    if geo_type_id is not None and isinstance(geo_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("GeoTypeID"), value=geo_type_id))
    if geo_type_id and isinstance(geo_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("GeoTypeID"), values=geo_type_id))
    if geo_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("GeoTypeID"), value=geo_type_id_prefix))
    if play_id is not None and isinstance(play_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("PlayID"), value=play_id))
    if play_id and isinstance(play_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("PlayID"), values=play_id))
    if play_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("PlayID"), value=play_id_prefix))
    if prospect_id is not None and isinstance(prospect_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ProspectID"), value=prospect_id))
    if prospect_id and isinstance(prospect_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ProspectID"), values=prospect_id))
    if prospect_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ProspectID"), value=prospect_id_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
