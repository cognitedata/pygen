from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    TimeSeries,
)

if TYPE_CHECKING:
    from ._asset import Asset, AssetApply
    from ._cdf_3_d_connection_properties import CdfConnectionProperties, CdfConnectionPropertiesApply


__all__ = ["Asset", "AssetApply", "AssetList", "AssetApplyList", "AssetFields", "AssetTextFields"]


AssetTextFields = Literal["description", "documents", "measurements", "source_db", "specification", "tag", "trajectory"]
AssetFields = Literal[
    "area_id",
    "category_id",
    "created_date",
    "description",
    "documents",
    "is_active",
    "is_critical_line",
    "measurements",
    "metrics",
    "pressure",
    "source_db",
    "specification",
    "tag",
    "trajectory",
    "updated_date",
]

_ASSET_PROPERTIES_BY_FIELD = {
    "area_id": "areaId",
    "category_id": "categoryId",
    "created_date": "createdDate",
    "description": "description",
    "documents": "documents",
    "is_active": "isActive",
    "is_critical_line": "isCriticalLine",
    "measurements": "measurements",
    "metrics": "metrics",
    "pressure": "pressure",
    "source_db": "sourceDb",
    "specification": "specification",
    "tag": "tag",
    "trajectory": "trajectory",
    "updated_date": "updatedDate",
}


class Asset(DomainModel):
    """This represents the reading version of asset.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the asset.
        area_id: @name area identification
        category_id: @name category identification
        children: The child field.
        created_date: @name created date
        description: The description field.
        documents: The document field.
        in_model_3_d: Cdf3dModel the Cdf3dEntity is part of
        is_active: @name active
        is_critical_line: @name critical line
        measurements: The measurement field.
        metrics: The metric field.
        parent: The parent field.
        pressure: The pressure field.
        source_db: @name source data
        specification: The specification field.
        tag: The tag field.
        trajectory: The trajectory field.
        updated_date: @name updated date
        created_time: The created time of the asset node.
        last_updated_time: The last updated time of the asset node.
        deleted_time: If present, the deleted time of the asset node.
        version: The version of the asset node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    area_id: Optional[int] = Field(None, alias="areaId")
    category_id: Optional[int] = Field(None, alias="categoryId")
    children: Union[list[Asset], list[str], None] = Field(default=None, repr=False)
    created_date: Optional[datetime.datetime] = Field(None, alias="createdDate")
    description: Optional[str] = None
    documents: Optional[list[str]] = None
    in_model_3_d: Optional[list[CdfConnectionProperties]] = Field(default=None, repr=False, alias="inModel3d")
    is_active: Optional[bool] = Field(None, alias="isActive")
    is_critical_line: Optional[bool] = Field(None, alias="isCriticalLine")
    measurements: Optional[list[str]] = None
    metrics: Optional[list[TimeSeries]] = None
    parent: Union[Asset, str, dm.NodeId, None] = Field(None, repr=False)
    pressure: Union[TimeSeries, str, None] = None
    source_db: Optional[str] = Field(None, alias="sourceDb")
    specification: Union[str, None] = None
    tag: Optional[str] = None
    trajectory: Union[str, None] = None
    updated_date: Optional[datetime.datetime] = Field(None, alias="updatedDate")

    def as_apply(self) -> AssetApply:
        """Convert this read version of asset to the writing version."""
        return AssetApply(
            space=self.space,
            external_id=self.external_id,
            area_id=self.area_id,
            category_id=self.category_id,
            children=[child.as_apply() if isinstance(child, DomainModel) else child for child in self.children or []],
            created_date=self.created_date,
            description=self.description,
            documents=self.documents,
            in_model_3_d=[in_model_3_d.as_apply() for in_model_3_d in self.in_model_3_d or []],
            is_active=self.is_active,
            is_critical_line=self.is_critical_line,
            measurements=self.measurements,
            metrics=self.metrics,
            parent=self.parent.as_apply() if isinstance(self.parent, DomainModel) else self.parent,
            pressure=self.pressure,
            source_db=self.source_db,
            specification=self.specification,
            tag=self.tag,
            trajectory=self.trajectory,
            updated_date=self.updated_date,
        )


class AssetApply(DomainModelApply):
    """This represents the writing version of asset.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the asset.
        area_id: @name area identification
        category_id: @name category identification
        children: The child field.
        created_date: @name created date
        description: The description field.
        documents: The document field.
        in_model_3_d: Cdf3dModel the Cdf3dEntity is part of
        is_active: @name active
        is_critical_line: @name critical line
        measurements: The measurement field.
        metrics: The metric field.
        parent: The parent field.
        pressure: The pressure field.
        source_db: @name source data
        specification: The specification field.
        tag: The tag field.
        trajectory: The trajectory field.
        updated_date: @name updated date
        existing_version: Fail the ingestion request if the asset version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    area_id: Optional[int] = Field(None, alias="areaId")
    category_id: Optional[int] = Field(None, alias="categoryId")
    children: Union[list[AssetApply], list[str], None] = Field(default=None, repr=False)
    created_date: Optional[datetime.datetime] = Field(None, alias="createdDate")
    description: Optional[str] = None
    documents: Optional[list[str]] = None
    in_model_3_d: Optional[list[CdfConnectionPropertiesApply]] = Field(default=None, repr=False, alias="inModel3d")
    is_active: Optional[bool] = Field(None, alias="isActive")
    is_critical_line: Optional[bool] = Field(None, alias="isCriticalLine")
    measurements: Optional[list[str]] = None
    metrics: Optional[list[TimeSeries]] = None
    parent: Union[AssetApply, str, dm.NodeId, None] = Field(None, repr=False)
    pressure: Union[TimeSeries, str, None] = None
    source_db: Optional[str] = Field(None, alias="sourceDb")
    specification: Union[str, None] = None
    tag: Optional[str] = None
    trajectory: Union[str, None] = None
    updated_date: Optional[datetime.datetime] = Field(None, alias="updatedDate")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        from ._cdf_3_d_connection_properties import CdfConnectionPropertiesApply

        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "tutorial_apm_simple", "Asset", "beb2bebdcbb4ad"
        )

        properties = {}
        if self.area_id is not None:
            properties["areaId"] = self.area_id
        if self.category_id is not None:
            properties["categoryId"] = self.category_id
        if self.created_date is not None:
            properties["createdDate"] = self.created_date.isoformat(timespec="milliseconds")
        if self.description is not None:
            properties["description"] = self.description
        if self.documents is not None:
            properties["documents"] = self.documents
        if self.is_active is not None:
            properties["isActive"] = self.is_active
        if self.is_critical_line is not None:
            properties["isCriticalLine"] = self.is_critical_line
        if self.measurements is not None:
            properties["measurements"] = self.measurements
        if self.metrics is not None:
            properties["metrics"] = self.metrics
        if self.parent is not None:
            properties["parent"] = {
                "space": self.space if isinstance(self.parent, str) else self.parent.space,
                "externalId": self.parent if isinstance(self.parent, str) else self.parent.external_id,
            }
        if self.pressure is not None:
            properties["pressure"] = self.pressure if isinstance(self.pressure, str) else self.pressure.external_id
        if self.source_db is not None:
            properties["sourceDb"] = self.source_db
        if self.specification is not None:
            properties["specification"] = self.specification
        if self.tag is not None:
            properties["tag"] = self.tag
        if self.trajectory is not None:
            properties["trajectory"] = self.trajectory
        if self.updated_date is not None:
            properties["updatedDate"] = self.updated_date.isoformat(timespec="milliseconds")

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

        edge_type = dm.DirectRelationReference("tutorial_apm_simple", "Asset.children")
        for child in self.children or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, child, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        for in_model_3_d in self.in_model_3_d or []:
            if isinstance(in_model_3_d, DomainRelationApply):
                other_resources = in_model_3_d._to_instances_apply(cache, self, view_by_write_class)
                resources.extend(other_resources)

        if isinstance(self.parent, DomainModelApply):
            other_resources = self.parent._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        if isinstance(self.pressure, CogniteTimeSeries):
            resources.time_series.append(self.pressure)

        return resources


class AssetList(DomainModelList[Asset]):
    """List of assets in the read version."""

    _INSTANCE = Asset

    def as_apply(self) -> AssetApplyList:
        """Convert these read versions of asset to the writing versions."""
        return AssetApplyList([node.as_apply() for node in self.data])


class AssetApplyList(DomainModelApplyList[AssetApply]):
    """List of assets in the writing version."""

    _INSTANCE = AssetApply


def _create_asset_filter(
    view_id: dm.ViewId,
    min_area_id: int | None = None,
    max_area_id: int | None = None,
    min_category_id: int | None = None,
    max_category_id: int | None = None,
    min_created_date: datetime.datetime | None = None,
    max_created_date: datetime.datetime | None = None,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    is_active: bool | None = None,
    is_critical_line: bool | None = None,
    parent: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    source_db: str | list[str] | None = None,
    source_db_prefix: str | None = None,
    tag: str | list[str] | None = None,
    tag_prefix: str | None = None,
    min_updated_date: datetime.datetime | None = None,
    max_updated_date: datetime.datetime | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_area_id or max_area_id:
        filters.append(dm.filters.Range(view_id.as_property_ref("areaId"), gte=min_area_id, lte=max_area_id))
    if min_category_id or max_category_id:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("categoryId"), gte=min_category_id, lte=max_category_id)
        )
    if min_created_date or max_created_date:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("createdDate"),
                gte=min_created_date.isoformat(timespec="milliseconds") if min_created_date else None,
                lte=max_created_date.isoformat(timespec="milliseconds") if max_created_date else None,
            )
        )
    if description and isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if is_active and isinstance(is_active, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isActive"), value=is_active))
    if is_critical_line and isinstance(is_critical_line, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isCriticalLine"), value=is_critical_line))
    if parent and isinstance(parent, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("parent"), value={"space": "tutorial_apm_simple", "externalId": parent}
            )
        )
    if parent and isinstance(parent, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("parent"), value={"space": parent[0], "externalId": parent[1]})
        )
    if parent and isinstance(parent, list) and isinstance(parent[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("parent"),
                values=[{"space": "tutorial_apm_simple", "externalId": item} for item in parent],
            )
        )
    if parent and isinstance(parent, list) and isinstance(parent[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("parent"), values=[{"space": item[0], "externalId": item[1]} for item in parent]
            )
        )
    if source_db and isinstance(source_db, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("sourceDb"), value=source_db))
    if source_db and isinstance(source_db, list):
        filters.append(dm.filters.In(view_id.as_property_ref("sourceDb"), values=source_db))
    if source_db_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("sourceDb"), value=source_db_prefix))
    if tag and isinstance(tag, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("tag"), value=tag))
    if tag and isinstance(tag, list):
        filters.append(dm.filters.In(view_id.as_property_ref("tag"), values=tag))
    if tag_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("tag"), value=tag_prefix))
    if min_updated_date or max_updated_date:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("updatedDate"),
                gte=min_updated_date.isoformat(timespec="milliseconds") if min_updated_date else None,
                lte=max_updated_date.isoformat(timespec="milliseconds") if max_updated_date else None,
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
