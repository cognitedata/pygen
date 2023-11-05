from __future__ import annotations

import datetime
from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._asset import AssetApply
    from ._cdf_3_d_model import CdfModelApply

__all__ = ["Asset", "AssetApply", "AssetList", "AssetApplyList", "AssetFields", "AssetTextFields"]


AssetTextFields = Literal[
    "description", "documents", "measurements", "metrics", "pressure", "source_db", "specification", "tag", "trajectory"
]
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
    """This represent a read version of asset.

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

    space: str = "tutorial_apm_simple"
    area_id: Optional[int] = Field(None, alias="areaId")
    category_id: Optional[int] = Field(None, alias="categoryId")
    children: Optional[list[str]] = None
    created_date: Optional[datetime.datetime] = Field(None, alias="createdDate")
    description: Optional[str] = None
    documents: Optional[list[str]] = None
    in_model_3_d: Optional[list[str]] = Field(None, alias="inModel3d")
    is_active: Optional[bool] = Field(None, alias="isActive")
    is_critical_line: Optional[bool] = Field(None, alias="isCriticalLine")
    measurements: Optional[list[str]] = None
    metrics: Optional[list[str]] = None
    parent: Optional[str] = None
    pressure: Optional[str] = None
    source_db: Optional[str] = Field(None, alias="sourceDb")
    specification: Optional[str] = None
    tag: Optional[str] = None
    trajectory: Optional[str] = None
    updated_date: Optional[datetime.datetime] = Field(None, alias="updatedDate")

    def as_apply(self) -> AssetApply:
        """Convert this read version of asset to a write version."""
        return AssetApply(
            space=self.space,
            external_id=self.external_id,
            area_id=self.area_id,
            category_id=self.category_id,
            children=self.children,
            created_date=self.created_date,
            description=self.description,
            documents=self.documents,
            in_model_3_d=self.in_model_3_d,
            is_active=self.is_active,
            is_critical_line=self.is_critical_line,
            measurements=self.measurements,
            metrics=self.metrics,
            parent=self.parent,
            pressure=self.pressure,
            source_db=self.source_db,
            specification=self.specification,
            tag=self.tag,
            trajectory=self.trajectory,
            updated_date=self.updated_date,
        )


class AssetApply(DomainModelApply):
    """This represent a write version of asset.

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
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "tutorial_apm_simple"
    area_id: Optional[int] = Field(None, alias="areaId")
    category_id: Optional[int] = Field(None, alias="categoryId")
    children: Union[list[AssetApply], list[str], None] = Field(default=None, repr=False)
    created_date: Optional[datetime.datetime] = Field(None, alias="createdDate")
    description: Optional[str] = None
    documents: Optional[list[str]] = None
    in_model_3_d: Union[list[CdfModelApply], list[str], None] = Field(default=None, repr=False, alias="inModel3d")
    is_active: Optional[bool] = Field(None, alias="isActive")
    is_critical_line: Optional[bool] = Field(None, alias="isCriticalLine")
    measurements: Optional[list[str]] = None
    metrics: Optional[list[str]] = None
    parent: Union[AssetApply, str, None] = Field(None, repr=False)
    pressure: Optional[str] = None
    source_db: Optional[str] = Field(None, alias="sourceDb")
    specification: Optional[str] = None
    tag: Optional[str] = None
    trajectory: Optional[str] = None
    updated_date: Optional[datetime.datetime] = Field(None, alias="updatedDate")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.area_id is not None:
            properties["areaId"] = self.area_id
        if self.category_id is not None:
            properties["categoryId"] = self.category_id
        if self.created_date is not None:
            properties["createdDate"] = self.created_date.isoformat()
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
            properties["pressure"] = self.pressure
        if self.source_db is not None:
            properties["sourceDb"] = self.source_db
        if self.specification is not None:
            properties["specification"] = self.specification
        if self.tag is not None:
            properties["tag"] = self.tag
        if self.trajectory is not None:
            properties["trajectory"] = self.trajectory
        if self.updated_date is not None:
            properties["updatedDate"] = self.updated_date.isoformat()
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("tutorial_apm_simple", "Asset", "beb2bebdcbb4ad"),
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

        for child in self.children or []:
            edge = self._create_child_edge(child)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(child, DomainModelApply):
                instances = child._to_instances_apply(cache, view_by_write_class)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for in_model_3_d in self.in_model_3_d or []:
            edge = self._create_in_model_3_d_edge(in_model_3_d)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(in_model_3_d, DomainModelApply):
                instances = in_model_3_d._to_instances_apply(cache, view_by_write_class)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.parent, DomainModelApply):
            instances = self.parent._to_instances_apply(cache, view_by_write_class)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_child_edge(self, child: Union[str, AssetApply]) -> dm.EdgeApply:
        if isinstance(child, str):
            end_space, end_node_ext_id = self.space, child
        elif isinstance(child, DomainModelApply):
            end_space, end_node_ext_id = child.space, child.external_id
        else:
            raise TypeError(f"Expected str or AssetApply, got {type(child)}")

        return dm.EdgeApply(
            space=self.space,
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("tutorial_apm_simple", "Asset.children"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference(end_space, end_node_ext_id),
        )

    def _create_in_model_3_d_edge(self, in_model_3_d: Union[str, CdfModelApply]) -> dm.EdgeApply:
        if isinstance(in_model_3_d, str):
            end_space, end_node_ext_id = self.space, in_model_3_d
        elif isinstance(in_model_3_d, DomainModelApply):
            end_space, end_node_ext_id = in_model_3_d.space, in_model_3_d.external_id
        else:
            raise TypeError(f"Expected str or CdfModelApply, got {type(in_model_3_d)}")

        return dm.EdgeApply(
            space=self.space,
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("cdf_3d_schema", "cdf3dEntityConnection"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference(end_space, end_node_ext_id),
        )


class AssetList(TypeList[Asset]):
    """List of assets in read version."""

    _NODE = Asset

    def as_apply(self) -> AssetApplyList:
        """Convert this read version of asset to a write version."""
        return AssetApplyList([node.as_apply() for node in self.data])


class AssetApplyList(TypeApplyList[AssetApply]):
    """List of assets in write version."""

    _NODE = AssetApply
