from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = [
    "CdfConnectionProperties",
    "CdfConnectionPropertiesApply",
    "CdfConnectionPropertiesList",
    "CdfConnectionPropertiesApplyList",
    "CdfConnectionPropertiesFields",
]
CdfConnectionPropertiesFields = Literal["revision_id", "revision_node_id"]

_CDFCONNECTIONPROPERTIES_PROPERTIES_BY_FIELD = {
    "revision_id": "revisionId",
    "revision_node_id": "revisionNodeId",
}


class CdfConnectionProperties(DomainModel):
    """This represent a read version of cdf 3 d connection property.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf 3 d connection property.
        revision_id: The revision id field.
        revision_node_id: The revision node id field.
        created_time: The created time of the cdf 3 d connection property node.
        last_updated_time: The last updated time of the cdf 3 d connection property node.
        deleted_time: If present, the deleted time of the cdf 3 d connection property node.
        version: The version of the cdf 3 d connection property node.
    """

    space: str = "cdf_3d_schema"
    revision_id: Optional[int] = Field(None, alias="revisionId")
    revision_node_id: Optional[int] = Field(None, alias="revisionNodeId")

    def as_apply(self) -> CdfConnectionPropertiesApply:
        """Convert this read version of cdf 3 d connection property to a write version."""
        return CdfConnectionPropertiesApply(
            space=self.space,
            external_id=self.external_id,
            revision_id=self.revision_id,
            revision_node_id=self.revision_node_id,
        )


class CdfConnectionPropertiesApply(DomainModelApply):
    """This represent a write version of cdf 3 d connection property.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf 3 d connection property.
        revision_id: The revision id field.
        revision_node_id: The revision node id field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "cdf_3d_schema"
    revision_id: int = Field(alias="revisionId")
    revision_node_id: int = Field(alias="revisionNodeId")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.revision_id is not None:
            properties["revisionId"] = self.revision_id
        if self.revision_node_id is not None:
            properties["revisionNodeId"] = self.revision_node_id
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("cdf_3d_schema", "Cdf3dConnectionProperties", "1"),
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

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class CdfConnectionPropertiesList(TypeList[CdfConnectionProperties]):
    """List of cdf 3 d connection properties in read version."""

    _NODE = CdfConnectionProperties

    def as_apply(self) -> CdfConnectionPropertiesApplyList:
        """Convert this read version of cdf 3 d connection property to a write version."""
        return CdfConnectionPropertiesApplyList([node.as_apply() for node in self.data])


class CdfConnectionPropertiesApplyList(TypeApplyList[CdfConnectionPropertiesApply]):
    """List of cdf 3 d connection properties in write version."""

    _NODE = CdfConnectionPropertiesApply
