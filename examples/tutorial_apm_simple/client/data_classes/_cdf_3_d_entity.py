from __future__ import annotations

from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._cdf_3_d_model import CdfModelApply

__all__ = ["CdfEntity", "CdfEntityApply", "CdfEntityList", "CdfEntityApplyList"]


class CdfEntity(DomainModel):
    """This represent a read version of cdf 3 d entity.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf 3 d entity.
        in_model_3_d: Cdf3dModel the Cdf3dEntity is part of
        created_time: The created time of the cdf 3 d entity node.
        last_updated_time: The last updated time of the cdf 3 d entity node.
        deleted_time: If present, the deleted time of the cdf 3 d entity node.
        version: The version of the cdf 3 d entity node.
    """

    space: str = "cdf_3d_schema"
    in_model_3_d: Optional[list[str]] = Field(None, alias="inModel3d")

    def as_apply(self) -> CdfEntityApply:
        """Convert this read version of cdf 3 d entity to a write version."""
        return CdfEntityApply(
            space=self.space,
            external_id=self.external_id,
            in_model_3_d=self.in_model_3_d,
        )


class CdfEntityApply(DomainModelApply):
    """This represent a write version of cdf 3 d entity.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf 3 d entity.
        in_model_3_d: Cdf3dModel the Cdf3dEntity is part of
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "cdf_3d_schema"
    in_model_3_d: Union[list[CdfModelApply], list[str], None] = Field(default=None, repr=False, alias="inModel3d")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        nodes = []

        edges = []
        cache.add(self.external_id)

        for in_model_3_d in self.in_model_3_d or []:
            edge = self._create_in_model_3_d_edge(in_model_3_d)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(in_model_3_d, DomainModelApply):
                instances = in_model_3_d._to_instances_apply(cache, view_by_write_class)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

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


class CdfEntityList(TypeList[CdfEntity]):
    """List of cdf 3 d entities in read version."""

    _NODE = CdfEntity

    def as_apply(self) -> CdfEntityApplyList:
        """Convert this read version of cdf 3 d entity to a write version."""
        return CdfEntityApplyList([node.as_apply() for node in self.data])


class CdfEntityApplyList(TypeApplyList[CdfEntityApply]):
    """List of cdf 3 d entities in write version."""

    _NODE = CdfEntityApply
