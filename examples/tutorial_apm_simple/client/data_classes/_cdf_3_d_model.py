from __future__ import annotations

from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._cdf_3_d_entity import CdfEntityApply

__all__ = ["CdfModel", "CdfModelApply", "CdfModelList", "CdfModelApplyList", "CdfModelFields", "CdfModelTextFields"]


CdfModelTextFields = Literal["name"]
CdfModelFields = Literal["name"]

_CDFMODEL_PROPERTIES_BY_FIELD = {
    "name": "name",
}


class CdfModel(DomainModel):
    """This represent a read version of cdf 3 d model.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf 3 d model.
        entities: Collection of Cdf3dEntity that are part of this Cdf3dModel
        name: The name field.
        created_time: The created time of the cdf 3 d model node.
        last_updated_time: The last updated time of the cdf 3 d model node.
        deleted_time: If present, the deleted time of the cdf 3 d model node.
        version: The version of the cdf 3 d model node.
    """

    space: str = "cdf_3d_schema"
    entities: Optional[list[str]] = None
    name: Optional[str] = None

    def as_apply(self) -> CdfModelApply:
        """Convert this read version of cdf 3 d model to a write version."""
        return CdfModelApply(
            space=self.space,
            external_id=self.external_id,
            entities=self.entities,
            name=self.name,
        )


class CdfModelApply(DomainModelApply):
    """This represent a write version of cdf 3 d model.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf 3 d model.
        entities: Collection of Cdf3dEntity that are part of this Cdf3dModel
        name: The name field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "cdf_3d_schema"
    entities: Union[list[CdfEntityApply], list[str], None] = Field(default=None, repr=False)
    name: str

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("cdf_3d_schema", "Cdf3dModel", "1"),
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

        for entity in self.entities or []:
            edge = self._create_entity_edge(entity)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(entity, DomainModelApply):
                instances = entity._to_instances_apply(cache, view_by_write_class)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_entity_edge(self, entity: Union[str, CdfEntityApply]) -> dm.EdgeApply:
        if isinstance(entity, str):
            end_space, end_node_ext_id = self.space, entity
        elif isinstance(entity, DomainModelApply):
            end_space, end_node_ext_id = entity.space, entity.external_id
        else:
            raise TypeError(f"Expected str or CdfEntityApply, got {type(entity)}")

        return dm.EdgeApply(
            space=self.space,
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("cdf_3d_schema", "cdf3dEntityConnection"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference(end_space, end_node_ext_id),
        )


class CdfModelList(TypeList[CdfModel]):
    """List of cdf 3 d models in read version."""

    _NODE = CdfModel

    def as_apply(self) -> CdfModelApplyList:
        """Convert this read version of cdf 3 d model to a write version."""
        return CdfModelApplyList([node.as_apply() for node in self.data])


class CdfModelApplyList(TypeApplyList[CdfModelApply]):
    """List of cdf 3 d models in write version."""

    _NODE = CdfModelApply
