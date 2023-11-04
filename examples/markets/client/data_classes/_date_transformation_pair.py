from __future__ import annotations

from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._date_transformation import DateTransformationApply

__all__ = [
    "DateTransformationPair",
    "DateTransformationPairApply",
    "DateTransformationPairList",
    "DateTransformationPairApplyList",
]


class DateTransformationPair(DomainModel):
    """This represent a read version of date transformation pair.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the date transformation pair.
        end: The end field.
        start: The start field.
        created_time: The created time of the date transformation pair node.
        last_updated_time: The last updated time of the date transformation pair node.
        deleted_time: If present, the deleted time of the date transformation pair node.
        version: The version of the date transformation pair node.
    """

    space: str = "market"
    end: Optional[list[str]] = None
    start: Optional[list[str]] = None

    def as_apply(self) -> DateTransformationPairApply:
        """Convert this read version of date transformation pair to a write version."""
        return DateTransformationPairApply(
            space=self.space,
            external_id=self.external_id,
            end=self.end,
            start=self.start,
        )


class DateTransformationPairApply(DomainModelApply):
    """This represent a write version of date transformation pair.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the date transformation pair.
        end: The end field.
        start: The start field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "market"
    end: Union[list[DateTransformationApply], list[str], None] = Field(default=None, repr=False)
    start: Union[list[DateTransformationApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        nodes = []

        edges = []
        cache.add(self.external_id)

        for end in self.end or []:
            edge = self._create_end_edge(end)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(end, DomainModelApply):
                instances = end._to_instances_apply(cache, view_by_write_class)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for start in self.start or []:
            edge = self._create_start_edge(start)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(start, DomainModelApply):
                instances = start._to_instances_apply(cache, view_by_write_class)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_end_edge(self, end: Union[str, DateTransformationApply]) -> dm.EdgeApply:
        if isinstance(end, str):
            end_node_ext_id = end
        elif isinstance(end, DomainModelApply):
            end_node_ext_id = end.external_id
        else:
            raise TypeError(f"Expected str or DateTransformationApply, got {type(end)}")

        return dm.EdgeApply(
            space="market",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("market", "DateTransformationPair.end"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("market", end_node_ext_id),
        )

    def _create_start_edge(self, start: Union[str, DateTransformationApply]) -> dm.EdgeApply:
        if isinstance(start, str):
            end_node_ext_id = start
        elif isinstance(start, DomainModelApply):
            end_node_ext_id = start.external_id
        else:
            raise TypeError(f"Expected str or DateTransformationApply, got {type(start)}")

        return dm.EdgeApply(
            space="market",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("market", "DateTransformationPair.start"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("market", end_node_ext_id),
        )


class DateTransformationPairList(TypeList[DateTransformationPair]):
    """List of date transformation pairs in read version."""

    _NODE = DateTransformationPair

    def as_apply(self) -> DateTransformationPairApplyList:
        """Convert this read version of date transformation pair to a write version."""
        return DateTransformationPairApplyList([node.as_apply() for node in self.data])


class DateTransformationPairApplyList(TypeApplyList[DateTransformationPairApply]):
    """List of date transformation pairs in write version."""

    _NODE = DateTransformationPairApply
