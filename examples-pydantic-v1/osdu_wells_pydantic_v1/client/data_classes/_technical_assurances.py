from __future__ import annotations

from typing import Literal, TYPE_CHECKING, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

if TYPE_CHECKING:
    from ._acceptable_usage import AcceptableUsageApply
    from ._reviewers import ReviewersApply
    from ._unacceptable_usage import UnacceptableUsageApply

__all__ = [
    "TechnicalAssurances",
    "TechnicalAssurancesApply",
    "TechnicalAssurancesList",
    "TechnicalAssurancesApplyList",
    "TechnicalAssurancesFields",
    "TechnicalAssurancesTextFields",
]


TechnicalAssurancesTextFields = Literal["comment", "effective_date", "technical_assurance_type_id"]
TechnicalAssurancesFields = Literal["comment", "effective_date", "technical_assurance_type_id"]

_TECHNICALASSURANCES_PROPERTIES_BY_FIELD = {
    "comment": "Comment",
    "effective_date": "EffectiveDate",
    "technical_assurance_type_id": "TechnicalAssuranceTypeID",
}


class TechnicalAssurances(DomainModel):
    """This represent a read version of technical assurance.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the technical assurance.
        acceptable_usage: The acceptable usage field.
        comment: The comment field.
        effective_date: The effective date field.
        reviewers: The reviewer field.
        technical_assurance_type_id: The technical assurance type id field.
        unacceptable_usage: The unacceptable usage field.
        created_time: The created time of the technical assurance node.
        last_updated_time: The last updated time of the technical assurance node.
        deleted_time: If present, the deleted time of the technical assurance node.
        version: The version of the technical assurance node.
    """

    space: str = "IntegrationTestsImmutable"
    acceptable_usage: Optional[list[str]] = Field(None, alias="AcceptableUsage")
    comment: Optional[str] = Field(None, alias="Comment")
    effective_date: Optional[str] = Field(None, alias="EffectiveDate")
    reviewers: Optional[list[str]] = Field(None, alias="Reviewers")
    technical_assurance_type_id: Optional[str] = Field(None, alias="TechnicalAssuranceTypeID")
    unacceptable_usage: Optional[list[str]] = Field(None, alias="UnacceptableUsage")

    def as_apply(self) -> TechnicalAssurancesApply:
        """Convert this read version of technical assurance to a write version."""
        return TechnicalAssurancesApply(
            space=self.space,
            external_id=self.external_id,
            acceptable_usage=self.acceptable_usage,
            comment=self.comment,
            effective_date=self.effective_date,
            reviewers=self.reviewers,
            technical_assurance_type_id=self.technical_assurance_type_id,
            unacceptable_usage=self.unacceptable_usage,
        )


class TechnicalAssurancesApply(DomainModelApply):
    """This represent a write version of technical assurance.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the technical assurance.
        acceptable_usage: The acceptable usage field.
        comment: The comment field.
        effective_date: The effective date field.
        reviewers: The reviewer field.
        technical_assurance_type_id: The technical assurance type id field.
        unacceptable_usage: The unacceptable usage field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    acceptable_usage: Union[list[AcceptableUsageApply], list[str], None] = Field(
        default=None, repr=False, alias="AcceptableUsage"
    )
    comment: Optional[str] = Field(None, alias="Comment")
    effective_date: Optional[str] = Field(None, alias="EffectiveDate")
    reviewers: Union[list[ReviewersApply], list[str], None] = Field(default=None, repr=False, alias="Reviewers")
    technical_assurance_type_id: Optional[str] = Field(None, alias="TechnicalAssuranceTypeID")
    unacceptable_usage: Union[list[UnacceptableUsageApply], list[str], None] = Field(
        default=None, repr=False, alias="UnacceptableUsage"
    )

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.comment is not None:
            properties["Comment"] = self.comment
        if self.effective_date is not None:
            properties["EffectiveDate"] = self.effective_date
        if self.technical_assurance_type_id is not None:
            properties["TechnicalAssuranceTypeID"] = self.technical_assurance_type_id
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "TechnicalAssurances", "20cfc9c180f3df"),
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

        for acceptable_usage in self.acceptable_usage or []:
            edge = self._create_acceptable_usage_edge(acceptable_usage)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(acceptable_usage, DomainModelApply):
                instances = acceptable_usage._to_instances_apply(cache, view_by_write_class)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for reviewer in self.reviewers or []:
            edge = self._create_reviewer_edge(reviewer)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(reviewer, DomainModelApply):
                instances = reviewer._to_instances_apply(cache, view_by_write_class)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for unacceptable_usage in self.unacceptable_usage or []:
            edge = self._create_unacceptable_usage_edge(unacceptable_usage)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(unacceptable_usage, DomainModelApply):
                instances = unacceptable_usage._to_instances_apply(cache, view_by_write_class)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_acceptable_usage_edge(self, acceptable_usage: Union[str, AcceptableUsageApply]) -> dm.EdgeApply:
        if isinstance(acceptable_usage, str):
            end_space, end_node_ext_id = self.space, acceptable_usage
        elif isinstance(acceptable_usage, DomainModelApply):
            end_space, end_node_ext_id = acceptable_usage.space, acceptable_usage.external_id
        else:
            raise TypeError(f"Expected str or AcceptableUsageApply, got {type(acceptable_usage)}")

        return dm.EdgeApply(
            space=self.space,
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "TechnicalAssurances.AcceptableUsage"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference(end_space, end_node_ext_id),
        )

    def _create_reviewer_edge(self, reviewer: Union[str, ReviewersApply]) -> dm.EdgeApply:
        if isinstance(reviewer, str):
            end_space, end_node_ext_id = self.space, reviewer
        elif isinstance(reviewer, DomainModelApply):
            end_space, end_node_ext_id = reviewer.space, reviewer.external_id
        else:
            raise TypeError(f"Expected str or ReviewersApply, got {type(reviewer)}")

        return dm.EdgeApply(
            space=self.space,
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "TechnicalAssurances.Reviewers"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference(end_space, end_node_ext_id),
        )

    def _create_unacceptable_usage_edge(self, unacceptable_usage: Union[str, UnacceptableUsageApply]) -> dm.EdgeApply:
        if isinstance(unacceptable_usage, str):
            end_space, end_node_ext_id = self.space, unacceptable_usage
        elif isinstance(unacceptable_usage, DomainModelApply):
            end_space, end_node_ext_id = unacceptable_usage.space, unacceptable_usage.external_id
        else:
            raise TypeError(f"Expected str or UnacceptableUsageApply, got {type(unacceptable_usage)}")

        return dm.EdgeApply(
            space=self.space,
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "TechnicalAssurances.UnacceptableUsage"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference(end_space, end_node_ext_id),
        )


class TechnicalAssurancesList(TypeList[TechnicalAssurances]):
    """List of technical assurances in read version."""

    _NODE = TechnicalAssurances

    def as_apply(self) -> TechnicalAssurancesApplyList:
        """Convert this read version of technical assurance to a write version."""
        return TechnicalAssurancesApplyList([node.as_apply() for node in self.data])


class TechnicalAssurancesApplyList(TypeApplyList[TechnicalAssurancesApply]):
    """List of technical assurances in write version."""

    _NODE = TechnicalAssurancesApply
