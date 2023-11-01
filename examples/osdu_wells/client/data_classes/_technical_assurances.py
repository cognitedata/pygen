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
    space: str = "IntegrationTestsImmutable"
    acceptable_usage: Optional[list[str]] = Field(None, alias="AcceptableUsage")
    comment: Optional[str] = Field(None, alias="Comment")
    effective_date: Optional[str] = Field(None, alias="EffectiveDate")
    reviewers: Optional[list[str]] = Field(None, alias="Reviewers")
    technical_assurance_type_id: Optional[str] = Field(None, alias="TechnicalAssuranceTypeID")
    unacceptable_usage: Optional[list[str]] = Field(None, alias="UnacceptableUsage")

    def as_apply(self) -> TechnicalAssurancesApply:
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

    def _to_instances_apply(self, cache: set[str], write_view: dm.ViewId | None) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

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
                instances = acceptable_usage._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for reviewer in self.reviewers or []:
            edge = self._create_reviewer_edge(reviewer)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(reviewer, DomainModelApply):
                instances = reviewer._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for unacceptable_usage in self.unacceptable_usage or []:
            edge = self._create_unacceptable_usage_edge(unacceptable_usage)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(unacceptable_usage, DomainModelApply):
                instances = unacceptable_usage._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_acceptable_usage_edge(self, acceptable_usage: Union[str, AcceptableUsageApply]) -> dm.EdgeApply:
        if isinstance(acceptable_usage, str):
            end_node_ext_id = acceptable_usage
        elif isinstance(acceptable_usage, DomainModelApply):
            end_node_ext_id = acceptable_usage.external_id
        else:
            raise TypeError(f"Expected str or AcceptableUsageApply, got {type(acceptable_usage)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "TechnicalAssurances.AcceptableUsage"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )

    def _create_reviewer_edge(self, reviewer: Union[str, ReviewersApply]) -> dm.EdgeApply:
        if isinstance(reviewer, str):
            end_node_ext_id = reviewer
        elif isinstance(reviewer, DomainModelApply):
            end_node_ext_id = reviewer.external_id
        else:
            raise TypeError(f"Expected str or ReviewersApply, got {type(reviewer)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "TechnicalAssurances.Reviewers"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )

    def _create_unacceptable_usage_edge(self, unacceptable_usage: Union[str, UnacceptableUsageApply]) -> dm.EdgeApply:
        if isinstance(unacceptable_usage, str):
            end_node_ext_id = unacceptable_usage
        elif isinstance(unacceptable_usage, DomainModelApply):
            end_node_ext_id = unacceptable_usage.external_id
        else:
            raise TypeError(f"Expected str or UnacceptableUsageApply, got {type(unacceptable_usage)}")

        return dm.EdgeApply(
            space="IntegrationTestsImmutable",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("IntegrationTestsImmutable", "TechnicalAssurances.UnacceptableUsage"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("IntegrationTestsImmutable", end_node_ext_id),
        )


class TechnicalAssurancesList(TypeList[TechnicalAssurances]):
    _NODE = TechnicalAssurances

    def as_apply(self) -> TechnicalAssurancesApplyList:
        return TechnicalAssurancesApplyList([node.as_apply() for node in self.data])


class TechnicalAssurancesApplyList(TypeApplyList[TechnicalAssurancesApply]):
    _NODE = TechnicalAssurancesApply
