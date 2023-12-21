from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

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

if TYPE_CHECKING:
    from ._acceptable_usage import AcceptableUsage, AcceptableUsageApply
    from ._reviewers import Reviewers, ReviewersApply
    from ._unacceptable_usage import UnacceptableUsage, UnacceptableUsageApply


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
    """This represents the reading version of technical assurance.

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

    space: str = DEFAULT_INSTANCE_SPACE
    acceptable_usage: Union[list[AcceptableUsage], list[str], None] = Field(
        default=None, repr=False, alias="AcceptableUsage"
    )
    comment: Optional[str] = Field(None, alias="Comment")
    effective_date: Optional[str] = Field(None, alias="EffectiveDate")
    reviewers: Union[list[Reviewers], list[str], None] = Field(default=None, repr=False, alias="Reviewers")
    technical_assurance_type_id: Optional[str] = Field(None, alias="TechnicalAssuranceTypeID")
    unacceptable_usage: Union[list[UnacceptableUsage], list[str], None] = Field(
        default=None, repr=False, alias="UnacceptableUsage"
    )

    def as_apply(self) -> TechnicalAssurancesApply:
        """Convert this read version of technical assurance to the writing version."""
        return TechnicalAssurancesApply(
            space=self.space,
            external_id=self.external_id,
            acceptable_usage=[
                acceptable_usage.as_apply() if isinstance(acceptable_usage, DomainModel) else acceptable_usage
                for acceptable_usage in self.acceptable_usage or []
            ],
            comment=self.comment,
            effective_date=self.effective_date,
            reviewers=[
                reviewer.as_apply() if isinstance(reviewer, DomainModel) else reviewer
                for reviewer in self.reviewers or []
            ],
            technical_assurance_type_id=self.technical_assurance_type_id,
            unacceptable_usage=[
                unacceptable_usage.as_apply() if isinstance(unacceptable_usage, DomainModel) else unacceptable_usage
                for unacceptable_usage in self.unacceptable_usage or []
            ],
        )


class TechnicalAssurancesApply(DomainModelApply):
    """This represents the writing version of technical assurance.

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
        existing_version: Fail the ingestion request if the technical assurance version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
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
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "TechnicalAssurances", "20cfc9c180f3df"
        )

        properties = {}

        if self.comment is not None:
            properties["Comment"] = self.comment

        if self.effective_date is not None:
            properties["EffectiveDate"] = self.effective_date

        if self.technical_assurance_type_id is not None:
            properties["TechnicalAssuranceTypeID"] = self.technical_assurance_type_id

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

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "TechnicalAssurances.AcceptableUsage")
        for acceptable_usage in self.acceptable_usage or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=acceptable_usage,
                edge_type=edge_type,
                view_by_write_class=view_by_write_class,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "TechnicalAssurances.Reviewers")
        for reviewer in self.reviewers or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, start_node=self, end_node=reviewer, edge_type=edge_type, view_by_write_class=view_by_write_class
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("IntegrationTestsImmutable", "TechnicalAssurances.UnacceptableUsage")
        for unacceptable_usage in self.unacceptable_usage or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=unacceptable_usage,
                edge_type=edge_type,
                view_by_write_class=view_by_write_class,
            )
            resources.extend(other_resources)

        return resources


class TechnicalAssurancesList(DomainModelList[TechnicalAssurances]):
    """List of technical assurances in the read version."""

    _INSTANCE = TechnicalAssurances

    def as_apply(self) -> TechnicalAssurancesApplyList:
        """Convert these read versions of technical assurance to the writing versions."""
        return TechnicalAssurancesApplyList([node.as_apply() for node in self.data])


class TechnicalAssurancesApplyList(DomainModelApplyList[TechnicalAssurancesApply]):
    """List of technical assurances in the writing version."""

    _INSTANCE = TechnicalAssurancesApply


def _create_technical_assurance_filter(
    view_id: dm.ViewId,
    comment: str | list[str] | None = None,
    comment_prefix: str | None = None,
    effective_date: str | list[str] | None = None,
    effective_date_prefix: str | None = None,
    technical_assurance_type_id: str | list[str] | None = None,
    technical_assurance_type_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if comment is not None and isinstance(comment, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Comment"), value=comment))
    if comment and isinstance(comment, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Comment"), values=comment))
    if comment_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Comment"), value=comment_prefix))
    if effective_date is not None and isinstance(effective_date, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("EffectiveDate"), value=effective_date))
    if effective_date and isinstance(effective_date, list):
        filters.append(dm.filters.In(view_id.as_property_ref("EffectiveDate"), values=effective_date))
    if effective_date_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("EffectiveDate"), value=effective_date_prefix))
    if technical_assurance_type_id is not None and isinstance(technical_assurance_type_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("TechnicalAssuranceTypeID"), value=technical_assurance_type_id)
        )
    if technical_assurance_type_id and isinstance(technical_assurance_type_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("TechnicalAssuranceTypeID"), values=technical_assurance_type_id)
        )
    if technical_assurance_type_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("TechnicalAssuranceTypeID"), value=technical_assurance_type_id_prefix
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
