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
    "HistoricalInterests",
    "HistoricalInterestsApply",
    "HistoricalInterestsList",
    "HistoricalInterestsApplyList",
    "HistoricalInterestsFields",
    "HistoricalInterestsTextFields",
]


HistoricalInterestsTextFields = Literal["effective_date_time", "interest_type_id", "termination_date_time"]
HistoricalInterestsFields = Literal["effective_date_time", "interest_type_id", "termination_date_time"]

_HISTORICALINTERESTS_PROPERTIES_BY_FIELD = {
    "effective_date_time": "EffectiveDateTime",
    "interest_type_id": "InterestTypeID",
    "termination_date_time": "TerminationDateTime",
}


class HistoricalInterests(DomainModel):
    """This represents the reading version of historical interest.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the historical interest.
        effective_date_time: The effective date time field.
        interest_type_id: The interest type id field.
        termination_date_time: The termination date time field.
        created_time: The created time of the historical interest node.
        last_updated_time: The last updated time of the historical interest node.
        deleted_time: If present, the deleted time of the historical interest node.
        version: The version of the historical interest node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    interest_type_id: Optional[str] = Field(None, alias="InterestTypeID")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")

    def as_apply(self) -> HistoricalInterestsApply:
        """Convert this read version of historical interest to the writing version."""
        return HistoricalInterestsApply(
            space=self.space,
            external_id=self.external_id,
            effective_date_time=self.effective_date_time,
            interest_type_id=self.interest_type_id,
            termination_date_time=self.termination_date_time,
        )


class HistoricalInterestsApply(DomainModelApply):
    """This represents the writing version of historical interest.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the historical interest.
        effective_date_time: The effective date time field.
        interest_type_id: The interest type id field.
        termination_date_time: The termination date time field.
        existing_version: Fail the ingestion request if the historical interest version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    effective_date_time: Optional[str] = Field(None, alias="EffectiveDateTime")
    interest_type_id: Optional[str] = Field(None, alias="InterestTypeID")
    termination_date_time: Optional[str] = Field(None, alias="TerminationDateTime")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "HistoricalInterests", "7399eff7364ba6"
        )

        properties = {}
        if self.effective_date_time is not None:
            properties["EffectiveDateTime"] = self.effective_date_time
        if self.interest_type_id is not None:
            properties["InterestTypeID"] = self.interest_type_id
        if self.termination_date_time is not None:
            properties["TerminationDateTime"] = self.termination_date_time

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=dm.DirectRelationReference("IntegrationTestsImmutable", "HistoricalInterests"),
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


class HistoricalInterestsList(DomainModelList[HistoricalInterests]):
    """List of historical interests in the read version."""

    _INSTANCE = HistoricalInterests

    def as_apply(self) -> HistoricalInterestsApplyList:
        """Convert these read versions of historical interest to the writing versions."""
        return HistoricalInterestsApplyList([node.as_apply() for node in self.data])


class HistoricalInterestsApplyList(DomainModelApplyList[HistoricalInterestsApply]):
    """List of historical interests in the writing version."""

    _INSTANCE = HistoricalInterestsApply


def _create_historical_interest_filter(
    view_id: dm.ViewId,
    effective_date_time: str | list[str] | None = None,
    effective_date_time_prefix: str | None = None,
    interest_type_id: str | list[str] | None = None,
    interest_type_id_prefix: str | None = None,
    termination_date_time: str | list[str] | None = None,
    termination_date_time_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if effective_date_time is not None and isinstance(effective_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("EffectiveDateTime"), value=effective_date_time))
    if effective_date_time and isinstance(effective_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("EffectiveDateTime"), values=effective_date_time))
    if effective_date_time_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("EffectiveDateTime"), value=effective_date_time_prefix)
        )
    if interest_type_id is not None and isinstance(interest_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("InterestTypeID"), value=interest_type_id))
    if interest_type_id and isinstance(interest_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("InterestTypeID"), values=interest_type_id))
    if interest_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("InterestTypeID"), value=interest_type_id_prefix))
    if termination_date_time is not None and isinstance(termination_date_time, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time))
    if termination_date_time and isinstance(termination_date_time, list):
        filters.append(dm.filters.In(view_id.as_property_ref("TerminationDateTime"), values=termination_date_time))
    if termination_date_time_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("TerminationDateTime"), value=termination_date_time_prefix)
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
