from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = ["Legal", "LegalApply", "LegalList", "LegalApplyList", "LegalFields", "LegalTextFields"]


LegalTextFields = Literal["legaltags", "other_relevant_data_countries", "status"]
LegalFields = Literal["legaltags", "other_relevant_data_countries", "status"]

_LEGAL_PROPERTIES_BY_FIELD = {
    "legaltags": "legaltags",
    "other_relevant_data_countries": "otherRelevantDataCountries",
    "status": "status",
}


class Legal(DomainModel):
    """This represent a read version of legal.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the legal.
        legaltags: The legaltag field.
        other_relevant_data_countries: The other relevant data country field.
        status: The status field.
        created_time: The created time of the legal node.
        last_updated_time: The last updated time of the legal node.
        deleted_time: If present, the deleted time of the legal node.
        version: The version of the legal node.
    """

    space: str = "IntegrationTestsImmutable"
    legaltags: Optional[list[str]] = None
    other_relevant_data_countries: Optional[list[str]] = Field(None, alias="otherRelevantDataCountries")
    status: Optional[str] = None

    def as_apply(self) -> LegalApply:
        """Convert this read version of legal to a write version."""
        return LegalApply(
            space=self.space,
            external_id=self.external_id,
            legaltags=self.legaltags,
            other_relevant_data_countries=self.other_relevant_data_countries,
            status=self.status,
        )


class LegalApply(DomainModelApply):
    """This represent a write version of legal.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the legal.
        legaltags: The legaltag field.
        other_relevant_data_countries: The other relevant data country field.
        status: The status field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    legaltags: Optional[list[str]] = None
    other_relevant_data_countries: Optional[list[str]] = Field(None, alias="otherRelevantDataCountries")
    status: Optional[str] = None

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.legaltags is not None:
            properties["legaltags"] = self.legaltags
        if self.other_relevant_data_countries is not None:
            properties["otherRelevantDataCountries"] = self.other_relevant_data_countries
        if self.status is not None:
            properties["status"] = self.status
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "Legal", "508188c6379675"),
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


class LegalList(TypeList[Legal]):
    """List of legals in read version."""

    _NODE = Legal

    def as_apply(self) -> LegalApplyList:
        """Convert this read version of legal to a write version."""
        return LegalApplyList([node.as_apply() for node in self.data])


class LegalApplyList(TypeApplyList[LegalApply]):
    """List of legals in write version."""

    _NODE = LegalApply
