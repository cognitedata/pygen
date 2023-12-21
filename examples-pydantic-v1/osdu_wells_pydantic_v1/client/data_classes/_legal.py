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


__all__ = ["Legal", "LegalApply", "LegalList", "LegalApplyList", "LegalFields", "LegalTextFields"]


LegalTextFields = Literal["legaltags", "other_relevant_data_countries", "status"]
LegalFields = Literal["legaltags", "other_relevant_data_countries", "status"]

_LEGAL_PROPERTIES_BY_FIELD = {
    "legaltags": "legaltags",
    "other_relevant_data_countries": "otherRelevantDataCountries",
    "status": "status",
}


class Legal(DomainModel):
    """This represents the reading version of legal.

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

    space: str = DEFAULT_INSTANCE_SPACE
    legaltags: Optional[list[str]] = None
    other_relevant_data_countries: Optional[list[str]] = Field(None, alias="otherRelevantDataCountries")
    status: Optional[str] = None

    def as_apply(self) -> LegalApply:
        """Convert this read version of legal to the writing version."""
        return LegalApply(
            space=self.space,
            external_id=self.external_id,
            legaltags=self.legaltags,
            other_relevant_data_countries=self.other_relevant_data_countries,
            status=self.status,
        )


class LegalApply(DomainModelApply):
    """This represents the writing version of legal.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the legal.
        legaltags: The legaltag field.
        other_relevant_data_countries: The other relevant data country field.
        status: The status field.
        existing_version: Fail the ingestion request if the legal version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    legaltags: Optional[list[str]] = None
    other_relevant_data_countries: Optional[list[str]] = Field(None, alias="otherRelevantDataCountries")
    status: Optional[str] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "IntegrationTestsImmutable", "Legal", "508188c6379675"
        )

        properties = {}

        if self.legaltags is not None:
            properties["legaltags"] = self.legaltags

        if self.other_relevant_data_countries is not None:
            properties["otherRelevantDataCountries"] = self.other_relevant_data_countries

        if self.status is not None:
            properties["status"] = self.status

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

        return resources


class LegalList(DomainModelList[Legal]):
    """List of legals in the read version."""

    _INSTANCE = Legal

    def as_apply(self) -> LegalApplyList:
        """Convert these read versions of legal to the writing versions."""
        return LegalApplyList([node.as_apply() for node in self.data])


class LegalApplyList(DomainModelApplyList[LegalApply]):
    """List of legals in the writing version."""

    _INSTANCE = LegalApply


def _create_legal_filter(
    view_id: dm.ViewId,
    status: str | list[str] | None = None,
    status_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if status is not None and isinstance(status, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("status"), value=status))
    if status and isinstance(status, list):
        filters.append(dm.filters.In(view_id.as_property_ref("status"), values=status))
    if status_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("status"), value=status_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
