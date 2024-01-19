from __future__ import annotations

from typing import Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelCore,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    TimeSeries,
)


__all__ = [
    "CDFExternalReferencesListed",
    "CDFExternalReferencesListedApply",
    "CDFExternalReferencesListedList",
    "CDFExternalReferencesListedApplyList",
    "CDFExternalReferencesListedFields",
    "CDFExternalReferencesListedTextFields",
]


CDFExternalReferencesListedTextFields = Literal["files", "sequences", "timeseries"]
CDFExternalReferencesListedFields = Literal["files", "sequences", "timeseries"]

_CDFEXTERNALREFERENCESLISTED_PROPERTIES_BY_FIELD = {
    "files": "files",
    "sequences": "sequences",
    "timeseries": "timeseries",
}


class CDFExternalReferencesListed(DomainModel):
    """This represents the reading version of cdf external references listed.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf external references listed.
        files: The file field.
        sequences: The sequence field.
        timeseries: The timesery field.
        created_time: The created time of the cdf external references listed node.
        last_updated_time: The last updated time of the cdf external references listed node.
        deleted_time: If present, the deleted time of the cdf external references listed node.
        version: The version of the cdf external references listed node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    files: Optional[list[str]] = None
    sequences: Optional[list[str]] = None
    timeseries: Union[list[TimeSeries], list[str], None] = None

    def as_apply(self) -> CDFExternalReferencesListedApply:
        """Convert this read version of cdf external references listed to the writing version."""
        return CDFExternalReferencesListedApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.version,
            files=self.files,
            sequences=self.sequences,
            timeseries=self.timeseries,
        )


class CDFExternalReferencesListedApply(DomainModelApply):
    """This represents the writing version of cdf external references listed.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf external references listed.
        files: The file field.
        sequences: The sequence field.
        timeseries: The timesery field.
        existing_version: Fail the ingestion request if the cdf external references listed version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    files: Optional[list[str]] = None
    sequences: Optional[list[str]] = None
    timeseries: Union[list[TimeSeries], list[str], None] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            CDFExternalReferencesListed, dm.ViewId("pygen-models", "CDFExternalReferencesListed", "1")
        )

        properties: dict[str, Any] = {}

        if self.files is not None or write_none:
            properties["files"] = self.files

        if self.sequences is not None or write_none:
            properties["sequences"] = self.sequences

        if self.timeseries is not None or write_none:
            properties["timeseries"] = [
                value if isinstance(value, str) else value.external_id for value in self.timeseries or []
            ] or None

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.timeseries, CogniteTimeSeries):
            resources.time_series.append(self.timeseries)

        return resources


class CDFExternalReferencesListedList(DomainModelList[CDFExternalReferencesListed]):
    """List of cdf external references listeds in the read version."""

    _INSTANCE = CDFExternalReferencesListed

    def as_apply(self) -> CDFExternalReferencesListedApplyList:
        """Convert these read versions of cdf external references listed to the writing versions."""
        return CDFExternalReferencesListedApplyList([node.as_apply() for node in self.data])


class CDFExternalReferencesListedApplyList(DomainModelApplyList[CDFExternalReferencesListedApply]):
    """List of cdf external references listeds in the writing version."""

    _INSTANCE = CDFExternalReferencesListedApply


def _create_cdf_external_references_listed_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
