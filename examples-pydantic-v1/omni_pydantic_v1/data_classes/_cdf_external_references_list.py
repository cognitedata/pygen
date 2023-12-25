from __future__ import annotations

from typing import Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries

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
    "CDFExternalReferencesList",
    "CDFExternalReferencesListApply",
    "CDFExternalReferencesListList",
    "CDFExternalReferencesListApplyList",
    "CDFExternalReferencesListFields",
    "CDFExternalReferencesListTextFields",
]


CDFExternalReferencesListTextFields = Literal["files", "sequences", "timeseries"]
CDFExternalReferencesListFields = Literal["files", "sequences", "timeseries"]

_CDFEXTERNALREFERENCESLIST_PROPERTIES_BY_FIELD = {
    "files": "files",
    "sequences": "sequences",
    "timeseries": "timeseries",
}


class CDFExternalReferencesList(DomainModel):
    """This represents the reading version of cdf external references list.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf external references list.
        files: The file field.
        sequences: The sequence field.
        timeseries: The timesery field.
        created_time: The created time of the cdf external references list node.
        last_updated_time: The last updated time of the cdf external references list node.
        deleted_time: If present, the deleted time of the cdf external references list node.
        version: The version of the cdf external references list node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    files: Optional[list[str]] = None
    sequences: Optional[list[str]] = None
    timeseries: Optional[list[TimeSeries]] = None

    def as_apply(self) -> CDFExternalReferencesListApply:
        """Convert this read version of cdf external references list to the writing version."""
        return CDFExternalReferencesListApply(
            space=self.space,
            external_id=self.external_id,
            files=self.files,
            sequences=self.sequences,
            timeseries=self.timeseries,
        )


class CDFExternalReferencesListApply(DomainModelApply):
    """This represents the writing version of cdf external references list.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf external references list.
        files: The file field.
        sequences: The sequence field.
        timeseries: The timesery field.
        existing_version: Fail the ingestion request if the cdf external references list version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    files: Optional[list[str]] = None
    sequences: Optional[list[str]] = None
    timeseries: Optional[list[TimeSeries]] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "pygen-models", "CDFExternalReferencesList", "1"
        )

        properties = {}

        if self.files is not None:
            properties["files"] = self.files

        if self.sequences is not None:
            properties["sequences"] = self.sequences

        if self.timeseries is not None:
            properties["timeseries"] = [
                value if isinstance(value, str) else value.external_id for value in self.timeseries
            ]

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

        if isinstance(self.timeseries, TimeSeries):
            resources.time_series.append(self.timeseries)

        return resources


class CDFExternalReferencesListList(DomainModelList[CDFExternalReferencesList]):
    """List of cdf external references lists in the read version."""

    _INSTANCE = CDFExternalReferencesList

    def as_apply(self) -> CDFExternalReferencesListApplyList:
        """Convert these read versions of cdf external references list to the writing versions."""
        return CDFExternalReferencesListApplyList([node.as_apply() for node in self.data])


class CDFExternalReferencesListApplyList(DomainModelApplyList[CDFExternalReferencesListApply]):
    """List of cdf external references lists in the writing version."""

    _INSTANCE = CDFExternalReferencesListApply


def _create_cdf_external_references_list_filter(
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
