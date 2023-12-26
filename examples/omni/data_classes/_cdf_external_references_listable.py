from __future__ import annotations

from typing import Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    TimeSeries,
)


__all__ = [
    "CDFExternalReferencesListable",
    "CDFExternalReferencesListableApply",
    "CDFExternalReferencesListableList",
    "CDFExternalReferencesListableApplyList",
    "CDFExternalReferencesListableFields",
    "CDFExternalReferencesListableTextFields",
]


CDFExternalReferencesListableTextFields = Literal["files", "sequences", "timeseries"]
CDFExternalReferencesListableFields = Literal["files", "sequences", "timeseries"]

_CDFEXTERNALREFERENCESLISTABLE_PROPERTIES_BY_FIELD = {
    "files": "files",
    "sequences": "sequences",
    "timeseries": "timeseries",
}


class CDFExternalReferencesListable(DomainModel):
    """This represents the reading version of cdf external references listable.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf external references listable.
        files: The file field.
        sequences: The sequence field.
        timeseries: The timesery field.
        created_time: The created time of the cdf external references listable node.
        last_updated_time: The last updated time of the cdf external references listable node.
        deleted_time: If present, the deleted time of the cdf external references listable node.
        version: The version of the cdf external references listable node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    files: Optional[list[str]] = None
    sequences: Optional[list[str]] = None
    timeseries: Union[list[TimeSeries], list[str], None] = None

    def as_apply(self) -> CDFExternalReferencesListableApply:
        """Convert this read version of cdf external references listable to the writing version."""
        return CDFExternalReferencesListableApply(
            space=self.space,
            external_id=self.external_id,
            files=self.files,
            sequences=self.sequences,
            timeseries=self.timeseries,
        )


class CDFExternalReferencesListableApply(DomainModelApply):
    """This represents the writing version of cdf external references listable.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf external references listable.
        files: The file field.
        sequences: The sequence field.
        timeseries: The timesery field.
        existing_version: Fail the ingestion request if the cdf external references listable version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    files: Optional[list[str]] = None
    sequences: Optional[list[str]] = None
    timeseries: Union[list[TimeSeries], list[str], None] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "pygen-models", "CDFExternalReferencesListable", "1"
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

        if isinstance(self.timeseries, CogniteTimeSeries):
            resources.time_series.append(self.timeseries)

        return resources


class CDFExternalReferencesListableList(DomainModelList[CDFExternalReferencesListable]):
    """List of cdf external references listables in the read version."""

    _INSTANCE = CDFExternalReferencesListable

    def as_apply(self) -> CDFExternalReferencesListableApplyList:
        """Convert these read versions of cdf external references listable to the writing versions."""
        return CDFExternalReferencesListableApplyList([node.as_apply() for node in self.data])


class CDFExternalReferencesListableApplyList(DomainModelApplyList[CDFExternalReferencesListableApply]):
    """List of cdf external references listables in the writing version."""

    _INSTANCE = CDFExternalReferencesListableApply


def _create_cdf_external_references_listable_filter(
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
