from __future__ import annotations

from typing import Literal, Optional, Union  # noqa: F401

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
    "CDFExternalReferences",
    "CDFExternalReferencesApply",
    "CDFExternalReferencesList",
    "CDFExternalReferencesApplyList",
    "CDFExternalReferencesFields",
    "CDFExternalReferencesTextFields",
]


CDFExternalReferencesTextFields = Literal["file", "sequence", "timeseries"]
CDFExternalReferencesFields = Literal["file", "sequence", "timeseries"]

_CDFEXTERNALREFERENCES_PROPERTIES_BY_FIELD = {
    "file": "file",
    "sequence": "sequence",
    "timeseries": "timeseries",
}


class CDFExternalReferences(DomainModel):
    """This represents the reading version of cdf external reference.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf external reference.
        file: The file field.
        sequence: The sequence field.
        timeseries: The timesery field.
        created_time: The created time of the cdf external reference node.
        last_updated_time: The last updated time of the cdf external reference node.
        deleted_time: If present, the deleted time of the cdf external reference node.
        version: The version of the cdf external reference node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    file: Union[str, None] = None
    sequence: Union[str, None] = None
    timeseries: Union[TimeSeries, str, None] = None

    def as_apply(self) -> CDFExternalReferencesApply:
        """Convert this read version of cdf external reference to the writing version."""
        return CDFExternalReferencesApply(
            space=self.space,
            external_id=self.external_id,
            file=self.file,
            sequence=self.sequence,
            timeseries=self.timeseries,
        )


class CDFExternalReferencesApply(DomainModelApply):
    """This represents the writing version of cdf external reference.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf external reference.
        file: The file field.
        sequence: The sequence field.
        timeseries: The timesery field.
        existing_version: Fail the ingestion request if the cdf external reference version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    file: Union[str, None] = None
    sequence: Union[str, None] = None
    timeseries: Union[TimeSeries, str, None] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "pygen-models", "CDFExternalReferences", "1"
        )

        properties = {}

        if self.file is not None:
            properties["file"] = self.file

        if self.sequence is not None:
            properties["sequence"] = self.sequence

        if self.timeseries is not None:
            properties["timeseries"] = (
                self.timeseries if isinstance(self.timeseries, str) else self.timeseries.external_id
            )

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


class CDFExternalReferencesList(DomainModelList[CDFExternalReferences]):
    """List of cdf external references in the read version."""

    _INSTANCE = CDFExternalReferences

    def as_apply(self) -> CDFExternalReferencesApplyList:
        """Convert these read versions of cdf external reference to the writing versions."""
        return CDFExternalReferencesApplyList([node.as_apply() for node in self.data])


class CDFExternalReferencesApplyList(DomainModelApplyList[CDFExternalReferencesApply]):
    """List of cdf external references in the writing version."""

    _INSTANCE = CDFExternalReferencesApply


def _create_cdf_external_reference_filter(
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
