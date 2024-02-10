from __future__ import annotations

import warnings
from typing import Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    ResourcesWrite,
    TimeSeries,
)


__all__ = [
    "CDFExternalReferences",
    "CDFExternalReferencesWrite",
    "CDFExternalReferencesApply",
    "CDFExternalReferencesList",
    "CDFExternalReferencesWriteList",
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
        data_record: The data record of the cdf external reference node.
        file: The file field.
        sequence: The sequence field.
        timeseries: The timesery field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    file: Union[str, None] = None
    sequence: Union[str, None] = None
    timeseries: Union[TimeSeries, str, None] = None

    def as_write(self) -> CDFExternalReferencesWrite:
        """Convert this read version of cdf external reference to the writing version."""
        return CDFExternalReferencesWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            file=self.file,
            sequence=self.sequence,
            timeseries=self.timeseries,
        )

    def as_apply(self) -> CDFExternalReferencesWrite:
        """Convert this read version of cdf external reference to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class CDFExternalReferencesWrite(DomainModelWrite):
    """This represents the writing version of cdf external reference.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf external reference.
        data_record: The data record of the cdf external reference node.
        file: The file field.
        sequence: The sequence field.
        timeseries: The timesery field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    file: Union[str, None] = None
    sequence: Union[str, None] = None
    timeseries: Union[TimeSeries, str, None] = None

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            CDFExternalReferences, dm.ViewId("pygen-models", "CDFExternalReferences", "1")
        )

        properties: dict[str, Any] = {}

        if self.file is not None or write_none:
            properties["file"] = self.file

        if self.sequence is not None or write_none:
            properties["sequence"] = self.sequence

        if self.timeseries is not None or write_none:
            if isinstance(self.timeseries, str) or self.timeseries is None:
                properties["timeseries"] = self.timeseries
            else:
                properties["timeseries"] = self.timeseries.external_id

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.data_record.existing_version,
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


class CDFExternalReferencesApply(CDFExternalReferencesWrite):
    def __new__(cls, *args, **kwargs) -> CDFExternalReferencesApply:
        warnings.warn(
            "CDFExternalReferencesApply is deprecated and will be removed in v1.0. Use CDFExternalReferencesWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "CDFExternalReferences.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class CDFExternalReferencesList(DomainModelList[CDFExternalReferences]):
    """List of cdf external references in the read version."""

    _INSTANCE = CDFExternalReferences

    def as_write(self) -> CDFExternalReferencesWriteList:
        """Convert these read versions of cdf external reference to the writing versions."""
        return CDFExternalReferencesWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> CDFExternalReferencesWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class CDFExternalReferencesWriteList(DomainModelWriteList[CDFExternalReferencesWrite]):
    """List of cdf external references in the writing version."""

    _INSTANCE = CDFExternalReferencesWrite


class CDFExternalReferencesApplyList(CDFExternalReferencesWriteList): ...


def _create_cdf_external_reference_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
