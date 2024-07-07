from __future__ import annotations

import warnings
from typing import Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    TimeSeries,
)


__all__ = [
    "CDFExternalReferencesListed",
    "CDFExternalReferencesListedWrite",
    "CDFExternalReferencesListedApply",
    "CDFExternalReferencesListedList",
    "CDFExternalReferencesListedWriteList",
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


class CDFExternalReferencesListedGraphQL(GraphQLCore):
    """This represents the reading version of cdf external references listed, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf external references listed.
        data_record: The data record of the cdf external references listed node.
        files: The file field.
        sequences: The sequence field.
        timeseries: The timesery field.
    """

    view_id = dm.ViewId("pygen-models", "CDFExternalReferencesListed", "1")
    files: Optional[list[dict]] = None
    sequences: Optional[list[dict]] = None
    timeseries: Union[list[TimeSeries], list[dict], None] = None

    @model_validator(mode="before")
    def parse_data_record(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        if "lastUpdatedTime" in values or "createdTime" in values:
            values["dataRecord"] = DataRecordGraphQL(
                created_time=values.pop("createdTime", None),
                last_updated_time=values.pop("lastUpdatedTime", None),
            )
        return values

    @field_validator("files", "sequences", "timeseries", mode="before")
    def clean_list(cls, value: Any) -> Any:
        if isinstance(value, list):
            return [v for v in value if v is not None] or None
        return value

    def as_read(self) -> CDFExternalReferencesListed:
        """Convert this GraphQL format of cdf external references listed to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return CDFExternalReferencesListed(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            files=[item["externalId"] for item in self.files or [] if "externalId" in item] or None,
            sequences=[item["externalId"] for item in self.sequences or [] if "externalId" in item] or None,
            timeseries=self.timeseries,
        )

    def as_write(self) -> CDFExternalReferencesListedWrite:
        """Convert this GraphQL format of cdf external references listed to the writing format."""
        return CDFExternalReferencesListedWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            files=[item["externalId"] for item in self.files or [] if "externalId" in item] or None,
            sequences=[item["externalId"] for item in self.sequences or [] if "externalId" in item] or None,
            timeseries=self.timeseries,
        )


class CDFExternalReferencesListed(DomainModel):
    """This represents the reading version of cdf external references listed.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf external references listed.
        data_record: The data record of the cdf external references listed node.
        files: The file field.
        sequences: The sequence field.
        timeseries: The timesery field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "CDFExternalReferencesListed", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    files: Optional[list[str]] = None
    sequences: Optional[list[str]] = None
    timeseries: Union[list[TimeSeries], list[str], None] = None

    def as_write(self) -> CDFExternalReferencesListedWrite:
        """Convert this read version of cdf external references listed to the writing version."""
        return CDFExternalReferencesListedWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            files=self.files,
            sequences=self.sequences,
            timeseries=self.timeseries,
        )

    def as_apply(self) -> CDFExternalReferencesListedWrite:
        """Convert this read version of cdf external references listed to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class CDFExternalReferencesListedWrite(DomainModelWrite):
    """This represents the writing version of cdf external references listed.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf external references listed.
        data_record: The data record of the cdf external references listed node.
        files: The file field.
        sequences: The sequence field.
        timeseries: The timesery field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "CDFExternalReferencesListed", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    files: Optional[list[str]] = None
    sequences: Optional[list[str]] = None
    timeseries: Union[list[TimeSeries], list[str], None] = None

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

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
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.timeseries, CogniteTimeSeries):
            resources.time_series.append(self.timeseries)

        return resources


class CDFExternalReferencesListedApply(CDFExternalReferencesListedWrite):
    def __new__(cls, *args, **kwargs) -> CDFExternalReferencesListedApply:
        warnings.warn(
            "CDFExternalReferencesListedApply is deprecated and will be removed in v1.0. Use CDFExternalReferencesListedWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "CDFExternalReferencesListed.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class CDFExternalReferencesListedList(DomainModelList[CDFExternalReferencesListed]):
    """List of cdf external references listeds in the read version."""

    _INSTANCE = CDFExternalReferencesListed

    def as_write(self) -> CDFExternalReferencesListedWriteList:
        """Convert these read versions of cdf external references listed to the writing versions."""
        return CDFExternalReferencesListedWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> CDFExternalReferencesListedWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class CDFExternalReferencesListedWriteList(DomainModelWriteList[CDFExternalReferencesListedWrite]):
    """List of cdf external references listeds in the writing version."""

    _INSTANCE = CDFExternalReferencesListedWrite


class CDFExternalReferencesListedApplyList(CDFExternalReferencesListedWriteList): ...


def _create_cdf_external_references_listed_filter(
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
