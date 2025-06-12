from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    FileMetadata as CogniteFileMetadata,
    FileMetadataWrite as CogniteFileMetadataWrite,
    TimeSeries as CogniteTimeSeries,
    TimeSeriesWrite as CogniteTimeSeriesWrite,
    Sequence as CogniteSequence,
    SequenceWrite as CogniteSequenceWrite,
)
from pydantic import field_validator, model_validator, ValidationInfo

from omni.config import global_config
from omni.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelation,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    FileMetadata,
    FileMetadataWrite,
    FileMetadataGraphQL,
    TimeSeries,
    TimeSeriesWrite,
    TimeSeriesGraphQL,
    TimeSeriesReferenceAPI,
    SequenceRead,
    SequenceWrite,
    SequenceGraphQL,
    T_DomainModelList,
    as_node_id,
    as_read_args,
    as_write_args,
    is_tuple_id,
    as_instance_dict_id,
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    ViewPropertyId,
)


__all__ = [
    "CDFExternalReferencesListed",
    "CDFExternalReferencesListedWrite",
    "CDFExternalReferencesListedList",
    "CDFExternalReferencesListedWriteList",
    "CDFExternalReferencesListedFields",
    "CDFExternalReferencesListedTextFields",
    "CDFExternalReferencesListedGraphQL",
]


CDFExternalReferencesListedTextFields = Literal["external_id", "files", "sequences", "timeseries"]
CDFExternalReferencesListedFields = Literal["external_id", "files", "sequences", "timeseries"]

_CDFEXTERNALREFERENCESLISTED_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
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

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "CDFExternalReferencesListed", "1")
    files: Optional[list[FileMetadataGraphQL]] = None
    sequences: Optional[list[SequenceGraphQL]] = None
    timeseries: Optional[list[TimeSeriesGraphQL]] = None

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
        return CDFExternalReferencesListed.model_validate(as_read_args(self))

    def as_write(self) -> CDFExternalReferencesListedWrite:
        """Convert this GraphQL format of cdf external references listed to the writing format."""
        return CDFExternalReferencesListedWrite.model_validate(as_write_args(self))


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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "CDFExternalReferencesListed", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    files: Optional[list[Union[FileMetadata, str]]] = None
    sequences: Optional[list[Union[SequenceRead, str]]] = None
    timeseries: Optional[list[Union[TimeSeries, str]]] = None

    def as_write(self) -> CDFExternalReferencesListedWrite:
        """Convert this read version of cdf external references listed to the writing version."""
        return CDFExternalReferencesListedWrite.model_validate(as_write_args(self))


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

    _container_fields: ClassVar[tuple[str, ...]] = (
        "files",
        "sequences",
        "timeseries",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "CDFExternalReferencesListed", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    files: Optional[list[Union[FileMetadataWrite, str]]] = None
    sequences: Optional[list[Union[SequenceWrite, str]]] = None
    timeseries: Optional[list[Union[TimeSeriesWrite, str]]] = None


class CDFExternalReferencesListedList(DomainModelList[CDFExternalReferencesListed]):
    """List of cdf external references listeds in the read version."""

    _INSTANCE = CDFExternalReferencesListed

    def as_write(self) -> CDFExternalReferencesListedWriteList:
        """Convert these read versions of cdf external references listed to the writing versions."""
        return CDFExternalReferencesListedWriteList([node.as_write() for node in self.data])


class CDFExternalReferencesListedWriteList(DomainModelWriteList[CDFExternalReferencesListedWrite]):
    """List of cdf external references listeds in the writing version."""

    _INSTANCE = CDFExternalReferencesListedWrite


def _create_cdf_external_references_listed_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _CDFExternalReferencesListedQuery(NodeQueryCore[T_DomainModelList, CDFExternalReferencesListedList]):
    _view_id = CDFExternalReferencesListed._view_id
    _result_cls = CDFExternalReferencesListed
    _result_list_cls_end = CDFExternalReferencesListedList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
    ):

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
            connection_property,
            connection_type,
            reverse_expression,
        )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
            ]
        )
        self.timeseries = TimeSeriesReferenceAPI(
            client,
            lambda limit: [
                ts if isinstance(ts, str) else ts.external_id  # type: ignore[misc]
                for item in self._list(limit=limit)
                if item.timeseries is not None
                for ts in item.timeseries
                if ts is not None and (isinstance(ts, str) or ts.external_id is not None)
            ],
        )

    def list_cdf_external_references_listed(self, limit: int = DEFAULT_QUERY_LIMIT) -> CDFExternalReferencesListedList:
        return self._list(limit=limit)


class CDFExternalReferencesListedQuery(_CDFExternalReferencesListedQuery[CDFExternalReferencesListedList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CDFExternalReferencesListedList)
