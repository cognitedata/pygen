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
    "CDFExternalReferences",
    "CDFExternalReferencesWrite",
    "CDFExternalReferencesList",
    "CDFExternalReferencesWriteList",
    "CDFExternalReferencesFields",
    "CDFExternalReferencesTextFields",
    "CDFExternalReferencesGraphQL",
]


CDFExternalReferencesTextFields = Literal["external_id", "file", "sequence", "timeseries"]
CDFExternalReferencesFields = Literal["external_id", "file", "sequence", "timeseries"]

_CDFEXTERNALREFERENCES_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "file": "file",
    "sequence": "sequence",
    "timeseries": "timeseries",
}


class CDFExternalReferencesGraphQL(GraphQLCore):
    """This represents the reading version of cdf external reference, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the cdf external reference.
        data_record: The data record of the cdf external reference node.
        file: The file field.
        sequence: The sequence field.
        timeseries: The timesery field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "CDFExternalReferences", "1")
    file: Optional[FileMetadataGraphQL] = None
    sequence: Optional[SequenceGraphQL] = None
    timeseries: Optional[TimeSeriesGraphQL] = None

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

    def as_read(self) -> CDFExternalReferences:
        """Convert this GraphQL format of cdf external reference to the reading format."""
        return CDFExternalReferences.model_validate(as_read_args(self))

    def as_write(self) -> CDFExternalReferencesWrite:
        """Convert this GraphQL format of cdf external reference to the writing format."""
        return CDFExternalReferencesWrite.model_validate(as_write_args(self))


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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "CDFExternalReferences", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    file: Union[FileMetadata, str, None] = None
    sequence: Union[SequenceRead, str, None] = None
    timeseries: Union[TimeSeries, str, None] = None

    def as_write(self) -> CDFExternalReferencesWrite:
        """Convert this read version of cdf external reference to the writing version."""
        return CDFExternalReferencesWrite.model_validate(as_write_args(self))


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

    _container_fields: ClassVar[tuple[str, ...]] = (
        "file",
        "sequence",
        "timeseries",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "CDFExternalReferences", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    file: Union[FileMetadataWrite, str, None] = None
    sequence: Union[SequenceWrite, str, None] = None
    timeseries: Union[TimeSeriesWrite, str, None] = None


class CDFExternalReferencesList(DomainModelList[CDFExternalReferences]):
    """List of cdf external references in the read version."""

    _INSTANCE = CDFExternalReferences

    def as_write(self) -> CDFExternalReferencesWriteList:
        """Convert these read versions of cdf external reference to the writing versions."""
        return CDFExternalReferencesWriteList([node.as_write() for node in self.data])


class CDFExternalReferencesWriteList(DomainModelWriteList[CDFExternalReferencesWrite]):
    """List of cdf external references in the writing version."""

    _INSTANCE = CDFExternalReferencesWrite


def _create_cdf_external_reference_filter(
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


class _CDFExternalReferencesQuery(NodeQueryCore[T_DomainModelList, CDFExternalReferencesList]):
    _view_id = CDFExternalReferences._view_id
    _result_cls = CDFExternalReferences
    _result_list_cls_end = CDFExternalReferencesList

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
                item.timeseries if isinstance(item.timeseries, str) else item.timeseries.external_id  # type: ignore[misc]
                for item in self._list(limit=limit)
                if item.timeseries is not None
                and (isinstance(item.timeseries, str) or item.timeseries.external_id is not None)
            ],
        )

    def list_cdf_external_reference(self, limit: int = DEFAULT_QUERY_LIMIT) -> CDFExternalReferencesList:
        return self._list(limit=limit)


class CDFExternalReferencesQuery(_CDFExternalReferencesQuery[CDFExternalReferencesList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CDFExternalReferencesList)
