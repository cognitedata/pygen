from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    FileMetadata as CogniteFileMetadata,
    FileMetadataWrite as CogniteFileMetadataWrite,
    TimeSeries as CogniteTimeSeries,
    TimeSeriesWrite as CogniteTimeSeriesWrite,
    Sequence as CogniteSequence,
    SequenceWrite as CogniteSequenceWrite,
)
from pydantic import field_validator, model_validator

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
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    is_tuple_id,
    select_best_node,
    QueryCore,
    NodeQueryCore,
    StringFilter,
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> CDFExternalReferences:
        """Convert this GraphQL format of cdf external reference to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return CDFExternalReferences(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            file=self.file.as_read() if self.file else None,
            sequence=self.sequence.as_read() if self.sequence else None,
            timeseries=self.timeseries.as_read() if self.timeseries else None,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CDFExternalReferencesWrite:
        """Convert this GraphQL format of cdf external reference to the writing format."""
        return CDFExternalReferencesWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            file=self.file.as_write() if self.file else None,
            sequence=self.sequence.as_write() if self.sequence else None,
            timeseries=self.timeseries.as_write() if self.timeseries else None,
        )


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
        return CDFExternalReferencesWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            file=self.file.as_write() if isinstance(self.file, CogniteFileMetadata) else self.file,
            sequence=self.sequence.as_write() if isinstance(self.sequence, CogniteSequence) else self.sequence,
            timeseries=(
                self.timeseries.as_write() if isinstance(self.timeseries, CogniteTimeSeries) else self.timeseries
            ),
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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_models", "CDFExternalReferences", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    file: Union[FileMetadataWrite, str, None] = None
    sequence: Union[SequenceWrite, str, None] = None
    timeseries: Union[TimeSeriesWrite, str, None] = None

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

        if self.file is not None or write_none:
            properties["file"] = self.file if isinstance(self.file, str) or self.file is None else self.file.external_id

        if self.sequence is not None or write_none:
            properties["sequence"] = (
                self.sequence if isinstance(self.sequence, str) or self.sequence is None else self.sequence.external_id
            )

        if self.timeseries is not None or write_none:
            properties["timeseries"] = (
                self.timeseries
                if isinstance(self.timeseries, str) or self.timeseries is None
                else self.timeseries.external_id
            )

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=as_direct_relation_reference(self.node_type),
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.file, CogniteFileMetadataWrite):
            resources.files.append(self.file)

        if isinstance(self.sequence, CogniteSequenceWrite):
            resources.sequences.append(self.sequence)

        if isinstance(self.timeseries, CogniteTimeSeriesWrite):
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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
    ):

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
            connection_type,
            reverse_expression,
        )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.timeseries = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.timeseries if isinstance(item.timeseries, str) else item.timeseries.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.timeseries is not None and
               (isinstance(item.timeseries, str) or item.timeseries.external_id is not None)
        ])

    def list_cdf_external_reference(self, limit: int = DEFAULT_QUERY_LIMIT) -> CDFExternalReferencesList:
        return self._list(limit=limit)


class CDFExternalReferencesQuery(_CDFExternalReferencesQuery[CDFExternalReferencesList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CDFExternalReferencesList)
