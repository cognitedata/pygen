from __future__ import annotations

import warnings
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import TimeSeries
from pydantic import validator, root_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
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
    T_DomainModelList,
    as_direct_relation_reference,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    select_best_node,
    QueryCore,
    NodeQueryCore,
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

    view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "CDFExternalReferences", "1")
    file: Union[dict, None] = None
    sequence: Union[dict, None] = None
    timeseries: Union[TimeSeriesGraphQL, dict, None] = None

    @root_validator(pre=True)
    def parse_data_record(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        if "lastUpdatedTime" in values or "createdTime" in values:
            values["dataRecord"] = DataRecordGraphQL(
                created_time=values.pop("createdTime", None),
                last_updated_time=values.pop("lastUpdatedTime", None),
            )
        return values

    @validator("timeseries", pre=True)
    def parse_timeseries(cls, value: Any) -> Any:
        if isinstance(value, list):
            return [TimeSeries.load(v) if isinstance(v, dict) else v for v in value]
        elif isinstance(value, dict):
            return TimeSeries.load(value)
        return value

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
            file=self.file["externalId"] if self.file and "externalId" in self.file else None,
            sequence=self.sequence["externalId"] if self.sequence and "externalId" in self.sequence else None,
            timeseries=self.timeseries,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> CDFExternalReferencesWrite:
        """Convert this GraphQL format of cdf external reference to the writing format."""
        return CDFExternalReferencesWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            file=self.file["externalId"] if self.file and "externalId" in self.file else None,
            sequence=self.sequence["externalId"] if self.sequence and "externalId" in self.sequence else None,
            timeseries=self.timeseries,
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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "CDFExternalReferences", "1")

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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("pygen-models", "CDFExternalReferences", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    file: Union[str, None] = None
    sequence: Union[str, None] = None
    timeseries: Union[TimeSeries, str, None] = None

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
            properties["file"] = self.file

        if self.sequence is not None or write_none:
            properties["sequence"] = self.sequence

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

        if isinstance(self.timeseries, TimeSeries):
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
    ):

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
        )


class CDFExternalReferencesQuery(_CDFExternalReferencesQuery[CDFExternalReferencesList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, CDFExternalReferencesList)
