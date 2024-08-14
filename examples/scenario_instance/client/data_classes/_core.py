from __future__ import annotations

import datetime
import math
import sys
import warnings

from abc import ABC, abstractmethod
from collections import UserList
from collections import defaultdict
from collections.abc import Collection, Mapping
from collections.abc import MutableSequence, Iterable
from dataclasses import dataclass, field
from typing import (
    Annotated,
    Callable,
    cast,
    ClassVar,
    Generic,
    Optional,
    Any,
    Iterator,
    TypeVar,
    overload,
    Union,
    SupportsIndex,
    Literal,
)

import pandas as pd
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from cognite.client.data_classes import TimeSeriesList, Datapoints
from cognite.client.data_classes.data_modeling.instances import (
    Instance,
    InstanceApply,
    Properties,
    PropertyValue,
)
from cognite.client.utils import datetime_to_ms
from pydantic import BaseModel, BeforeValidator, Field, model_validator
from pydantic.functional_serializers import PlainSerializer

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


TimeSeries = Annotated[
    CogniteTimeSeries,
    PlainSerializer(
        lambda v: v.dump(camel_case=True) if isinstance(v, CogniteTimeSeries) else v,
        return_type=dict,
        when_used="unless-none",
    ),
    BeforeValidator(lambda v: CogniteTimeSeries.load(v) if isinstance(v, dict) else v),
]


class TimeSeriesGraphQL(BaseModel, arbitrary_types_allowed=True, populate_by_name=True):
    id: Optional[int] = None
    external_id: Optional[str] = Field(None, alias="externalId")
    instance_id: Optional[dm.NodeId] = Field(None, alias="instanceId")
    name: Optional[str] = None
    is_string: Optional[bool] = Field(None, alias="isString")
    metadata: Optional[dict[str, str]] = None
    unit: Optional[str] = None
    unit_external_id: Optional[str] = Field(None, alias="unitExternalId")
    asset_id: Optional[int] = Field(None, alias="assetId")
    is_step: Optional[bool] = Field(None, alias="isStep")
    description: Optional[str] = None
    security_categories: Optional[list[int]] = Field(None, alias="securityCategories")
    data_set_id: Optional[int] = Field(None, alias="dataSetId")
    created_time: Optional[int] = Field(None, alias="createdTime")
    last_updated_time: Optional[int] = Field(None, alias="lastUpdatedTime")
    data: Optional[Datapoints] = None

    @model_validator(mode="before")
    def parse_datapoints(cls, data: dict[str, Any]) -> dict[str, Any]:
        if "getDataPoints" in data:
            datapoints = data.pop("getDataPoints")
            if "items" in datapoints:
                for item in datapoints["items"]:
                    item["timestamp"] = datetime_to_ms(
                        datetime.datetime.fromisoformat(item["timestamp"].replace("Z", "+00:00"))
                    )
                data["datapoints"] = datapoints["items"]
                data["data"] = Datapoints.load(data)
        return data


DEFAULT_QUERY_LIMIT = 5
INSTANCE_QUERY_LIMIT = 1_000
# This is the actual limit of the API, we typically set it to a lower value to avoid hitting the limit.
ACTUAL_INSTANCE_QUERY_LIMIT = 10_000
DEFAULT_INSTANCE_SPACE = "IntegrationTestsImmutable"


class _NotSetSentinel:
    """This is a special class that indicates that a value has not been set.
    It is used when we need to distinguish between not set and None."""

    ...


@dataclass
class ResourcesWrite:
    nodes: dm.NodeApplyList = field(default_factory=lambda: dm.NodeApplyList([]))
    edges: dm.EdgeApplyList = field(default_factory=lambda: dm.EdgeApplyList([]))
    time_series: TimeSeriesList = field(default_factory=lambda: TimeSeriesList([]))

    def extend(self, other: ResourcesWrite) -> None:
        self.nodes.extend(other.nodes)
        self.edges.extend(other.edges)
        self.time_series.extend(other.time_series)


@dataclass
class ResourcesWriteResult:
    nodes: dm.NodeApplyResultList = field(default_factory=lambda: dm.NodeApplyResultList([]))
    edges: dm.EdgeApplyResultList = field(default_factory=lambda: dm.EdgeApplyResultList([]))
    time_series: TimeSeriesList = field(default_factory=lambda: TimeSeriesList([]))


# Arbitrary types are allowed to be able to use the TimeSeries class
class Core(BaseModel, arbitrary_types_allowed=True, populate_by_name=True):
    def to_pandas(self) -> pd.Series:
        return pd.Series(self.model_dump())

    def _repr_html_(self) -> str:
        """Returns HTML representation of DomainModel."""
        return self.to_pandas().to_frame("value")._repr_html_()  # type: ignore[operator]

    def dump(self, by_alias: bool = True) -> dict[str, Any]:
        """Returns the item as a dictionary.

        Args:
            by_alias: Whether to use the alias names in the dictionary.

        Returns:
            The item as a dictionary.

        """
        return self.model_dump(by_alias=by_alias)


T_Core = TypeVar("T_Core", bound=Core)


class DataRecordGraphQL(Core):
    last_updated_time: Optional[datetime.datetime] = Field(None, alias="lastUpdatedTime")
    created_time: Optional[datetime.datetime] = Field(None, alias="createdTime")


class GraphQLCore(Core, ABC):
    view_id: ClassVar[dm.ViewId]
    space: Optional[str] = None
    external_id: Optional[str] = Field(None, alias="externalId")
    data_record: Optional[DataRecordGraphQL] = Field(None, alias="dataRecord")


class PageInfo(BaseModel):
    has_next_page: Optional[bool] = Field(None, alias="hasNextPage")
    has_previous_page: Optional[bool] = Field(None, alias="hasPreviousPage")
    start_cursor: Optional[str] = Field(None, alias="startCursor")
    end_cursor: Optional[str] = Field(None, alias="endCursor")

    @classmethod
    def load(cls, data: dict[str, Any]) -> PageInfo:
        return cls.model_validate(data)


class GraphQLList(UserList):
    def __init__(self, nodes: Collection[GraphQLCore] | None = None):
        super().__init__(nodes or [])
        self.page_info: PageInfo | None = None

    # The dunder implementations are to get proper type hints
    def __iter__(self) -> Iterator[GraphQLCore]:
        return super().__iter__()

    @overload
    def __getitem__(self, item: SupportsIndex) -> GraphQLCore: ...

    @overload
    def __getitem__(self, item: slice) -> GraphQLList: ...

    def __getitem__(self, item: SupportsIndex | slice) -> GraphQLCore | GraphQLList:
        value = self.data[item]
        if isinstance(item, slice):
            return type(self)(value)
        return cast(GraphQLCore, value)

    def dump(self) -> list[dict[str, Any]]:
        return [node.model_dump() for node in self.data]

    def to_pandas(self) -> pd.DataFrame:
        """
        Convert the list of nodes to a pandas.DataFrame.

        Returns:
            A pandas.DataFrame with the nodes as rows.
        """
        df = pd.DataFrame(self.dump())
        if df.empty:
            df = pd.DataFrame(columns=GraphQLCore.model_fields)
        # Reorder columns to have the most relevant first
        id_columns = ["space", "external_id"]
        end_columns = ["data_record"]
        fixed_columns = set(id_columns + end_columns)
        columns = (
            id_columns + [col for col in df if col not in fixed_columns] + [col for col in end_columns if col in df]
        )
        return df[columns]

    def _repr_html_(self) -> str:
        return self.to_pandas()._repr_html_()  # type: ignore[operator]


def as_node_id(value: dm.DirectRelationReference) -> dm.NodeId:
    return dm.NodeId(space=value.space, external_id=value.external_id)


class DomainModelCore(Core, ABC):
    _view_id: ClassVar[dm.ViewId]

    space: str
    external_id: str = Field(min_length=1, max_length=255, alias="externalId")

    def as_tuple_id(self) -> tuple[str, str]:
        return self.space, self.external_id

    def as_direct_reference(self) -> dm.DirectRelationReference:
        return dm.DirectRelationReference(space=self.space, external_id=self.external_id)

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | dm.EdgeId | str, Self],
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        # This is used when unpacking a query result and should be overridden in the subclasses
        return None


T_DomainModelCore = TypeVar("T_DomainModelCore", bound=DomainModelCore)


class DataRecord(BaseModel):
    """The data record represents the metadata of a node.

    Args:
        created_time: The created time of the node.
        last_updated_time: The last updated time of the node.
        deleted_time: If present, the deleted time of the node.
        version: The version of the node.
    """

    version: int
    last_updated_time: datetime.datetime
    created_time: datetime.datetime
    deleted_time: Optional[datetime.datetime] = None


class DomainModel(DomainModelCore, ABC):
    data_record: DataRecord
    node_type: Optional[dm.DirectRelationReference] = None

    def as_id(self) -> dm.NodeId:
        return dm.NodeId(space=self.space, external_id=self.external_id)

    @classmethod
    def from_instance(cls, instance: Instance) -> Self:
        data = instance.dump(camel_case=False)
        node_type = data.pop("type", None)
        space = data.pop("space")
        external_id = data.pop("external_id")
        return cls(
            space=space,
            external_id=external_id,
            data_record=DataRecord(**data),
            node_type=node_type,
            **unpack_properties(instance.properties),
        )


T_DomainModel = TypeVar("T_DomainModel", bound=DomainModel)


def as_pygen_node_id(value: DomainModel | dm.NodeId | str) -> dm.NodeId | str:
    if isinstance(value, str):
        return value
    elif value.space == DEFAULT_INSTANCE_SPACE:
        return value.external_id
    elif isinstance(value, dm.NodeId):
        return value
    return value.as_id()


def are_nodes_equal(node1: DomainModel | str | dm.NodeId, node2: DomainModel | str | dm.NodeId) -> bool:
    if isinstance(node1, (str, dm.NodeId)):
        node1_id = node1
    else:
        node1_id = node1.as_id() if node1.space != DEFAULT_INSTANCE_SPACE else node1.external_id
    if isinstance(node2, (str, dm.NodeId)):
        node2_id = node2
    else:
        node2_id = node2.as_id() if node2.space != DEFAULT_INSTANCE_SPACE else node2.external_id
    return node1_id == node2_id


def select_best_node(
    node1: T_DomainModel | str | dm.NodeId, node2: T_DomainModel | str | dm.NodeId
) -> T_DomainModel | str | dm.NodeId:
    if isinstance(node1, DomainModel):
        return node1  # type: ignore[return-value]
    elif isinstance(node2, DomainModel):
        return node2  # type: ignore[return-value]
    else:
        return node1


class DataRecordWrite(BaseModel):
    """The data record represents the metadata of a node.

    Args:
        existing_version: Fail the ingestion request if the node version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    existing_version: Optional[int] = None


T_DataRecord = TypeVar("T_DataRecord", bound=Union[DataRecord, DataRecordWrite])


class _DataRecordListCore(UserList, Generic[T_DataRecord]):
    _INSTANCE: type[T_DataRecord]

    def __init__(self, nodes: Collection[T_DataRecord] | None = None):
        super().__init__(nodes or [])

    # The dunder implementations are to get proper type hints
    def __iter__(self) -> Iterator[T_DataRecord]:
        return super().__iter__()

    @overload
    def __getitem__(self, item: SupportsIndex) -> T_DataRecord: ...

    @overload
    def __getitem__(self, item: slice) -> _DataRecordListCore[T_DataRecord]: ...

    def __getitem__(self, item: SupportsIndex | slice) -> T_DataRecord | _DataRecordListCore[T_DataRecord]:
        value = self.data[item]
        if isinstance(item, slice):
            return type(self)(value)
        return cast(T_DataRecord, value)

    def to_pandas(self) -> pd.DataFrame:
        """
        Convert the list of nodes to a pandas.DataFrame.

        Returns:
            A pandas.DataFrame with the nodes as rows.
        """
        df = pd.DataFrame([item.model_dump() for item in self])
        if df.empty:
            df = pd.DataFrame(columns=self._INSTANCE.model_fields)
        return df

    def _repr_html_(self) -> str:
        return self.to_pandas()._repr_html_()  # type: ignore[operator]


class DataRecordList(_DataRecordListCore[DataRecord]):
    _INSTANCE = DataRecord


class DataRecordWriteList(_DataRecordListCore[DataRecordWrite]):
    _INSTANCE = DataRecordWrite


class DomainModelWrite(DomainModelCore, extra="ignore", populate_by_name=True):
    external_id_factory: ClassVar[Optional[Callable[[type[DomainModelWrite], dict], str]]] = None
    data_record: DataRecordWrite = Field(default_factory=DataRecordWrite)
    node_type: Optional[dm.DirectRelationReference] = None

    def as_id(self) -> dm.NodeId:
        return dm.NodeId(space=self.space, external_id=self.external_id)

    def to_instances_write(
        self,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        return self._to_instances_write(set(), write_none, allow_version_increase)

    def to_instances_apply(self, write_none: bool = False) -> ResourcesWrite:
        warnings.warn(
            "to_instances_apply is deprecated and will be removed in v1.0. Use to_instances_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.to_instances_write(write_none)

    @abstractmethod
    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        raise NotImplementedError()

    @model_validator(mode="before")
    def create_external_id_if_factory(cls, data: Any) -> Any:
        if (
            isinstance(data, dict)
            and cls.external_id_factory is not None
            and data.get("external_id") is None
            and data.get("externalId") is None
        ):
            data["external_id"] = cls.external_id_factory(cls, data)
        return data

    @classmethod
    def from_instance(cls: type[T_DomainModelWrite], instance: InstanceApply) -> T_DomainModelWrite:
        data = instance.dump(camel_case=False)
        data.pop("instance_type", None)
        node_type = data.pop("type", None)
        space = data.pop("space")
        external_id = data.pop("external_id")
        sources = data.pop("sources", [])
        properties = {}
        for source in sources:
            for prop_name, prop_value in source["properties"].items():
                if isinstance(prop_value, dict) and "externalId" in prop_value and "space" in prop_value:
                    if prop_value["space"] == DEFAULT_INSTANCE_SPACE:
                        properties[prop_name] = prop_value["externalId"]
                    else:
                        properties[prop_name] = dm.NodeId(
                            space=prop_value["space"], external_id=prop_value["externalId"]
                        )
                else:
                    properties[prop_name] = prop_value
        return cls(
            space=space, external_id=external_id, node_type=node_type, data_record=DataRecordWrite(**data), **properties
        )


T_DomainModelWrite = TypeVar("T_DomainModelWrite", bound=DomainModelWrite)


class CoreList(UserList, Generic[T_Core]):
    _INSTANCE: type[T_Core]
    _PARENT_CLASS: type[Core]

    def __init__(self, nodes: Collection[T_Core] | None = None):
        super().__init__(nodes or [])

    # The dunder implementations are to get proper type hints
    def __iter__(self) -> Iterator[T_Core]:
        return super().__iter__()

    @overload
    def __getitem__(self, item: SupportsIndex) -> T_Core: ...

    @overload
    def __getitem__(self, item: slice) -> Self: ...

    def __getitem__(self, item: SupportsIndex | slice) -> T_Core | Self:
        value = self.data[item]
        if isinstance(item, slice):
            return type(self)(value)
        return cast(T_Core, value)

    def dump(self) -> list[dict[str, Any]]:
        return [node.model_dump() for node in self.data]

    def as_external_ids(self) -> list[str]:
        return [node.external_id for node in self.data]

    def to_pandas(self) -> pd.DataFrame:
        """
        Convert the list of nodes to a pandas.DataFrame.

        Returns:
            A pandas.DataFrame with the nodes as rows.
        """
        df = pd.DataFrame(self.dump())
        if df.empty:
            df = pd.DataFrame(columns=self._INSTANCE.model_fields)
        # Reorder columns to have the most relevant first
        id_columns = ["space", "external_id"]
        end_columns = ["node_type", "data_record"]
        fixed_columns = set(id_columns + end_columns)
        columns = (
            id_columns + [col for col in df if col not in fixed_columns] + [col for col in end_columns if col in df]
        )
        return df[columns]

    def _repr_html_(self) -> str:
        return self.to_pandas()._repr_html_()  # type: ignore[operator]


class DomainModelList(CoreList[T_DomainModel]):
    _PARENT_CLASS = DomainModel

    @property
    def data_records(self) -> DataRecordList:
        return DataRecordList([node.data_record for node in self.data])

    def as_node_ids(self) -> list[dm.NodeId]:
        return [dm.NodeId(space=node.space, external_id=node.external_id) for node in self.data]


T_DomainModelList = TypeVar("T_DomainModelList", bound=DomainModelList, covariant=True)


class DomainModelWriteList(CoreList[T_DomainModelWrite]):
    _PARENT_CLASS = DomainModelWrite

    @property
    def data_records(self) -> DataRecordWriteList:
        return DataRecordWriteList([node.data_record for node in self])

    def as_node_ids(self) -> list[dm.NodeId]:
        return [dm.NodeId(space=node.space, external_id=node.external_id) for node in self]

    def to_instances_write(
        self,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        cache: set[tuple[str, str]] = set()
        domains = ResourcesWrite()
        for node in self:
            result = node._to_instances_write(cache, write_none, allow_version_increase)
            domains.extend(result)
        return domains

    def to_instances_apply(self, write_none: bool = False) -> ResourcesWrite:
        warnings.warn(
            "to_instances_apply is deprecated and will be removed in v1.0. Use to_instances_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.to_instances_write(write_none)


T_DomainModelWriteList = TypeVar("T_DomainModelWriteList", bound=DomainModelWriteList, covariant=True)


class DomainRelation(DomainModelCore):
    edge_type: dm.DirectRelationReference
    start_node: dm.DirectRelationReference
    end_node: Any
    data_record: DataRecord

    def as_id(self) -> dm.EdgeId:
        return dm.EdgeId(space=self.space, external_id=self.external_id)

    @classmethod
    def from_instance(cls, instance: Instance) -> Self:
        data = instance.dump(camel_case=False)
        data.pop("instance_type", None)
        edge_type = data.pop("type", None)
        start_node = data.pop("start_node")
        end_node = data.pop("end_node")
        space = data.pop("space")
        external_id = data.pop("external_id")
        return cls(
            space=space,
            external_id=external_id,
            data_record=DataRecord(**data),
            edge_type=edge_type,
            start_node=start_node,
            end_node=end_node,
            **unpack_properties(instance.properties),
        )


T_DomainRelation = TypeVar("T_DomainRelation", bound=DomainRelation)


def default_edge_external_id_factory(
    start_node: DomainModelWrite | str | dm.NodeId,
    end_node: DomainModelWrite | str | dm.NodeId,
    edge_type: dm.DirectRelationReference,
) -> str:
    start = start_node if isinstance(start_node, str) else start_node.external_id
    end = end_node if isinstance(end_node, str) else end_node.external_id
    return f"{start}:{end}"


class DomainRelationWrite(Core, extra="forbid", populate_by_name=True):
    external_id_factory: ClassVar[
        Callable[
            [
                Union[DomainModelWrite, str, dm.NodeId],
                Union[DomainModelWrite, str, dm.NodeId],
                dm.DirectRelationReference,
            ],
            str,
        ]
    ] = default_edge_external_id_factory
    data_record: DataRecordWrite = Field(default_factory=DataRecordWrite)
    external_id: Optional[str] = Field(None, min_length=1, max_length=255)

    @abstractmethod
    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        start_node: DomainModelWrite,
        edge_type: dm.DirectRelationReference,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        raise NotImplementedError()

    @classmethod
    def create_edge(
        cls,
        start_node: DomainModelWrite | str | dm.NodeId,
        end_node: DomainModelWrite | str | dm.NodeId,
        edge_type: dm.DirectRelationReference,
    ) -> dm.EdgeApply:
        if isinstance(start_node, (DomainModelWrite, dm.NodeId)):
            space = start_node.space
        elif isinstance(end_node, (DomainModelWrite, dm.NodeId)):
            space = end_node.space
        else:
            space = DEFAULT_INSTANCE_SPACE

        if isinstance(end_node, str):
            end_ref = dm.DirectRelationReference(space, end_node)
        elif isinstance(end_node, DomainModelWrite):
            end_ref = end_node.as_direct_reference()
        elif isinstance(end_node, dm.NodeId):
            end_ref = dm.DirectRelationReference(end_node.space, end_node.external_id)
        else:
            raise TypeError(f"Expected str or subclass of {DomainRelationWrite.__name__}, got {type(end_node)}")

        if isinstance(start_node, str):
            start_ref = dm.DirectRelationReference(space, start_node)
        elif isinstance(start_node, DomainModelWrite):
            start_ref = start_node.as_direct_reference()
        elif isinstance(start_node, dm.NodeId):
            start_ref = dm.DirectRelationReference(start_node.space, start_node.external_id)
        else:
            raise TypeError(f"Expected str or subclass of {DomainRelationWrite.__name__}, got {type(start_node)}")

        return dm.EdgeApply(
            space=space,
            external_id=cls.external_id_factory(start_node, end_node, edge_type),
            type=edge_type,
            start_node=start_ref,
            end_node=end_ref,
        )

    @classmethod
    def from_edge_to_resources(
        cls,
        cache: set[tuple[str, str]],
        start_node: DomainModelWrite | str | dm.NodeId,
        end_node: DomainModelWrite | str | dm.NodeId,
        edge_type: dm.DirectRelationReference,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        edge = DomainRelationWrite.create_edge(start_node, end_node, edge_type)
        if (edge.space, edge.external_id) in cache:
            return resources
        resources.edges.append(edge)
        cache.add((edge.space, edge.external_id))

        if isinstance(end_node, DomainModelWrite):
            other_resources = end_node._to_instances_write(
                cache,
                write_none,
                allow_version_increase,
            )
            resources.extend(other_resources)
        if isinstance(start_node, DomainModelWrite):
            other_resources = start_node._to_instances_write(
                cache,
                write_none,
                allow_version_increase,
            )
            resources.extend(other_resources)

        return resources

    @classmethod
    def reset_external_id_factory(cls) -> None:
        cls.external_id_factory = default_edge_external_id_factory


T_DomainRelationWrite = TypeVar("T_DomainRelationWrite", bound=DomainRelationWrite)


class DomainRelationList(CoreList[T_DomainRelation]):
    _PARENT_CLASS = DomainRelation

    def as_edge_ids(self) -> list[dm.EdgeId]:
        return [edge.as_id() for edge in self.data]

    @property
    def data_records(self) -> DataRecordList:
        return DataRecordList([connection.data_record for connection in self.data])


class DomainRelationWriteList(CoreList[T_DomainRelationWrite]):
    _PARENT_CLASS = DomainRelationWrite

    @property
    def data_records(self) -> DataRecordWriteList:
        return DataRecordWriteList([connection.data_record for connection in self.data])

    def as_edge_ids(self) -> list[dm.EdgeId]:
        return [edge.as_id() for edge in self.data]


T_DomainRelationList = TypeVar("T_DomainRelationList", bound=DomainRelationList)


def unpack_properties(properties: Properties) -> Mapping[str, PropertyValue | dm.NodeId]:
    unpacked: dict[str, PropertyValue | dm.NodeId] = {}
    for view_properties in properties.values():
        for prop_name, prop_value in view_properties.items():
            if isinstance(prop_value, dict) and "externalId" in prop_value and "space" in prop_value:
                if prop_value["space"] == DEFAULT_INSTANCE_SPACE:
                    unpacked[prop_name] = prop_value["externalId"]
                else:
                    unpacked[prop_name] = dm.NodeId(space=prop_value["space"], external_id=prop_value["externalId"])
            elif isinstance(prop_value, list):
                values = []
                for value in prop_value:
                    if isinstance(value, dict) and "externalId" in value and "space" in value:
                        if value["space"] == DEFAULT_INSTANCE_SPACE:
                            values.append(value["externalId"])
                        else:
                            values.append(dm.NodeId(space=value["space"], external_id=value["externalId"]))
                    else:
                        values.append(value)
                unpacked[prop_name] = values
            else:
                unpacked[prop_name] = prop_value
    return unpacked


T_DomainList = TypeVar("T_DomainList", bound=Union[DomainModelList, DomainRelationList], covariant=True)
T_DomainListEnd = TypeVar("T_DomainListEnd", bound=Union[DomainModelList, DomainRelationList], covariant=True)


class QueryCore(Generic[T_DomainList, T_DomainListEnd]):
    _view_id: ClassVar[dm.ViewId]
    _result_list_cls_end: type[T_DomainListEnd]
    _result_cls: ClassVar[type[DomainModelCore]]

    def __init__(
        self,
        created_types: set[type],
        creation_path: "list[QueryCore]",
        client: CogniteClient,
        result_list_cls: type[T_DomainList],
        expression: dm.query.ResultSetExpression | None = None,
        view_filter: dm.filters.Filter | None = None,
        connection_name: str | None = None,
    ):
        created_types.add(type(self))
        self._creation_path = creation_path[:] + [self]
        self._client = client
        self._result_list_cls = result_list_cls
        self._view_filter = view_filter
        self._expression = expression or dm.query.NodeResultSetExpression()
        self._connection_name = connection_name
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.space = StringFilter(self, ["node", "space"])
        self._filter_classes: list[Filtering] = [self.external_id, self.space]

    @property
    def _connection_names(self) -> set[str]:
        return {step._connection_name for step in self._creation_path if step._connection_name}

    def __getattr__(self, item: str) -> Any:
        if item in self._connection_names:
            nodes = [step._result_cls.__name__ for step in self._creation_path]
            raise ValueError(f"Circular reference detected. Cannot query a circular reference: {nodes}")
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{item}'")

    def _assemble_filter(self) -> dm.filters.Filter:
        filters: list[dm.filters.Filter] = [self._view_filter] if self._view_filter else []
        for filter_cls in self._filter_classes:
            if item := filter_cls._as_filter():
                filters.append(item)
        return dm.filters.And(*filters)

    def _repr_html_(self) -> str:
        nodes = [step._result_cls.__name__ for step in self._creation_path]
        edges = [step._connection_name or "missing" for step in self._creation_path[1:]]
        w = 120
        h = 40
        circles = "    \n".join(f'<circle cx="{i * w + 40}" cy="{h}" r="2" />' for i in range(len(nodes)))
        circle_text = "    \n".join(
            f'<text x="{i * w + 40}" y="{h}" dy="-10">{node}</text>' for i, node in enumerate(nodes)
        )
        arrows = "    \n".join(
            f'<path id="arrow-line"  marker-end="url(#head)" stroke-width="2" fill="none" stroke="black" d="M{i*w+40},{h}, {i*w + 150} {h}" />'
            for i in range(len(edges))
        )
        arrow_text = "    \n".join(
            f'<text x="{i*w+40+120/2}" y="{h}" dy="-5">{edge}</text>' for i, edge in enumerate(edges)
        )

        return f"""<h5>Query</h5>
<div>
<svg height="50" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <marker
      id='head'
      orient="auto"
      markerWidth='3'
      markerHeight='4'
      refX='0.1'
      refY='2'
    >
      <path d='M0,0 V4 L2,2 Z' fill="black" />
    </marker>
  </defs>

    {arrows}

<g stroke="black" stroke-width="3" fill="black">
    {circles}
</g>
<g font-size="10" font-family="sans-serif" text-anchor="middle">
    {arrow_text}
</g>
<g font-size="10" font-family="sans-serif" text-anchor="middle">
    {circle_text}
</g>
</svg>
</div>
<p>Call <em>.execute()</em> to return a list of {nodes[0].title()} and
<em>.list()</em> to return a list of {nodes[-1].title()}.</p>
"""


class NodeQueryCore(QueryCore[T_DomainModelList, T_DomainListEnd]):
    _result_cls: ClassVar[type[DomainModel]]

    def execute(self, limit: int = DEFAULT_QUERY_LIMIT) -> T_DomainModelList:
        builder = self._create_query(limit, self._result_list_cls)
        return builder.execute(self._client)

    def list(self, limit: int = DEFAULT_QUERY_LIMIT) -> T_DomainListEnd:
        builder = self._create_query(limit, cast(type[DomainModelList], self._result_list_cls_end))
        for step in builder[:-1]:
            step.select = None
        return builder.execute(self._client)

    def _create_query(self, limit: int, result_list_cls: type[DomainModelList]) -> QueryBuilder:
        builder = QueryBuilder(result_list_cls)
        from_: str | None = None
        first: bool = True
        for item in self._creation_path:
            name = builder.create_name(from_)
            max_retrieve_limit = limit if first else -1
            step: QueryStep
            if isinstance(item, NodeQueryCore) and isinstance(item._expression, dm.query.NodeResultSetExpression):
                step = NodeQueryStep(
                    name=name,
                    expression=item._expression,
                    result_cls=item._result_cls,
                    max_retrieve_limit=max_retrieve_limit,
                )
                step.expression.from_ = from_
                step.expression.filter = item._assemble_filter()
                builder.append(step)
            elif isinstance(item, NodeQueryCore) and isinstance(item._expression, dm.query.EdgeResultSetExpression):
                edge_name = name
                step = EdgeQueryStep(name=edge_name, expression=item._expression, max_retrieve_limit=max_retrieve_limit)
                step.expression.from_ = from_
                builder.append(step)

                name = builder.create_name(edge_name)
                node_step = NodeQueryStep(
                    name=name,
                    expression=dm.query.NodeResultSetExpression(
                        from_=edge_name,
                        filter=item._assemble_filter(),
                    ),
                    result_cls=item._result_cls,
                )
                builder.append(node_step)
            elif isinstance(item, EdgeQueryCore):
                step = EdgeQueryStep(
                    name=name,
                    expression=cast(dm.query.EdgeResultSetExpression, item._expression),
                    result_cls=item._result_cls,
                )
                step.expression.from_ = from_
                step.expression.filter = item._assemble_filter()
                builder.append(step)
            else:
                raise TypeError(f"Unsupported query step type: {type(item._expression)}")

            first = False
            from_ = name
        return builder


class EdgeQueryCore(QueryCore[T_DomainList, T_DomainListEnd]):
    _result_cls: ClassVar[type[DomainRelation]]


class QueryStep:
    def __init__(
        self,
        name: str,
        expression: dm.query.ResultSetExpression,
        max_retrieve_limit: int = -1,
        select: dm.query.Select | None | type[_NotSetSentinel] = _NotSetSentinel,
    ):
        self.name = name
        self.expression = expression
        self.max_retrieve_limit = max_retrieve_limit
        self.select: dm.query.Select | None
        if select is _NotSetSentinel:
            self.select = self._default_select()
        else:
            self.select = select  # type: ignore[assignment]
        self.cursor: str | None = None
        self.total_retrieved: int = 0
        self.last_batch_count: int = 0
        self.results: list[Instance] = []

    @abstractmethod
    def _default_select(self) -> dm.query.Select:
        raise NotImplementedError()

    @property
    def from_(self) -> str | None:
        return self.expression.from_

    @property
    def is_single_direct_relation(self) -> bool:
        return isinstance(self.expression, dm.query.NodeResultSetExpression) and self.expression.through is not None

    def update_expression_limit(self) -> None:
        if self.is_unlimited:
            self.expression.limit = ACTUAL_INSTANCE_QUERY_LIMIT
        else:
            self.expression.limit = max(min(INSTANCE_QUERY_LIMIT, self.max_retrieve_limit - self.total_retrieved), 0)

    @property
    def is_unlimited(self) -> bool:
        return self.max_retrieve_limit in {None, -1, math.inf}

    @property
    def is_finished(self) -> bool:
        return (
            (not self.is_unlimited and self.total_retrieved >= self.max_retrieve_limit)
            or self.cursor is None
            or self.last_batch_count == 0
            # Single direct relations are dependent on the parent node,
            # so we assume that the parent node is the limiting factor.
            or self.is_single_direct_relation
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r}, from={self.from_!r}, results={len(self.results)})"


class NodeQueryStep(QueryStep):
    def __init__(
        self,
        name: str,
        expression: dm.query.NodeResultSetExpression,
        result_cls: type[DomainModel],
        max_retrieve_limit: int = -1,
        select: dm.query.Select | None | type[_NotSetSentinel] = _NotSetSentinel,
    ):
        self.result_cls = result_cls
        super().__init__(name, expression, max_retrieve_limit, select)

    def _default_select(self) -> dm.query.Select:
        return dm.query.Select([dm.query.SourceSelector(self.result_cls._view_id, ["*"])])

    def unpack(self) -> dict[dm.NodeId | str, DomainModel]:
        return {
            (
                instance.as_id() if instance.space != DEFAULT_INSTANCE_SPACE else instance.external_id
            ): self.result_cls.from_instance(instance)
            for instance in cast(list[dm.Node], self.results)
        }


class EdgeQueryStep(QueryStep):
    def __init__(
        self,
        name: str,
        expression: dm.query.EdgeResultSetExpression,
        result_cls: type[DomainRelation] | None = None,
        max_retrieve_limit: int = -1,
        select: dm.query.Select | None | type[_NotSetSentinel] = _NotSetSentinel,
    ):
        self.result_cls = result_cls
        super().__init__(name, expression, max_retrieve_limit, select)

    def _default_select(self) -> dm.query.Select:
        if self.result_cls is None:
            return dm.query.Select()
        else:
            return dm.query.Select([dm.query.SourceSelector(self.result_cls._view_id, ["*"])])

    def unpack(self) -> dict[dm.NodeId, list[dm.Edge | DomainRelation]]:
        output: dict[dm.NodeId, list[dm.Edge | DomainRelation]] = defaultdict(list)
        for edge in cast(list[dm.Edge], self.results):
            edge_source = edge.start_node if self.expression.direction == "outwards" else edge.end_node
            value = self.result_cls.from_instance(edge) if self.result_cls is not None else edge
            output[as_node_id(edge_source)].append(value)  # type: ignore[arg-type]
        return output


class QueryBuilder(list, MutableSequence[QueryStep], Generic[T_DomainModelList]):
    """This is a helper class to build and execute a query. It is responsible for
    doing the paging of the query and keeping track of the results."""

    def __init__(self, result_cls: type[T_DomainModelList], steps: Collection[QueryStep] | None = None):
        super().__init__(steps or [])
        self._result_list_cls = result_cls
        self._return_step: Literal["first", "last"] = "first"

    def _reset(self):
        for expression in self:
            expression.total_retrieved = 0
            expression.cursor = None
            expression.results = []

    def _update_expression_limits(self) -> None:
        for expression in self:
            expression.update_expression_limit()

    def _build(self) -> dm.query.Query:
        with_ = {expression.name: expression.expression for expression in self}
        select = {expression.name: expression.select for expression in self if expression.select is not None}
        cursors = self._cursors

        return dm.query.Query(with_=with_, select=select, cursors=cursors)

    @property
    def _cursors(self) -> dict[str, str | None]:
        return {expression.name: expression.cursor for expression in self}

    def _update(self, batch: dm.query.QueryResult):
        for expression in self:
            if expression.name not in batch:
                continue
            expression.last_batch_count = len(batch[expression.name])
            expression.total_retrieved += expression.last_batch_count
            expression.cursor = batch.cursors.get(expression.name)
            expression.results.extend(batch[expression.name].data)

    @property
    def _is_finished(self):
        return all(expression.is_finished for expression in self)

    def _unpack(self) -> T_DomainModelList:
        selected = [step for step in self if step.select is not None]
        if len(selected) == 0:
            return self._result_list_cls([])
        elif len(selected) == 1:
            # Validated in the append method
            if self._return_step == "first":
                selected_step = cast(NodeQueryStep, self[0])
            elif self._return_step == "last":
                selected_step = cast(NodeQueryStep, self[-1])
            else:
                raise ValueError(f"Invalid return_step: {self._return_step}")
            return self._result_list_cls(selected_step.unpack().values())
        # More than one step, we need to unpack the nodes and edges
        nodes_by_from: dict[str | None, dict[dm.NodeId | str, DomainModel]] = defaultdict(dict)
        edges_by_from: dict[str, dict[dm.NodeId, list[dm.Edge | DomainRelation]]] = defaultdict(dict)
        for step in reversed(self):
            # Validated in the append method
            from_ = cast(str, step.from_)
            if isinstance(step, EdgeQueryStep):
                edges_by_from[from_].update(step.unpack())
                if step.name in nodes_by_from:
                    nodes_by_from[from_].update(nodes_by_from[step.name])
                    del nodes_by_from[step.name]
            elif isinstance(step, NodeQueryStep):
                unpacked = step.unpack()
                nodes_by_from[from_].update(unpacked)
                if step.name in nodes_by_from or step.name in edges_by_from:
                    step.result_cls._update_connections(
                        unpacked,  # type: ignore[arg-type]
                        nodes_by_from.get(step.name, {}),
                        edges_by_from.get(step.name, {}),
                    )
        if self._return_step == "first":
            return self._result_list_cls(nodes_by_from[None].values())
        elif self._return_step == "last" and self[-1].from_ in nodes_by_from:
            return self._result_list_cls(nodes_by_from[self[-1].from_].values())
        elif self._return_step == "last":
            raise ValueError("Cannot return the last step when the last step is an edge query")
        else:
            raise ValueError(f"Invalid return_step: {self._return_step}")

    def execute(self, client: CogniteClient) -> T_DomainModelList:
        self._reset()
        query = self._build()

        while True:
            self._update_expression_limits()
            query.cursors = self._cursors
            batch = client.data_modeling.instances.query(query)
            self._update(batch)
            if self._is_finished:
                break
        return self._unpack()

    def get_from(self) -> str | None:
        if len(self) == 0:
            return None
        return self[-1].name

    def create_name(self, from_: str | None) -> str:
        if from_ is None:
            return "0"
        return f"{from_}_{len(self)}"

    def append(self, __object: QueryStep, /) -> None:
        # Extra validation to ensure all assumptions are met
        if len(self) == 0:
            if __object.from_ is not None:
                raise ValueError("The first step should not have a 'from_' value")
            if not isinstance(__object, NodeQueryStep):
                raise ValueError("The first step should be a NodeQueryStep")
            # If the first step is a NodeQueryStep, and matches the instance
            # in the result_list_cls we can return the result from the first step
            if __object.result_cls is self._result_list_cls._INSTANCE:
                self._return_step = "first"
            else:
                # If not, we assume that the last step is the one we want to return
                self._return_step = "last"
        else:
            if __object.from_ is None:
                raise ValueError("The 'from_' value should be set")
        super().append(__object)

    def extend(self, __iterable: Iterable[QueryStep], /) -> None:
        for item in __iterable:
            self.append(item)

    # The implementations below are to get proper type hints
    def __iter__(self) -> Iterator[QueryStep]:
        return super().__iter__()

    @overload
    def __getitem__(self, item: SupportsIndex) -> QueryStep: ...

    @overload
    def __getitem__(self, item: slice) -> QueryBuilder[T_DomainModelList]: ...

    def __getitem__(self, item: SupportsIndex | slice) -> QueryStep | QueryBuilder[T_DomainModelList]:
        value = super().__getitem__(item)
        if isinstance(item, slice):
            return QueryBuilder(self._result_list_cls, value)  # type: ignore[arg-type]
        return cast(QueryStep, value)


T_QueryCore = TypeVar("T_QueryCore")


class Filtering(Generic[T_QueryCore], ABC):
    def __init__(self, query: T_QueryCore, prop_path: list[str] | tuple[str, ...]):
        self._query = query
        self._prop_path = prop_path
        self._filter: dm.Filter | None = None

    def _raise_if_filter_set(self):
        if self._filter is not None:
            raise ValueError("Filter has already been set")

    def _as_filter(self) -> dm.Filter | None:
        return self._filter


class StringFilter(Filtering[T_QueryCore]):
    def equals(self, value: str) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Equals(self._prop_path, value)
        return self._query

    def prefix(self, prefix: str) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Prefix(self._prop_path, prefix)
        return self._query

    def in_(self, values: list[str]) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.In(self._prop_path, values)
        return self._query


class BooleanFilter(Filtering[T_QueryCore]):
    def equals(self, value: bool) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Equals(self._prop_path, value)
        return self._query


class IntFilter(Filtering[T_QueryCore]):
    def range(self, gte: int | None, lte: int | None) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Range(self._prop_path, gte=gte, lte=lte)
        return self._query


class FloatFilter(Filtering[T_QueryCore]):
    def range(self, gte: float | None, lte: float | None) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Range(self._prop_path, gte=gte, lte=lte)
        return self._query


class TimestampFilter(Filtering[T_QueryCore]):
    def range(self, gte: datetime.datetime | None, lte: datetime.datetime | None) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Range(
            self._prop_path,
            gte=gte.isoformat(timespec="milliseconds") if gte else None,
            lte=lte.isoformat(timespec="milliseconds") if lte else None,
        )
        return self._query


class DateFilter(Filtering[T_QueryCore]):
    def range(self, gte: datetime.date | None, lte: datetime.date | None) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Range(
            self._prop_path,
            gte=gte.isoformat() if gte else None,
            lte=lte.isoformat() if lte else None,
        )
        return self._query
