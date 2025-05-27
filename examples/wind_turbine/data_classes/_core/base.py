from __future__ import annotations

import datetime
import sys
import warnings
from abc import ABC, abstractmethod
from collections import UserList
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from typing import (
    Callable,
    cast,
    ClassVar,
    Generic,
    Literal,
    Optional,
    Any,
    Iterator,
    TypeVar,
    overload,
    Union,
    SupportsIndex,
)

import pandas as pd
from cognite.client import data_modeling as dm
from cognite.client.data_classes import (
    TimeSeriesWrite,
    TimeSeriesWriteList,
    FileMetadataWrite,
    FileMetadataWriteList,
    SequenceWrite,
    SequenceWriteList,
    TimeSeriesList,
    FileMetadataList,
    SequenceList,
)
from cognite.client.data_classes.data_modeling.instances import (
    Instance,
    InstanceApply,
    Properties,
    PropertyValue,
)
from cognite.client.utils import ms_to_datetime
from pydantic import BaseModel, Field, field_validator, model_validator

from wind_turbine.data_classes._core.constants import DEFAULT_INSTANCE_SPACE
from wind_turbine.data_classes._core.cdf_external import GraphQLExternal
from wind_turbine.data_classes._core.helpers import as_direct_relation_reference, parse_single_connection
from wind_turbine.config import global_config

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


@dataclass
class ResourcesWrite:
    nodes: dm.NodeApplyList = field(default_factory=lambda: dm.NodeApplyList([]))
    edges: dm.EdgeApplyList = field(default_factory=lambda: dm.EdgeApplyList([]))
    time_series: TimeSeriesWriteList = field(default_factory=lambda: TimeSeriesWriteList([]))
    files: FileMetadataWriteList = field(default_factory=lambda: FileMetadataWriteList([]))
    sequences: SequenceWriteList = field(default_factory=lambda: SequenceWriteList([]))

    def extend(self, other: ResourcesWrite) -> None:
        self.nodes.extend(other.nodes)
        self.edges.extend(other.edges)
        self.time_series.extend(other.time_series)
        self.files.extend(other.files)
        self.sequences.extend(other.sequences)


@dataclass
class ResourcesWriteResult:
    nodes: dm.NodeApplyResultList = field(default_factory=lambda: dm.NodeApplyResultList([]))
    edges: dm.EdgeApplyResultList = field(default_factory=lambda: dm.EdgeApplyResultList([]))
    time_series: TimeSeriesList = field(default_factory=lambda: TimeSeriesList([]))
    files: FileMetadataList = field(default_factory=lambda: FileMetadataList([]))
    sequences: SequenceList = field(default_factory=lambda: SequenceList([]))


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

    def to_pandas(self, dropna_columns: bool = False) -> pd.DataFrame:
        """
        Convert the list of nodes to a pandas.DataFrame.

        Args:
            dropna_columns: Whether to drop columns that are all NaN.

        Returns:
            A pandas.DataFrame with the nodes as rows.
        """
        df = pd.DataFrame(self.dump())
        if df.empty:
            df = pd.DataFrame(columns=list(GraphQLCore.model_fields.keys()))
        # Reorder columns to have the most relevant first
        id_columns = ["space", "external_id"]
        end_columns = ["data_record"]
        fixed_columns = set(id_columns + end_columns)
        columns = (
            id_columns + [col for col in df if col not in fixed_columns] + [col for col in end_columns if col in df]
        )
        df = df[columns]
        if df.empty:
            return df
        if dropna_columns:
            df.dropna(how="all", axis=1, inplace=True)
        return df

    def _repr_html_(self) -> str:
        return self.to_pandas(dropna_columns=True)._repr_html_()  # type: ignore[operator]


class DomainModelCore(Core, ABC):
    _view_id: ClassVar[dm.ViewId]

    space: str
    external_id: str = Field(min_length=1, max_length=255, alias="externalId")

    def as_tuple_id(self) -> tuple[str, str]:
        return self.space, self.external_id

    def as_direct_reference(self) -> dm.DirectRelationReference:
        return dm.DirectRelationReference(space=self.space, external_id=self.external_id)


T_DomainModelCore = TypeVar("T_DomainModelCore", bound=DomainModelCore)


class DataRecord(BaseModel, populate_by_name=True):
    """The data record represents the metadata of a node.

    Args:
        created_time: The created time of the node.
        last_updated_time: The last updated time of the node.
        deleted_time: If present, the deleted time of the node.
        version: The version of the node.
    """

    version: int
    last_updated_time: datetime.datetime = Field(alias="lastUpdatedTime")
    created_time: datetime.datetime = Field(alias="createdTime")
    deleted_time: Optional[datetime.datetime] = Field(None, alias="deletedTime")

    @field_validator("created_time", "last_updated_time", "deleted_time", mode="before")
    @classmethod
    def parse_ms(cls, v: Any) -> Any:
        if isinstance(v, int):
            return ms_to_datetime(v)
        return v


class DomainModel(DomainModelCore, ABC, extra="allow"):
    data_record: DataRecord
    node_type: Optional[dm.DirectRelationReference] = None

    @field_validator("node_type", mode="before")
    @classmethod
    def parse_type(cls, value: Any) -> Any:
        return as_direct_relation_reference(value)

    def as_id(self) -> dm.NodeId:
        return dm.NodeId(space=self.space, external_id=self.external_id)

    @classmethod
    def _to_dict(cls, instance: Instance) -> dict[str, Any]:
        data = instance.dump(camel_case=False)
        node_type = data.pop("type", None)
        space = data.pop("space")
        external_id = data.pop("external_id")
        return dict(
            space=space,
            external_id=external_id,
            data_record=DataRecord(**data),
            node_type=node_type,
            **unpack_properties(instance.properties),
        )

    @classmethod
    def from_instance(cls, instance: Instance) -> Self:
        return parse_pydantic(cls, cls._to_dict(instance))


T_DomainModel = TypeVar("T_DomainModel", bound=DomainModel)


class DataRecordWrite(BaseModel):
    """The data record represents the metadata of a node.

    Args:
        existing_version: Fail the ingestion request if the node version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge
            (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the
            item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing
            the ingestion request.
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
            df = pd.DataFrame(columns=list(self._INSTANCE.model_fields.keys()))
        return df

    def _repr_html_(self) -> str:
        return self.to_pandas()._repr_html_()  # type: ignore[operator]


class DataRecordList(_DataRecordListCore[DataRecord]):
    _INSTANCE = DataRecord


class DataRecordWriteList(_DataRecordListCore[DataRecordWrite]):
    _INSTANCE = DataRecordWrite


class DomainModelWrite(DomainModelCore, extra="ignore", populate_by_name=True):
    _container_fields: ClassVar[tuple[str, ...]] = tuple()
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = tuple()
    _inwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = tuple()
    _direct_relations: ClassVar[tuple[str, ...]] = tuple()
    external_id_factory: ClassVar[Optional[Callable[[type[DomainModelWrite], dict], str]]] = None
    data_record: DataRecordWrite = Field(default_factory=DataRecordWrite)
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None

    def as_id(self) -> dm.NodeId:
        return dm.NodeId(space=self.space, external_id=self.external_id)

    def to_instances_write(
        self,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        return self._to_resources_write(set(), allow_version_increase)

    def _to_resources_write(self, cache: set[tuple[str, str]], allow_version_increase: bool = False) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources
        properties = serialize_properties(self, resources)

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=None if allow_version_increase else self.data_record.existing_version,
            type=as_direct_relation_reference(self.node_type),
            sources=[dm.NodeOrEdgeData(source=self._view_id, properties=properties)] if properties else None,
        )
        resources.nodes.append(this_node)
        cache.add(self.as_tuple_id())

        resources.extend(connection_resources(self, cache, allow_version_increase))

        return resources

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
    def _to_dict(cls, instance: InstanceApply) -> dict[str, Any]:
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
        return dict(
            space=space, external_id=external_id, node_type=node_type, data_record=DataRecordWrite(**data), **properties
        )

    @classmethod
    def from_instance(cls: type[T_DomainModelWrite], instance: InstanceApply) -> T_DomainModelWrite:
        return parse_pydantic(cls, cls._to_dict(instance))


T_DomainModelWrite = TypeVar("T_DomainModelWrite", bound=DomainModelWrite)


class CoreList(UserList, Generic[T_Core]):
    _INSTANCE: type[T_Core]
    _PARENT_CLASS: type[Core]

    def __init__(self, nodes: Collection[T_Core] | None = None) -> None:
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

    def to_pandas(self, dropna_columns: bool = False) -> pd.DataFrame:
        """
        Convert the list of nodes to a pandas.DataFrame.

        Args:
            dropna_columns: Whether to drop columns that are all NaN.

        Returns:
            A pandas.DataFrame with the nodes as rows.
        """
        df = pd.DataFrame(self.dump())
        if df.empty:
            df = pd.DataFrame(columns=list(self._INSTANCE.model_fields.keys()))
        # Reorder columns to have the most relevant first
        id_columns = ["space", "external_id"]
        end_columns = ["node_type", "data_record"]
        fixed_columns = set(id_columns + end_columns)
        columns = (
            id_columns + [col for col in df if col not in fixed_columns] + [col for col in end_columns if col in df]
        )
        df = df[columns]
        if df.empty:
            return df
        if dropna_columns:
            df.dropna(how="all", axis=1, inplace=True)
        return df

    def _repr_html_(self) -> str:
        return self.to_pandas(dropna_columns=True)._repr_html_()  # type: ignore[operator]


class DomainModelList(CoreList[T_DomainModel]):
    _PARENT_CLASS = DomainModel

    def __init__(
        self, nodes: Collection[T_DomainModel] | None = None, cursors: dict[str, str | None] | None = None
    ) -> None:
        super().__init__(nodes)
        self.cursors = cursors

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
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        cache: set[tuple[str, str]] = set()
        domains = ResourcesWrite()
        for node in self:
            result = node._to_resources_write(cache, allow_version_increase)
            domains.extend(result)
        return domains


T_DomainModelWriteList = TypeVar("T_DomainModelWriteList", bound=DomainModelWriteList, covariant=True)


class DomainRelation(DomainModelCore, extra="allow"):
    edge_type: dm.DirectRelationReference
    start_node: dm.DirectRelationReference = Field(alias="startNode")
    end_node: Any = Field(alias="endNode")
    data_record: DataRecord

    @field_validator("start_node", "edge_type", mode="before")
    @classmethod
    def parse_direct_relation(cls, value: Any) -> Any:
        return as_direct_relation_reference(value)

    @field_validator("end_node", mode="before")
    @classmethod
    def parse_single_connection(cls, value: Any) -> Any:
        return parse_single_connection(value, "end_node")

    def as_id(self) -> dm.EdgeId:
        return dm.EdgeId(space=self.space, external_id=self.external_id)

    @classmethod
    def _to_dict(cls, instance: Instance) -> dict[str, Any]:
        data = instance.dump(camel_case=False)
        data.pop("instance_type", None)
        edge_type = data.pop("type", None)
        start_node = data.pop("start_node")
        end_node = data.pop("end_node")
        space = data.pop("space")
        external_id = data.pop("external_id")
        return dict(
            space=space,
            external_id=external_id,
            data_record=DataRecord(**data),
            edge_type=edge_type,
            start_node=start_node,
            end_node=end_node,
            **unpack_properties(instance.properties),
        )

    @classmethod
    def from_instance(cls, instance: Instance) -> Self:
        return parse_pydantic(cls, cls._to_dict(instance))


T_DomainRelation = TypeVar("T_DomainRelation", bound=DomainRelation)


def default_edge_external_id_factory(
    start_node: DomainModelWrite | str | dm.NodeId,
    end_node: DomainModelWrite | str | dm.NodeId,
    edge_type: dm.DirectRelationReference,
) -> str:
    start = start_node if isinstance(start_node, str) else start_node.external_id
    end = end_node if isinstance(end_node, str) else end_node.external_id
    return f"{start}:{end}"


class DomainRelationWrite(Core, extra="forbid"):
    _view_id: ClassVar[dm.ViewId]

    _container_fields: ClassVar[tuple[str, ...]] = tuple()
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = tuple()
    _inwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = tuple()
    _direct_relations: ClassVar[tuple[str, ...]] = tuple()
    _validate_other_node: ClassVar[Callable | None] = None

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
    space: str = DEFAULT_INSTANCE_SPACE
    data_record: DataRecordWrite = Field(default_factory=DataRecordWrite)
    external_id: Optional[str] = Field(None, min_length=1, max_length=255)
    end_node: Any

    def _to_resources_write(
        self,
        cache: set[tuple[str, str]],
        other_node: DomainModelWrite,
        edge_type: dm.DirectRelationReference,
        direction: Literal["outwards", "inwards"],
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.external_id and (self.space, self.external_id) in cache:
            return resources

        if self._validate_other_node:
            self._validate_other_node(other_node, self.end_node)

        start_node = other_node
        end_node = self.end_node
        if direction == "inwards":
            start_node, end_node = end_node, start_node

        external_id = self.external_id or DomainRelationWrite.external_id_factory(start_node, end_node, edge_type)
        properties = serialize_properties(self, resources)
        this_edge = dm.EdgeApply(
            space=self.space,
            external_id=external_id,
            type=edge_type,
            start_node=as_direct_relation_reference(start_node, return_none=False),
            end_node=as_direct_relation_reference(end_node, return_none=False),
            existing_version=None if allow_version_increase else self.data_record.existing_version,
            sources=[dm.NodeOrEdgeData(source=self._view_id, properties=properties)] if properties else None,
        )
        resources.edges.append(this_edge)
        cache.add((self.space, external_id))

        if isinstance(self.end_node, DomainModelWrite):
            other_resources = self.end_node._to_resources_write(cache, allow_version_increase)
            resources.extend(other_resources)

        resources.extend(connection_resources(self, cache, allow_version_increase))
        return resources

    @classmethod
    def create_edge(
        cls,
        start_node: DomainModelWrite | str | dm.NodeId,
        end_node: DomainModelWrite | str | dm.NodeId,
        edge_type: dm.DirectRelationReference,
    ) -> dm.EdgeApply:
        if isinstance(start_node, DomainModelWrite | dm.NodeId):
            space = start_node.space
        elif isinstance(end_node, DomainModelWrite | dm.NodeId):
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
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        edge = DomainRelationWrite.create_edge(start_node, end_node, edge_type)
        if (edge.space, edge.external_id) in cache:
            return resources
        resources.edges.append(edge)
        cache.add((edge.space, edge.external_id))

        if isinstance(end_node, DomainModelWrite):
            other_resources = end_node._to_resources_write(
                cache,
                allow_version_increase,
            )
            resources.extend(other_resources)
        if isinstance(start_node, DomainModelWrite):
            other_resources = start_node._to_resources_write(
                cache,
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
                values: list[Any] = []
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


def serialize_properties(model: DomainModelWrite | DomainRelationWrite, resources: ResourcesWrite) -> dict[str, Any]:
    properties: dict[str, Any] = {}
    model_fields = type(model).model_fields
    for field_name in model._container_fields:
        if field_name in model.model_fields_set:
            value = getattr(model, field_name)
            key = model_fields[field_name].alias or field_name
            if field_name in model._direct_relations:
                properties[key] = serialize_relation(value, model.space)
            else:
                properties[key] = serialize_property(value)

            values = value if isinstance(value, Sequence) else [value]
            for item in values:
                if isinstance(item, FileMetadataWrite):
                    resources.files.append(item)
                elif isinstance(item, TimeSeriesWrite):
                    resources.time_series.append(item)
                elif isinstance(item, SequenceWrite):
                    resources.sequences.append(item)

    return properties


def connection_resources(
    model: DomainModelWrite | DomainRelationWrite, cache: set[tuple[str, str]], allow_version_increase: bool = False
) -> ResourcesWrite:
    resources = ResourcesWrite()
    for field_name in model._direct_relations:
        if field_name not in model.model_fields_set:
            continue
        value = getattr(model, field_name)
        values = value if isinstance(value, Sequence) else [value]
        for item in values:
            if isinstance(item, DomainModelWrite):
                other_resources = item._to_resources_write(cache, allow_version_increase)
                resources.extend(other_resources)

    for field_name, edge_type in model._outwards_edges:
        value = getattr(model, field_name)
        if value is None or field_name not in model.model_fields_set:
            continue
        values = value if isinstance(value, Sequence) else [value]
        for item in values:
            if isinstance(item, DomainRelationWrite):
                other_resources = item._to_resources_write(
                    cache, model, edge_type, "outwards", allow_version_increase  # type: ignore[arg-type]
                )
            else:
                other_resources = DomainRelationWrite.from_edge_to_resources(
                    cache,
                    start_node=model,  # type: ignore[arg-type]
                    end_node=item,  # type: ignore[arg-type]
                    edge_type=edge_type,
                    allow_version_increase=allow_version_increase,
                )
            resources.extend(other_resources)

    for field_name, edge_type in model._inwards_edges:
        value = getattr(model, field_name)
        if value is None or field_name not in model.model_fields_set:
            continue
        values = value if isinstance(value, Sequence) else [value]
        for item in values:
            if isinstance(item, DomainRelationWrite):
                other_resources = item._to_resources_write(
                    cache, model, edge_type, "inwards", allow_version_increase  # type: ignore[arg-type]
                )
            else:
                other_resources = DomainRelationWrite.from_edge_to_resources(
                    cache,
                    start_node=item,  # type: ignore[arg-type]
                    end_node=model,  # type: ignore[arg-type]
                    edge_type=edge_type,
                    allow_version_increase=allow_version_increase,
                )
            resources.extend(other_resources)
    return resources


def serialize_property(value: Any) -> Any:
    if isinstance(value, Sequence) and not isinstance(value, str):
        return [serialize_property(item) for item in value]
    elif isinstance(value, datetime.datetime):
        return value.isoformat(timespec="milliseconds")
    elif isinstance(value, datetime.date):
        return value.isoformat()
    elif isinstance(value, TimeSeriesWrite | FileMetadataWrite | SequenceWrite):
        return value.external_id
    return value


def serialize_relation(
    value: DomainModelWrite | str | dm.NodeId | None | Sequence[DomainModelWrite | str | dm.NodeId], default_space: str
) -> Any:
    if value is None:
        return None
    elif isinstance(value, str):
        return {"space": default_space, "externalId": value}
    elif isinstance(value, Sequence):
        return [serialize_relation(item, default_space) for item in value]
    elif isinstance(value, DomainModelWrite | dm.NodeId):
        return {"space": value.space, "externalId": value.external_id}
    raise TypeError(f"Expected str, subclass of {DomainModelWrite.__name__} or NodeId, got {type(value)}")


T_DomainList = TypeVar("T_DomainList", bound=Union[DomainModelList, DomainRelationList], covariant=True)


def as_read_args(model: GraphQLCore | GraphQLExternal) -> dict[str, Any]:
    output: dict[str, Any] = {}
    model_fields = type(model).model_fields
    for field_name in model.model_fields_set:
        value = getattr(model, field_name)
        key = model_fields[field_name].alias or field_name
        if field_name == "data_record" and isinstance(model, GraphQLCore):
            # Dict to postpone validation
            if model.data_record is None:
                output[field_name] = None
            else:
                output[field_name] = dict(
                    version=0,
                    last_updated_time=model.data_record.last_updated_time,
                    created_time=model.data_record.created_time,
                )
        else:
            output[key] = as_read_value(value)
    return output


def as_read_value(value: Any) -> Any:
    if isinstance(value, GraphQLCore | GraphQLExternal):
        return as_read_args(value)
    elif isinstance(value, Sequence) and not isinstance(value, str):
        return [as_read_value(item) for item in value]
    return value


def as_write_args(model: DomainModel | GraphQLCore | DomainRelation | GraphQLExternal) -> dict[str, Any]:
    output: dict[str, Any] = {}
    model_fields = type(model).model_fields
    for field_name in model.model_fields_set:
        if field_name not in model_fields:
            # The field is an extra field. Typically, happens when a property has been added
            # to the model after the SDK was generated. Extra fields are not allowed in the write
            # format of the data class.
            continue
        value = getattr(model, field_name)
        key = model_fields[field_name].alias or field_name
        if field_name == "data_record" and isinstance(model, DomainModel | DomainRelation):
            output[field_name] = DataRecordWrite(existing_version=model.data_record.version)
        elif field_name == "data_record" and isinstance(model, GraphQLCore):
            output[field_name] = DataRecordWrite(existing_version=0)
        else:
            output[key] = as_write_value(value)
    return output


def as_write_value(value: Any) -> Any:
    if isinstance(value, DomainModel | GraphQLCore | GraphQLExternal):
        return as_write_args(value)
    elif isinstance(value, Sequence) and not isinstance(value, str):
        return [as_write_value(item) for item in value]
    return value


def parse_pydantic(cls: type[T_Core], data: dict[str, Any]) -> T_Core:
    if global_config.validate_retrieve:
        return cls.model_validate(data)
    else:
        return cls.model_construct(**data)  # type: ignore[return-value]
