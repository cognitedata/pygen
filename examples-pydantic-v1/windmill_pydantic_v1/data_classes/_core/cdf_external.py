from __future__ import annotations

import datetime
from typing import (
    Annotated,
    Optional,
    Any,
    no_type_check,
)

from cognite.client import data_modeling as dm
from cognite.client.data_classes import Datapoints, SequenceColumnWrite, SequenceColumn
from cognite.client.data_classes import (
    TimeSeries as CogniteTimeSeries,
    Sequence as CogniteSequence,
    FileMetadata as CogniteFileMetadata,
    TimeSeriesWrite as CogniteTimeSeriesWrite,
    SequenceWrite as CogniteSequenceWrite,
    FileMetadataWrite as CogniteFileMetadataWrite,
)
from cognite.client.utils import datetime_to_ms
from pydantic import BaseModel, Extra, root_validator, validator


def to_camel(string: str) -> str:
    return "".join(word.capitalize() if no > 0 else word.casefold() for no, word in enumerate(string.split("_")))


class GraphQLExternal(BaseModel):
    class Config:
        arbitrary_types_allowed = True
        allow_population_by_field_name = True
        alias_generator = to_camel


class TimeSeriesGraphQL(GraphQLExternal):
    id: Optional[int] = None
    external_id: Optional[str] = None
    instance_id: Optional[dm.NodeId] = None
    name: Optional[str] = None
    is_string: Optional[bool] = None
    metadata: Optional[dict[str, str]] = None
    unit: Optional[str] = None
    unit_external_id: Optional[str] = None
    asset_id: Optional[int] = None
    is_step: Optional[bool] = None
    description: Optional[str] = None
    security_categories: Optional[list[int]] = None
    data_set_id: Optional[int] = None
    created_time: Optional[int] = None
    last_updated_time: Optional[int] = None
    data: Optional[Datapoints] = None

    @root_validator(pre=True, allow_reuse=True)
    def parse_datapoints(cls, data: Any) -> Any:
        if isinstance(data, dict) and "getDataPoints" in data:
            datapoints = data.pop("getDataPoints")
            if "items" in datapoints:
                for item in datapoints["items"]:
                    # The Datapoints expects the timestamp to be in milliseconds
                    item["timestamp"] = datetime_to_ms(
                        datetime.datetime.fromisoformat(item["timestamp"].replace("Z", "+00:00"))
                    )
                data["datapoints"] = datapoints["items"]
                data["data"] = Datapoints.load(data)
        return data

    def as_write(self) -> CogniteTimeSeriesWrite:
        return CogniteTimeSeriesWrite(
            external_id=self.external_id,
            name=self.name,
            is_string=self.is_string,
            metadata=self.metadata,
            unit=self.unit,
            unit_external_id=self.unit_external_id,
            asset_id=self.asset_id,
            is_step=self.is_step,
            description=self.description,
        )

    def as_read(self) -> CogniteTimeSeries:
        return CogniteTimeSeries(
            id=self.id,
            external_id=self.external_id,
            instance_id=self.instance_id,
            name=self.name,
            is_string=self.is_string,
            metadata=self.metadata,
            unit=self.unit,
            unit_external_id=self.unit_external_id,
            asset_id=self.asset_id,
            is_step=self.is_step,
            description=self.description,
            security_categories=self.security_categories,
        )


class FileMetadataGraphQL(GraphQLExternal):
    external_id: Optional[str] = None
    name: Optional[str] = None
    source: Optional[str] = None
    mime_type: Optional[str] = None
    metadata: Optional[dict[str, str]] = None
    directory: Optional[str] = None
    asset_ids: Optional[list[int]] = None
    data_set_id: Optional[int] = None
    labels: Optional[list[dict]] = None
    geo_location: Optional[dict] = None
    source_created_time: Optional[int] = None
    source_modified_time: Optional[int] = None
    security_categories: Optional[list[int]] = None
    id: Optional[int] = None
    uploaded: Optional[bool] = None
    uploaded_time: Optional[int] = None
    created_time: Optional[int] = None
    last_updated_time: Optional[int] = None

    @no_type_check
    def as_write(self) -> CogniteFileMetadataWrite:
        return CogniteFileMetadataWrite(
            external_id=self.external_id,
            name=self.name,
            source=self.source,
            mime_type=self.mime_type,
            metadata=self.metadata,
            directory=self.directory,
            asset_ids=self.asset_ids,
            data_set_id=self.data_set_id,
            labels=self.labels,
            geo_location=self.geo_location,
            source_created_time=self.source_created_time,
            source_modified_time=self.source_modified_time,
            security_categories=self.security_categories,
        )

    @no_type_check
    def as_read(self) -> CogniteFileMetadata:
        return CogniteFileMetadata(
            external_id=self.external_id,
            name=self.name,
            source=self.source,
            mime_type=self.mime_type,
            metadata=self.metadata,
            directory=self.directory,
            asset_ids=self.asset_ids,
            data_set_id=self.data_set_id,
            labels=self.labels,
            geo_location=self.geo_location,
            source_created_time=self.source_created_time,
            source_modified_time=self.source_modified_time,
            security_categories=self.security_categories,
            id=self.id,
            uploaded=self.uploaded,
            uploaded_time=self.uploaded_time,
            created_time=self.created_time,
            last_updated_time=self.last_updated_time,
        )


class SequenceColumnGraphQL(GraphQLExternal):
    external_id: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    value_type: Optional[str] = None
    metadata: Optional[dict[str, str]] = None
    created_time: Optional[int] = None
    last_updated_time: Optional[int] = None

    @validator("value_type", pre=True, allow_reuse=True)
    def title_value_type(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.title()
        return value

    @no_type_check
    def as_write(self) -> SequenceColumnWrite:
        if self.value_type is None:
            raise ValueError("value_type is required")
        return SequenceColumnWrite(
            external_id=self.external_id,
            name=self.name,
            description=self.description,
            value_type=self.value_type,
            metadata=self.metadata,
        )

    @no_type_check
    def as_read(self) -> SequenceColumn:
        if self.value_type is None:
            raise ValueError("value_type is required")
        return SequenceColumn(
            external_id=self.external_id,
            name=self.name,
            description=self.description,
            value_type=self.value_type,
            metadata=self.metadata,
            created_time=self.created_time,
            last_updated_time=self.last_updated_time,
        )


class SequenceGraphQL(GraphQLExternal):
    id: Optional[int] = None
    name: Optional[str] = None
    description: Optional[str] = None
    asset_id: Optional[int] = None
    external_id: Optional[str] = None
    metadata: Optional[dict[str, str]] = None
    columns: Optional[list[SequenceColumnGraphQL]] = None
    created_time: Optional[int] = None
    last_updated_time: Optional[int] = None
    data_set_id: Optional[int] = None

    @no_type_check
    def as_write(self) -> CogniteSequenceWrite:
        return CogniteSequenceWrite(
            name=self.name,
            description=self.description,
            asset_id=self.asset_id,
            external_id=self.external_id,
            metadata=self.metadata,
            columns=[col.as_write() for col in self.columns or []] if self.columns else None,
            data_set_id=self.data_set_id,
        )

    @no_type_check
    def as_read(self) -> CogniteSequence:
        return CogniteSequence(
            id=self.id,
            name=self.name,
            description=self.description,
            asset_id=self.asset_id,
            external_id=self.external_id,
            metadata=self.metadata,
            columns=[col.as_read() for col in self.columns or []] if self.columns else None,
            created_time=self.created_time,
            last_updated_time=self.last_updated_time,
            data_set_id=self.data_set_id,
        )
