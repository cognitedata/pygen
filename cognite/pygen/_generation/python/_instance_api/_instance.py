import datetime
from typing import Any, Literal, TypeVar

from pydantic import BaseModel, Field, field_validator

from ._utils import ms_to_datetime


class InstanceModel(BaseModel):
    instance_type: Literal["node", "edge"] = Field(alias="instanceType")
    space: str
    external_id: str = Field(alias="externalId")


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
    deleted_time: datetime.datetime | None = Field(None, alias="deletedTime")

    @field_validator("created_time", "last_updated_time", "deleted_time", mode="before")
    @classmethod
    def parse_ms(cls, v: Any) -> Any:
        if isinstance(v, int):
            return ms_to_datetime(v)
        return v


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

    existing_version: int | None = None


class InstanceId(InstanceModel): ...


class InstanceResult(BaseModel):
    created: list[InstanceId]
    updated: list[InstanceId]
    unchanged: list[InstanceId]
    deleted: list[InstanceId]


class Instance(InstanceModel):
    data_record: DataRecord


class InstanceWrite(InstanceModel):
    data_record: DataRecordWrite = Field(default_factory=DataRecordWrite)


T_Instance = TypeVar("T_Instance", bound=Instance)
T_InstanceWrite = TypeVar("T_InstanceWrite", bound=InstanceWrite)
