import itertools
from datetime import date, datetime
from typing import Annotated, Any, ClassVar, Generic, Literal, TypeVar

from pydantic import BaseModel, BeforeValidator, Field, model_validator
from pydantic.functional_serializers import PlainSerializer

from ._utils import datetime_to_ms, ms_to_datetime

DateTimeMS = Annotated[
    datetime,
    BeforeValidator(ms_to_datetime, json_schema_input_type=int),
    PlainSerializer(datetime_to_ms, return_type=int, when_used="always"),
]
DateTime = Annotated[
    datetime, PlainSerializer(lambda d: d.isoformat(timespec="milliseconds"), return_type=str, when_used="always")
]
Date = Annotated[date, PlainSerializer(lambda d: d.isoformat(), return_type=str, when_used="always")]


class ViewRef(BaseModel, populate_by_name=True):
    """Reference to a view.

    Args:
        space: The space of the view.
        external_id: The external ID of the view.
    """

    space: str
    external_id: str = Field(alias="externalId")
    version: str


class DataRecord(BaseModel, populate_by_name=True):
    """The data record represents the metadata of a node.

    Args:
        created_time: The created time of the node.
        last_updated_time: The last updated time of the node.
        deleted_time: If present, the deleted time of the node.
        version: The version of the node.
    """

    version: int
    last_updated_time: DateTimeMS = Field(alias="lastUpdatedTime")
    created_time: DateTimeMS = Field(alias="createdTime")
    deleted_time: DateTimeMS | None = Field(None, alias="deletedTime")


_DATA_RECORD_FIELDS = frozenset(field_.alias or field_id for field_id, field_ in DataRecord.model_fields.items())


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

    existing_version: int | None = Field(None, alias="existingVersion")


_DATA_RECORD_WRITE_FIELDS = frozenset(
    field_.alias or field_id for field_id, field_ in DataRecordWrite.model_fields.items()
)


class InstanceModel(BaseModel):
    _view_id: ClassVar[ViewRef]
    instance_type: Literal["node", "edge"] = Field(alias="instanceType")
    space: str
    external_id: str = Field(alias="externalId")

    def dump(self, camel_case: bool = True, format: Literal["model", "instance"] = "model") -> dict[str, Any]:
        """Dump the model to a dictionary.

        Args:
            camel_case: Whether to use camel case for the keys. Defaults to True.


        Returns:
            The dictionary representation of the model.
        """
        if format == "model":
            return self.model_dump(by_alias=camel_case)
        elif format == "instance":
            data = self.model_dump(by_alias=camel_case)
            data_record = data.pop("data_record", {})
            property_values: dict[str, Any] = {}
            for key in list(data.keys()):
                if key not in InstanceModel.model_fields and key not in _INSTANCE_MODEL_ALIASES:
                    property_values[key] = data.pop(key)
            data["properties"] = {
                self._view_id.space: {f"{self._view_id.external_id}/{self._view_id.version}": property_values}
            }
            data.update(data_record)
            return data
        else:
            raise ValueError(f"Unknown format: {format}")

    @model_validator(mode="before")
    def reshape_structure(cls, data: dict[str, Any]) -> dict[str, Any]:
        data = data.copy()
        data.pop("instanceType", None)
        record_data = {
            field_id: data.pop(field_id)
            for field_id in itertools.chain(_DATA_RECORD_FIELDS, _DATA_RECORD_WRITE_FIELDS)
            if field_id in data
        }
        if record_data:
            data["data_record"] = record_data
        if "properties" in data:
            properties = data.pop("properties")
            if isinstance(properties, dict):
                view_data = next(iter(properties.values()))
                if isinstance(view_data, dict):
                    data_values = next(iter(view_data.values()))
                    if isinstance(data_values, dict):
                        data.update(data_values)
        return data


_INSTANCE_MODEL_ALIASES = frozenset(
    {field_.alias for field_ in InstanceModel.model_fields.values() if field_.alias is not None}
)


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


class Page(BaseModel, Generic[T_Instance], populate_by_name=True):
    """A page of results from a paginated API response.

    Attributes:
        items: The list of items in this page.
        next_cursor: The cursor for the next page, or None if this is the last page.
    """

    items: list[T_Instance]
    next_cursor: str | None = Field(default=None, alias="nextCursor")
