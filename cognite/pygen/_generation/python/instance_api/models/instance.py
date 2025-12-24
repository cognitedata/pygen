import itertools
from collections.abc import Collection
from typing import Any, ClassVar, Literal, TypeVar, get_args

import pandas as pd
from pydantic import BaseModel, Field, GetCoreSchemaHandler, model_validator
from pydantic_core import CoreSchema, core_schema

from cognite.pygen._generation.python.instance_api.models._types import DateTimeMS

from ._references import ViewReference


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


class InstanceModel(BaseModel, populate_by_name=True):
    _view_id: ClassVar[ViewReference]
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
                try:
                    view_data = next(iter(properties.values()))
                    if isinstance(view_data, dict):
                        data_values = next(iter(view_data.values()))
                        if isinstance(data_values, dict):
                            data.update(data_values)
                except StopIteration:
                    # This can happen if an instance has no properties, which is valid.
                    pass
        return data


_INSTANCE_MODEL_ALIASES = frozenset(
    {field_.alias for field_ in InstanceModel.model_fields.values() if field_.alias is not None}
)


class Instance(InstanceModel):
    data_record: DataRecord


class InstanceWrite(InstanceModel):
    data_record: DataRecordWrite = Field(default_factory=DataRecordWrite)


T_Instance = TypeVar("T_Instance", bound=Instance)
T_InstanceWrite = TypeVar("T_InstanceWrite", bound=InstanceWrite)


class InstanceList(Collection[T_Instance]):
    """A list of instances with pandas integration.

    This class wraps a list of instances and provides convenient methods
    for conversion to pandas DataFrames and dumping to dictionaries.
    """

    _INSTANCE: ClassVar[type[Instance]] = Instance

    def __init__(self, collection: Collection[T_Instance] | None = None) -> None:
        self.data: list[T_Instance] = list(collection or [])

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)

    def __contains__(self, item):
        return item in self.data

    def __getitem__(self, index):
        return self.data[index]

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
        # Get the type argument (e.g., T_Instance) from the generic alias
        args = get_args(source_type)
        if args:
            item_type = args[0]
        else:
            item_type = Instance

        # Get the schema for the item type
        item_schema = handler.generate_schema(item_type)

        # Create a schema that validates a list of items and wraps it in InstanceList
        return core_schema.no_info_after_validator_function(
            cls,
            core_schema.list_schema(item_schema),
        )

    def dump(self, camel_case: bool = True, format: Literal["model", "instance"] = "model") -> list[dict[str, Any]]:
        """
        Dump the list of nodes to a list of dictionaries.
        Args:
            camel_case: Whether to use camel case for the keys. Defaults to True.
            format: The format of the dump. Can be "model" or "instance". Defaults
                to "model".
        Returns:
            A list of dictionaries representing the nodes.
        """
        return [node.dump(camel_case=camel_case, format=format) for node in self.data]

    def to_pandas(self, dropna_columns: bool = False) -> pd.DataFrame:
        """
        Convert the list of nodes to a pandas.DataFrame.

        Args:
            dropna_columns: Whether to drop columns that are all NaN.

        Returns:
            A pandas.DataFrame with the nodes as rows.
        """
        df = pd.DataFrame(self.dump(camel_case=False))
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


T_InstanceList = TypeVar("T_InstanceList", bound=InstanceList)


class InstanceId(BaseModel, populate_by_name=True):
    """Identifier for an instance (node or edge)."""

    instance_type: Literal["node", "edge"] = Field(alias="instanceType")
    space: str
    external_id: str = Field(alias="externalId")

    def dump(self, camel_case: bool = True, format: Literal["model", "instance"] = "model") -> dict[str, Any]:
        """Dump the model to a dictionary.

        Args:
            camel_case: Whether to use camel case for the keys. Defaults to True.
            format: The format of the dump (unused for InstanceId, kept for compatibility).

        Returns:
            The dictionary representation of the model.
        """
        return self.model_dump(by_alias=camel_case)
