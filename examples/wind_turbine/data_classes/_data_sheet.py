from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

from wind_turbine.config import global_config
from wind_turbine.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    FileContentAPI,
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
    BooleanFilter,
    TimestampFilter,
)


__all__ = [
    "DataSheet",
    "DataSheetWrite",
    "DataSheetList",
    "DataSheetWriteList",
    "DataSheetFields",
    "DataSheetTextFields",
    "DataSheetGraphQL",
]


DataSheetTextFields = Literal["external_id", "description", "directory", "mime_type", "name"]
DataSheetFields = Literal[
    "external_id", "description", "directory", "is_uploaded", "mime_type", "name", "uploaded_time"
]

_DATASHEET_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "description": "description",
    "directory": "directory",
    "is_uploaded": "isUploaded",
    "mime_type": "mimeType",
    "name": "name",
    "uploaded_time": "uploadedTime",
}


class DataSheetGraphQL(GraphQLCore):
    """This represents the reading version of data sheet, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the data sheet.
        data_record: The data record of the data sheet node.
        description: Description of the instance
        directory: Contains the path elements from the source (if the source system has a file system hierarchy or
            similar.)
        is_uploaded: Specifies if the file content has been uploaded to Cognite Data Fusion or not.
        mime_type: The MIME type of the file.
        name: Name of the instance
        uploaded_time: The time the file upload completed.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "DataSheet", "1")
    description: Optional[str] = None
    directory: Optional[str] = None
    is_uploaded: Optional[bool] = Field(None, alias="isUploaded")
    mime_type: Optional[str] = Field(None, alias="mimeType")
    name: Optional[str] = None
    uploaded_time: Optional[datetime.datetime] = Field(None, alias="uploadedTime")

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

    def as_read(self) -> DataSheet:
        """Convert this GraphQL format of data sheet to the reading format."""
        return DataSheet.model_validate(as_read_args(self))

    def as_write(self) -> DataSheetWrite:
        """Convert this GraphQL format of data sheet to the writing format."""
        return DataSheetWrite.model_validate(as_write_args(self))


class DataSheet(DomainModel):
    """This represents the reading version of data sheet.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the data sheet.
        data_record: The data record of the data sheet node.
        description: Description of the instance
        directory: Contains the path elements from the source (if the source system has a file system hierarchy or
            similar.)
        is_uploaded: Specifies if the file content has been uploaded to Cognite Data Fusion or not.
        mime_type: The MIME type of the file.
        name: Name of the instance
        uploaded_time: The time the file upload completed.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "DataSheet", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    description: Optional[str] = None
    directory: Optional[str] = None
    is_uploaded: Optional[bool] = Field(None, alias="isUploaded")
    mime_type: Optional[str] = Field(None, alias="mimeType")
    name: Optional[str] = None
    uploaded_time: Optional[datetime.datetime] = Field(None, alias="uploadedTime")

    def as_write(self) -> DataSheetWrite:
        """Convert this read version of data sheet to the writing version."""
        return DataSheetWrite.model_validate(as_write_args(self))


class DataSheetWrite(DomainModelWrite):
    """This represents the writing version of data sheet.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the data sheet.
        data_record: The data record of the data sheet node.
        description: Description of the instance
        directory: Contains the path elements from the source (if the source system has a file system hierarchy or
            similar.)
        mime_type: The MIME type of the file.
        name: Name of the instance
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "description",
        "directory",
        "mime_type",
        "name",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "DataSheet", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    description: Optional[str] = None
    directory: Optional[str] = None
    mime_type: Optional[str] = Field(None, alias="mimeType")
    name: Optional[str] = None


class DataSheetList(DomainModelList[DataSheet]):
    """List of data sheets in the read version."""

    _INSTANCE = DataSheet

    def as_write(self) -> DataSheetWriteList:
        """Convert these read versions of data sheet to the writing versions."""
        return DataSheetWriteList([node.as_write() for node in self.data])


class DataSheetWriteList(DomainModelWriteList[DataSheetWrite]):
    """List of data sheets in the writing version."""

    _INSTANCE = DataSheetWrite


def _create_data_sheet_filter(
    view_id: dm.ViewId,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    directory: str | list[str] | None = None,
    directory_prefix: str | None = None,
    is_uploaded: bool | None = None,
    mime_type: str | list[str] | None = None,
    mime_type_prefix: str | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    min_uploaded_time: datetime.datetime | None = None,
    max_uploaded_time: datetime.datetime | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if isinstance(directory, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("directory"), value=directory))
    if directory and isinstance(directory, list):
        filters.append(dm.filters.In(view_id.as_property_ref("directory"), values=directory))
    if directory_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("directory"), value=directory_prefix))
    if isinstance(is_uploaded, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isUploaded"), value=is_uploaded))
    if isinstance(mime_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("mimeType"), value=mime_type))
    if mime_type and isinstance(mime_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("mimeType"), values=mime_type))
    if mime_type_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("mimeType"), value=mime_type_prefix))
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if min_uploaded_time is not None or max_uploaded_time is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("uploadedTime"),
                gte=min_uploaded_time.isoformat(timespec="milliseconds") if min_uploaded_time else None,
                lte=max_uploaded_time.isoformat(timespec="milliseconds") if max_uploaded_time else None,
            )
        )
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _DataSheetQuery(NodeQueryCore[T_DomainModelList, DataSheetList]):
    _view_id = DataSheet._view_id
    _result_cls = DataSheet
    _result_list_cls_end = DataSheetList

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
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.directory = StringFilter(self, self._view_id.as_property_ref("directory"))
        self.is_uploaded = BooleanFilter(self, self._view_id.as_property_ref("isUploaded"))
        self.mime_type = StringFilter(self, self._view_id.as_property_ref("mimeType"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.uploaded_time = TimestampFilter(self, self._view_id.as_property_ref("uploadedTime"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.description,
                self.directory,
                self.is_uploaded,
                self.mime_type,
                self.name,
                self.uploaded_time,
            ]
        )
        self.content = FileContentAPI(client, lambda limit: self._list(limit=limit).as_node_ids())

    def list_data_sheet(self, limit: int = DEFAULT_QUERY_LIMIT) -> DataSheetList:
        return self._list(limit=limit)


class DataSheetQuery(_DataSheetQuery[DataSheetList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, DataSheetList)
