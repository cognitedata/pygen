from abc import ABC
from typing import Any

from pydantic import Field, field_serializer
from pydantic_core.core_schema import FieldSerializationInfo

from .references import DataModelReference, ViewReference
from .resource import APIResource, ResponseResource


class DataModel(APIResource[DataModelReference], ABC):
    """Cognite Data Model resource.

    Data models group and structure views into reusable collections.
    A data model contains a set of views where the node types can
    refer to each other with direct relations and edges.
    """

    space: str
    external_id: str
    version: str
    description: str | None = None
    # The API supports View here, but in Neat we will only use ViewReference
    views: list[ViewReference] | None = Field(
        description="List of views included in this data model.",
        default=None,
    )

    def as_reference(self) -> DataModelReference:
        return DataModelReference(
            space=self.space,
            external_id=self.external_id,
            version=self.version,
        )

    @field_serializer("views", mode="plain")
    @classmethod
    def serialize_views(
        cls, views: list[ViewReference] | None, info: FieldSerializationInfo
    ) -> list[dict[str, Any]] | None:
        if views is None:
            return None
        output: list[dict[str, Any]] = []
        for view in views:
            dumped = view.model_dump(**vars(info))
            dumped["type"] = "view"
            output.append(dumped)
        return output


class DataModelRequest(DataModel): ...


class DataModelResponse(DataModel, ResponseResource[DataModelRequest]):
    created_time: int
    last_updated_time: int
    is_global: bool

    def as_request(self) -> DataModelRequest:
        return DataModelRequest.model_validate(self.model_dump(by_alias=True))
