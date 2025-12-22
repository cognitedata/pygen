from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from cognite.pygen._client.models import DataModel, DataModelReference, DataModelRequest, DataModelResponse

from .resources import ResourceAPI


class DataModelsAPI(ResourceAPI[DataModelRequest, DataModelResponse, DataModelReference]):
    """Data models resource client."""

    resource_path = "/models/datamodels"
    request_cls = DataModelRequest
    response_cls = DataModelResponse

    def _to_reference(self, value: Any) -> DataModelReference:
        if isinstance(value, DataModelReference):
            return value
        if isinstance(value, DataModelResponse | DataModel):
            return value.as_reference()
        if isinstance(value, tuple) and len(value) == 3:
            space, external_id, version = value
            if not all(isinstance(item, str) for item in (space, external_id, version)):
                raise TypeError("Tuple reference for DataModel must contain three strings")
            return DataModelReference(space=space, external_id=external_id, version=version)
        raise TypeError("Unsupported reference type for DataModel")

    def _to_request(self, value: Any) -> DataModelRequest:
        if isinstance(value, DataModelRequest):
            return value
        if isinstance(value, DataModelResponse):
            return value.as_request()
        if isinstance(value, DataModel):
            return DataModelRequest.model_validate(value.model_dump(by_alias=True))
        raise TypeError("Unsupported resource type for DataModel")

    def iterate(
        self, *, space: str | None = None, include_global: bool = False, limit: int | None = None
    ) -> Iterator[DataModelResponse]:
        return super().iterate(space=space, include_global=include_global, limit=limit)

    def list(
        self, *, space: str | None = None, include_global: bool = False, limit: int | None = None
    ) -> list[DataModelResponse]:
        return list(super().iterate(space=space, include_global=include_global, limit=limit))
