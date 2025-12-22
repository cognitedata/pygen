from datetime import datetime, date
from typing import Literal

from pydantic import JsonValue, Field

from cognite.pygen._generation.python._instance_api._instance import Instance, InstanceWrite, ViewRef


class PrimitiveNullableWrite(InstanceWrite):
    instance_type: Literal["node"] = Field("node", alias="instanceType")
    text: str | None = None
    boolean: bool | None = None
    float32: float | None = None
    float64: float | None = None
    int32: int | None = None
    int64: int | None = None
    timestamp: datetime | None = None
    date_: date | None = None
    json: JsonValue | None = None


class PrimitiveNullable(Instance):
    _view_id = ViewRef(space="sp_pygen_models", external_id="PrimitiveNullable", version="v1")
    instance_type: Literal["node"] = Field("node", alias="instanceType")
    text: str | None
    boolean: bool | None
    float32: float | None
    float64: float | None
    int32: int | None
    int64: int | None
    timestamp: datetime | None
    date_: date | None = None
    json: JsonValue | None = None

    def as_write(self) -> PrimitiveNullableWrite:
        return PrimitiveNullableWrite.model_validate(self.dump())
