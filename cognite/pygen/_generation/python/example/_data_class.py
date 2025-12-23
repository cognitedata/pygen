from typing import Literal

from pydantic import Field, JsonValue

from cognite.pygen._generation.python.instance_api._instance import (
    Date,
    DateTime,
    Instance,
    InstanceList,
    InstanceWrite,
    ViewRef,
)


class PrimitiveNullableWrite(InstanceWrite):
    instance_type: Literal["node"] = Field("node", alias="instanceType")
    text: str | None = None
    boolean: bool | None = None
    float32: float | None = None
    float64: float | None = None
    int32: int | None = None
    int64: int | None = None
    timestamp: DateTime | None = None
    date_: Date | None = None
    json_: JsonValue | None = Field(None, alias="json")


class PrimitiveNullable(Instance):
    _view_id = ViewRef(space="sp_pygen_models", external_id="PrimitiveNullable", version="v1")
    instance_type: Literal["node"] = Field("node", alias="instanceType")
    text: str | None
    boolean: bool | None
    float32: float | None
    float64: float | None
    int32: int | None
    int64: int | None
    timestamp: DateTime | None
    date_: Date | None = Field(None, alias="date")
    json_: JsonValue | None = Field(None, alias="json")

    def as_write(self) -> PrimitiveNullableWrite:
        return PrimitiveNullableWrite.model_validate(self.dump())


class PrimitiveNullableList(InstanceList[PrimitiveNullable]):
    _INSTANCE = PrimitiveNullable
