from typing import Literal

from pydantic import Field, JsonValue

from cognite.pygen._generation.python.instance_api.models._references import ViewReference
from cognite.pygen._generation.python.instance_api.models._types import Date, DateTime
from cognite.pygen._generation.python.instance_api.models.dtype_filters import (
    BooleanFilter,
    DateFilter,
    DateTimeFilter,
    FilterContainer,
    FloatFilter,
    IntegerFilter,
    TextFilter,
)
from cognite.pygen._generation.python.instance_api.models.instance import (
    Instance,
    InstanceList,
    InstanceWrite,
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
    _view_id = ViewReference(space="sp_pygen_models", external_id="PrimitiveNullable", version="v1")
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


class PrimitiveNullableFilter(FilterContainer):
    def __init__(self, operator: Literal["and", "or"]) -> None:
        view_id = PrimitiveNullable._view_id
        self.text = TextFilter(view_id, "text", operator)
        self.boolean = BooleanFilter(view_id, "boolean", operator)
        self.float32 = FloatFilter(view_id, "float32", operator)
        self.float64 = FloatFilter(view_id, "float64", operator)
        self.int32 = IntegerFilter(view_id, "int32", operator)
        self.int64 = IntegerFilter(view_id, "int64", operator)
        self.timestamp = DateTimeFilter(view_id, "timestamp", operator)
        self.date_ = DateFilter(view_id, "date", operator)
        super().__init__(
            [
                self.text,
                self.boolean,
                self.float32,
                self.float64,
                self.int32,
                self.int64,
                self.timestamp,
                self.date_,
            ],
            operator,
        )
