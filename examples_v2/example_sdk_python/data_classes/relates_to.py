from typing import ClassVar, Literal

from pydantic import Field

from cognite.pygen._python.instance_api.models._references import ViewReference
from cognite.pygen._python.instance_api.models._types import DateTime
from cognite.pygen._python.instance_api.models.dtype_filters import (
    DateTimeFilter,
    FilterContainer,
    FloatFilter,
    TextFilter,
)
from cognite.pygen._python.instance_api.models.instance import (
    Instance,
    InstanceList,
    InstanceWrite,
)


class RelatesToWrite(InstanceWrite):
    """Write class for RelatesTo Edge instances."""

    _view_id: ClassVar[ViewReference] = ViewReference(space="pygen_example", external_id="RelatesTo", version="v1")
    instance_type: Literal["edge"] = Field("edge", alias="instanceType")
    relation_type: str
    strength: float
    created_at: DateTime


class RelatesTo(Instance):
    """Read class for RelatesTo Edge instances."""

    _view_id: ClassVar[ViewReference] = ViewReference(space="pygen_example", external_id="RelatesTo", version="v1")
    instance_type: Literal["edge"] = Field("edge", alias="instanceType")
    relation_type: str
    strength: float
    created_at: DateTime

    def as_write(self) -> RelatesToWrite:
        """Convert to write representation."""
        return RelatesToWrite.model_validate(self.model_dump(by_alias=True))


class RelatesToList(InstanceList[RelatesTo]):
    """List of RelatesTo Edge instances."""

    _INSTANCE: ClassVar[type[RelatesTo]] = RelatesTo


class RelatesToFilter(FilterContainer):
    def __init__(self, operator: Literal["and", "or"] = "and") -> None:
        view_id = RelatesTo._view_id
        self.relation_type = TextFilter(view_id, "relation_type", operator)
        self.strength = FloatFilter(view_id, "strength", operator)
        self.created_at = DateTimeFilter(view_id, "created_at", operator)
        super().__init__(
            data_type_filters=[
                self.relation_type,
                self.strength,
                self.created_at,
            ],
            operator=operator,
            instance_type="edge",
        )
