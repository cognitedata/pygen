from abc import ABC
from typing import Literal

from pydantic import JsonValue

from .constraints import Constraint
from .data_types import DataType
from .indexes import Index
from .references import ContainerReference
from .resource import APIResource, BaseModelObject, ResponseResource


class ContainerPropertyDefinition(BaseModelObject):
    immutable: bool | None = None
    nullable: bool | None = None
    auto_increment: bool | None = None
    default_value: str | int | float | bool | dict[str, JsonValue] | None = None
    description: str | None = None
    name: str | None = None
    type: DataType


class Container(APIResource[ContainerReference], ABC):
    space: str
    external_id: str
    name: str | None = None
    description: str | None = None
    used_for: Literal["node", "edge", "all"] | None = None
    properties: dict[str, ContainerPropertyDefinition]
    constraints: dict[str, Constraint] | None = None
    indexes: dict[str, Index] | None = None

    def as_reference(self) -> ContainerReference:
        return ContainerReference(
            space=self.space,
            external_id=self.external_id,
        )


class ContainerRequest(Container): ...


class ContainerResponse(Container, ResponseResource[ContainerRequest]):
    created_time: int
    last_updated_time: int
    is_global: bool

    def as_request(self) -> "ContainerRequest":
        return ContainerRequest.model_validate(self.model_dump(by_alias=True))
