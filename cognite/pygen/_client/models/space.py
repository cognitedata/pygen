from abc import ABC

from .references import SpaceReference
from .resource import APIResource, ResponseResource


class Space(APIResource[SpaceReference], ABC):
    space: str
    name: str | None = None
    description: str | None = None

    def as_reference(self) -> SpaceReference:
        return SpaceReference(space=self.space)


class SpaceRequest(Space): ...


class SpaceResponse(Space, ResponseResource[SpaceReference, Space]):
    created_time: int
    last_updated_time: int
    is_global: bool

    def as_request(self) -> "SpaceRequest":
        return SpaceRequest.model_validate(self.model_dump(by_alias=True))
