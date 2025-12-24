from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel


class ReferenceObject(BaseModel):
    """Base class for all reference objects - these are identifiers."""

    model_config = ConfigDict(frozen=True, populate_by_name=True, alias_generator=to_camel)


class ContainerReference(ReferenceObject):
    type: Literal["container"] = Field("container", exclude=True)
    space: str
    external_id: str

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}"


class ViewReference(ReferenceObject):
    type: Literal["view"] = Field("view", exclude=True)
    space: str
    external_id: str
    version: str

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}(version={self.version})"


class NodeReference(ReferenceObject):
    space: str
    external_id: str
