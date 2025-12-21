from typing import Literal

from pydantic import Field

from .resource import ReferenceObject


class SpaceReference(ReferenceObject):
    space: str


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


class DataModelReference(ReferenceObject):
    space: str
    external_id: str
    version: str

    def __str__(self) -> str:
        return f"{self.space}:{self.external_id}(version={self.version})"


class NodeReference(ReferenceObject):
    space: str
    external_id: str


class ContainerDirectReference(ReferenceObject):
    source: ContainerReference
    identifier: str

    def __str__(self) -> str:
        return f"{self.source!s}.{self.identifier}"


class ViewDirectReference(ReferenceObject):
    source: ViewReference
    identifier: str

    def __str__(self) -> str:
        return f"{self.source!s}.{self.identifier}"


class ContainerIndexReference(ContainerReference):
    identifier: str


class ContainerConstraintReference(ContainerReference):
    identifier: str
