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

    def dump(self, camel_case: bool = True, include_type: bool = False) -> dict[str, str]:
        """Dump the view reference to a dictionary.

        Args:
            camel_case: Whether to use camel case for the keys. Defaults to True.
            include_type: Whether to include the 'type' field in the output. Defaults to False.

        Returns:
            The dictionary representation of the view reference.
        """
        data = self.model_dump(by_alias=camel_case)
        if include_type:
            data["type"] = "view"
        return data


class NodeReference(ReferenceObject):
    space: str
    external_id: str
