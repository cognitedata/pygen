from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class BaseModelObject(BaseModel):
    """Base class for all object. This includes resources and nested objects."""

    model_config = ConfigDict(alias_generator=to_camel, extra="ignore", populate_by_name=True)
    ...


class ReferenceObject(BaseModelObject):
    """Base class for all reference objects - these are identifiers."""

    model_config = ConfigDict(frozen=True)


T_Reference = TypeVar("T_Reference", bound=ReferenceObject, covariant=True)


class APIResource(BaseModelObject, Generic[T_Reference], ABC):
    """Base class for all data modeling resources."""

    @abstractmethod
    def as_reference(self) -> T_Reference:
        """Convert the resource to a reference object (identifier)."""
        raise NotImplementedError()


T_APIResource = TypeVar("T_APIResource", bound=APIResource)


class ResponseResource(APIResource[T_Reference], Generic[T_Reference, T_APIResource], ABC):
    """Base class for all writeable data modeling resources."""

    @abstractmethod
    def as_request(self) -> T_APIResource:
        """Convert the response model to a request model by removing read-only fields."""
        raise NotImplementedError()
