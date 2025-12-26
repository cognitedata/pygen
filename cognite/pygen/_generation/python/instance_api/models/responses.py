"""Response classes for instance API operations."""

from functools import cached_property
from typing import Any, Generic, Literal

from pydantic import BaseModel, Field

from ._types import DateTimeMS
from .instance import InstanceId, T_InstanceList


class RetrieveResponse(BaseModel, Generic[T_InstanceList], populate_by_name=True):
    """Response from a retrieve operation.

    Attributes:
        items: The list of items returned by the operation.
        typing: Optional typing information about the items.
    """

    items: T_InstanceList
    typing: dict[str, Any] | None = None


class ListResponse(BaseModel, Generic[T_InstanceList], populate_by_name=True):
    """Response from a list operation.

    Attributes:
        items: The list of items returned by the operation.
        typing: Optional typing information about the items.
    """

    items: T_InstanceList
    typing: dict[str, Any] | None = None


class Page(BaseModel, Generic[T_InstanceList], populate_by_name=True):
    """A page of results from a paginated API response.

    Attributes:
        items: The list of items in this page.
        next_cursor: The cursor for the next page, or None if this is the last page.
    """

    items: T_InstanceList
    next_cursor: str | None = Field(default=None, alias="nextCursor")
    typing: dict[str, Any] | None = None
    debug: dict[str, Any] | None = Field(default=None)


class InstanceResultItem(BaseModel, populate_by_name=True):
    """Result item from instance operations.

    Attributes:
        instance_type: The type of the instance (node or edge).
        version: The version of the instance after the operation.
        was_modified: Whether the instance was modified by the operation.
        space: The space of the instance.
        external_id: The external ID of the instance.
        created_time: The time the instance was created.
        last_updated_time: The time the instance was last updated.
    """

    instance_type: Literal["node", "edge"] = Field(alias="instanceType")
    space: str
    external_id: str = Field(alias="externalId")
    version: int
    was_modified: bool = Field(alias="wasModified")
    created_time: DateTimeMS = Field(alias="createdTime")
    last_updated_time: DateTimeMS = Field(alias="lastUpdatedTime")


class UpsertResult(BaseModel):
    """Result from instance CRUD operations.

    Attributes:
        items: List of instance result items.
        deleted: List of instance IDs that were deleted.
    """

    items: list[InstanceResultItem] = Field(default_factory=list)
    deleted: list[InstanceId] = Field(default_factory=list)

    @cached_property
    def created(self) -> list[InstanceResultItem]:
        return [item for item in self.items if item.was_modified and item.created_time == item.last_updated_time]

    @cached_property
    def updated(self) -> list[InstanceResultItem]:
        return [item for item in self.items if item.was_modified and item.created_time != item.last_updated_time]

    @cached_property
    def unchanged(self) -> list[InstanceResultItem]:
        return [item for item in self.items if not item.was_modified]

    def extend(self, other: "UpsertResult") -> None:
        """Extend this result with another result.

        Args:
            other: The other result to extend with.
        """
        self.items.extend(other.items)
        self.deleted.extend(other.deleted)


class DeleteResponse(BaseModel):
    """Response from the delete operation.

    This matches the CDF API response format from the
    /models/instances endpoint (DELETE).

    Attributes:
        items: List of instance IDs that were deleted.
    """

    items: list[InstanceId] = Field(default_factory=list)
