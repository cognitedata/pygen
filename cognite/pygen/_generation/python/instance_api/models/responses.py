"""Response classes for instance API operations."""

from typing import Generic, Literal

from pydantic import BaseModel, Field

from ._types import DateTimeMS
from .instance import InstanceId, T_InstanceList


class Page(BaseModel, Generic[T_InstanceList], populate_by_name=True):
    """A page of results from a paginated API response.

    Attributes:
        items: The list of items in this page.
        next_cursor: The cursor for the next page, or None if this is the last page.
    """

    items: T_InstanceList
    next_cursor: str | None = Field(default=None, alias="nextCursor")


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


class InstanceResult(BaseModel):
    """Result from instance CRUD operations.

    Attributes:
        created: List of instances that were created.
        updated: List of instances that were updated.
        unchanged: List of instances that were unchanged.
        deleted: List of instance IDs that were deleted.
    """

    created: list[InstanceResultItem] = Field(default_factory=list)
    updated: list[InstanceResultItem] = Field(default_factory=list)
    unchanged: list[InstanceResultItem] = Field(default_factory=list)
    deleted: list[InstanceId] = Field(default_factory=list)

    def extend(self, other: "InstanceResult") -> None:
        """Extend this result with another result.

        Args:
            other: The other result to extend with.
        """
        self.created.extend(other.created)
        self.updated.extend(other.updated)
        self.unchanged.extend(other.unchanged)
        self.deleted.extend(other.deleted)


class ApplyResponse(BaseModel, populate_by_name=True):
    """Response from the apply (upsert) operation.

    This matches the CDF API response format from the
    /models/instances endpoint (POST).

    Attributes:
        items: List of instances that were created or updated.
        deleted: List of instance IDs that were deleted (if delete was included in the request).
    """

    items: list[InstanceResultItem] = Field(default_factory=list)
    deleted: list[InstanceId] = Field(default_factory=list)
