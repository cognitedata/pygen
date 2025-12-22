"""Validation issue types.

Each issue type represents a specific validation problem that was detected.
Issues are informational - they describe what was found but don't dictate
how to handle it. The parser/transformer handles graceful degradation.
"""

from dataclasses import dataclass
from typing import Literal

from cognite.pygen._client.models import ViewReference


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    """Base class for all validation issues."""

    @property
    def severity(self) -> Literal["warning", "error"]:
        """Issue severity. Warnings allow generation to continue."""
        return "warning"


@dataclass(frozen=True, slots=True)
class ReverseDirectRelationMissingTarget(ValidationIssue):
    """Reverse direct relation points to a view/property that doesn't exist.

    Graceful degradation: Exclude this property from generated code.
    """

    view: ViewReference
    property_name: str
    target_view: ViewReference
    target_property: str

    def __str__(self) -> str:
        return (
            f"Reverse direct relation '{self.view.external_id}.{self.property_name}' "
            f"targets '{self.target_view.external_id}.{self.target_property}' which doesn't exist. "
            "This property will be excluded."
        )


@dataclass(frozen=True, slots=True)
class DirectRelationMissingSource(ValidationIssue):
    """Direct relation has no source view defined.

    Graceful degradation: Use generic node reference instead of typed class.
    """

    view: ViewReference
    property_name: str

    def __str__(self) -> str:
        return (
            f"Direct relation '{self.view.external_id}.{self.property_name}' "
            "has no source view defined. Will use generic node reference."
        )


@dataclass(frozen=True, slots=True)
class NameConflict(ValidationIssue):
    """Name conflicts with Python or Pydantic reserved word.

    Graceful degradation: Append underscore suffix to the name.
    """

    view: ViewReference | None
    name: str
    context: Literal["field", "class", "parameter", "file"]
    reserved_in: str  # e.g., "Python keyword", "Pydantic BaseModel", etc.

    def __str__(self) -> str:
        location = f"in '{self.view.external_id}'" if self.view else ""
        return (
            f"Name '{self.name}' {location} conflicts with {self.reserved_in} "
            f"(context: {self.context}). An underscore will be appended."
        )
