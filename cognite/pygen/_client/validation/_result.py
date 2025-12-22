"""Validation result container."""

from dataclasses import dataclass, field

from ._issues import (
    DirectRelationMissingSource,
    NameConflict,
    ReverseDirectRelationMissingTarget,
    ValidationIssue,
)


@dataclass
class ValidationResult:
    """Result of validating a data model.

    Contains all detected issues organized by type for easy access.
    All issues are warnings that allow generation to proceed with
    graceful degradation.
    """

    # Organized issue lists for convenient access
    missing_reverse_targets: list[ReverseDirectRelationMissingTarget] = field(default_factory=list)
    missing_direct_sources: list[DirectRelationMissingSource] = field(default_factory=list)
    name_conflicts: list[NameConflict] = field(default_factory=list)

    @property
    def all_issues(self) -> list[ValidationIssue]:
        """All issues as a flat list."""
        return [*self.missing_reverse_targets, *self.missing_direct_sources, *self.name_conflicts]

    @property
    def has_issues(self) -> bool:
        """Whether any issues were detected."""
        return bool(self.missing_reverse_targets or self.missing_direct_sources or self.name_conflicts)

    @property
    def issue_count(self) -> int:
        """Total number of issues."""
        return len(self.missing_reverse_targets) + len(self.missing_direct_sources) + len(self.name_conflicts)

    def add(self, issue: ValidationIssue) -> None:
        """Add an issue to the appropriate list."""
        if isinstance(issue, ReverseDirectRelationMissingTarget):
            self.missing_reverse_targets.append(issue)
        elif isinstance(issue, DirectRelationMissingSource):
            self.missing_direct_sources.append(issue)
        elif isinstance(issue, NameConflict):
            self.name_conflicts.append(issue)

    def summary(self) -> str:
        """Human-readable summary of all issues."""
        if not self.has_issues:
            return "No validation issues found."

        parts = [f"Found {self.issue_count} validation issue(s):"]

        if self.missing_reverse_targets:
            parts.append(f"  - {len(self.missing_reverse_targets)} reverse direct relation(s) with missing targets")

        if self.missing_direct_sources:
            parts.append(f"  - {len(self.missing_direct_sources)} direct relation(s) without source views")

        if self.name_conflicts:
            parts.append(f"  - {len(self.name_conflicts)} name conflict(s) with reserved words")

        return "\n".join(parts)

    def __repr__(self) -> str:
        return (
            f"ValidationResult(missing_reverse_targets={len(self.missing_reverse_targets)}, "
            f"missing_direct_sources={len(self.missing_direct_sources)}, "
            f"name_conflicts={len(self.name_conflicts)})"
        )
