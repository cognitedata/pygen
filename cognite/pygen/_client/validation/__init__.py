"""Validation layer for data models.

This module provides validation for CDF data models before code generation.
It detects issues such as:
- Missing reverse direct relation targets
- Missing source in direct relations
- Name conflicts with Python/Pydantic reserved words

Validation produces warnings but allows graceful degradation for incomplete models.
"""

from ._issues import (
    DirectRelationMissingSource,
    NameConflict,
    ReverseDirectRelationMissingTarget,
    ValidationIssue,
)
from ._reserved_words import (
    DATA_CLASS_RESERVED,
    FIELD_RESERVED,
    FILE_RESERVED,
    PARAMETER_RESERVED,
    is_reserved,
)
from ._result import ValidationResult
from ._validator import validate_data_model, validate_views

__all__ = [
    "DATA_CLASS_RESERVED",
    "FIELD_RESERVED",
    "FILE_RESERVED",
    "PARAMETER_RESERVED",
    "DirectRelationMissingSource",
    "NameConflict",
    "ReverseDirectRelationMissingTarget",
    "ValidationIssue",
    "ValidationResult",
    "is_reserved",
    "validate_data_model",
    "validate_views",
]
