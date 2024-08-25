from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling import DataModelId


class PygenException(Exception):
    """
    Base class for all exceptions raised by pygen.
    """

    pass


class DataModelNotFound(PygenException):
    """
    Raised when a data model(s) is not found.
    """

    def __init__(self, missing_ids: DataModelId | Sequence[DataModelId]):
        self.missing_ids = missing_ids

    def __str__(self) -> str:
        return f"Could not find data model(s) with id(s) {self.missing_ids}"


class NameConflict(PygenException):
    """
    Raised when a name conflict is detected.
    """

    def __init__(self, conflicting_names: list[tuple[str, list[dm.VersionedDataModelingId]]], class_name: str) -> None:
        self.conflicting_names = conflicting_names
        self.class_name = class_name

    def __str__(self) -> str:
        return (
            f"Name conflict detected in {self.class_name}. The following names are used by multiple views/data models: "
            f"{self.conflicting_names}"
        )


class ReservedWordConflict(PygenException):
    def __init__(self, source: str | list[Any], reserved_word: str):
        self.source = source
        self.reserved_word = reserved_word

    def __str__(self) -> str:
        return (
            f"Reserved word conflict detected in {self.source}. The following reserved word is used: "
            f"{self.reserved_word}. This is used by the SDK and cannot be used by the user."
        )


class PygenImportError(PygenException, ModuleNotFoundError):
    """Raised when a module is not found."""

    ...
