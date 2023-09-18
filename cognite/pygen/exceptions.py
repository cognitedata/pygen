from __future__ import annotations

from collections.abc import Sequence

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

    def __init__(self, conflicting_names: Sequence[str]):
        self.conflicting_names = conflicting_names

    def __str__(self) -> str:
        return f"Name conflict detected. The following names are used multiple times: {self.conflicting_names}"
