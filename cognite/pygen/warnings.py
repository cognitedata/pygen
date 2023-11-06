from __future__ import annotations

import warnings

from cognite.client.data_classes.data_modeling import ViewId


class PygenWarning(UserWarning):
    """Base class for warnings in pygen."""

    ...


class NameCollisionWarning(PygenWarning, RuntimeWarning):
    @classmethod
    def create(cls, view_id: ViewId, word_type: str, property_name: str | None) -> NameCollisionWarning:
        if property_name is None:
            return ViewNameCollisionWarning(view_id, word_type)
        return ViewPropertyNameCollisionWarning(view_id, property_name, word_type)

    def warn(self):
        warnings.warn(self, stacklevel=3)


class ViewNameCollisionWarning(NameCollisionWarning):
    def __init__(self, view_id: ViewId, word_type: str):
        self.view_id = view_id
        self.word_type = word_type

    def __str__(self) -> str:
        return (
            f"Name collision detected in {self.view_id}. "
            f"An underscore will be added to the {self.word_type} to avoid name collision."
        )


class ViewPropertyNameCollisionWarning(NameCollisionWarning):
    def __init__(self, view_id: ViewId, property_name: str, word_type: str):
        self.view_id = view_id
        self.property_name = property_name
        self.word_type = word_type

    def __str__(self) -> str:
        return (
            f"Name collision detected in {self.view_id}. "
            f"The following property name is used: {self.property_name}."
            f"An underscore will be added to the {self.word_type} to avoid name collision."
        )
