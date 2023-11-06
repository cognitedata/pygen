from __future__ import annotations

import warnings

from cognite.client.data_classes.data_modeling import ViewId


class PygenWarning(UserWarning):
    """Base class for warnings in pygen."""

    ...


class NameCollisionWarning(PygenWarning, RuntimeWarning):
    @classmethod
    def create(cls, word: str, view_id: ViewId | None, property_name: str | None) -> NameCollisionWarning:
        if view_id and property_name:
            return ViewPropertyNameCollisionWarning(view_id, property_name, word)
        elif view_id:
            return ViewNameCollisionWarning(view_id, word)
        else:
            return ParameterNameCollisionWarning(word)

    def warn(self):
        warnings.warn(self, stacklevel=2)


class ViewNameCollisionWarning(NameCollisionWarning):
    def __init__(self, view_id: ViewId, word: str):
        self.view_id = view_id
        self.word = word

    def __str__(self) -> str:
        return (
            f"Name collision detected in {self.view_id}: {self.word!r}. "
            f"An underscore will be added to the {self.word!r} to avoid name collision."
        )


class ViewPropertyNameCollisionWarning(NameCollisionWarning):
    def __init__(self, view_id: ViewId, property_name: str, word: str):
        self.view_id = view_id
        self.property_name = property_name
        self.word = word

    def __str__(self) -> str:
        return (
            f"Name collision detected in {self.view_id}: {self.property_name!r}. "
            f"An underscore will be added to the {self.word!r} to avoid name collision."
        )


class ParameterNameCollisionWarning(NameCollisionWarning):
    def __init__(self, word: str):
        self.word = word

    def __str__(self) -> str:
        return (
            f"Name collision detected. The following filter parameter {self.word!r} name is used by pygen."
            "An underscore will be added to this parameter to avoid name collision."
        )
