from __future__ import annotations

import warnings

from cognite.client.data_classes.data_modeling import ViewId


class PygenWarning(UserWarning):
    """Base class for warnings in pygen."""

    ...


class NameCollisionWarning(PygenWarning, RuntimeWarning):
    ...


class ViewPropertyNameCollisionWarning(NameCollisionWarning):
    def __init__(self, view_id: ViewId, property_name: str):
        self.view_id = view_id
        self.property_name = property_name

    def __str__(self) -> str:
        return (
            f"Name collision detected in {self.view_id}. "
            f"The following property name is used: {self.property_name}."
            "An underscore will be added to the property name to avoid name collision."
        )

    def warning(self):
        warnings.warn(self, stacklevel=3)


class ViewIDNameCollisionWarning(NameCollisionWarning):
    ...
