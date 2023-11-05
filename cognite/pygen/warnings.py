from __future__ import annotations

from cognite.client.data_classes.data_modeling import ViewId


class PygenWarning(Warning):
    """Base class for warnings in pygen."""

    ...


class NameCollisionWarning(PygenWarning, RuntimeWarning):
    def __init__(self, view_id: ViewId, property_name: str):
        self.view_id = view_id
        self.property_name = property_name


class PythonNameCollisionWarning(NameCollisionWarning):
    ...


class PygenNameCollisionWarning(NameCollisionWarning):
    ...
