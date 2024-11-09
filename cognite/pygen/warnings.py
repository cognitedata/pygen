from __future__ import annotations

import itertools
import warnings
from collections.abc import Callable
from pathlib import Path
from typing import Literal

from cognite.client.data_classes.data_modeling import ViewId


class PygenWarning(UserWarning):
    """Base class for warnings in pygen."""

    def warn(self):
        warnings.warn(self, stacklevel=2)


class NameCollisionWarning(PygenWarning, RuntimeWarning):
    @classmethod
    def create(
        cls,
        word: str,
        word_type: Literal["field", "data class", "parameter", "filename"],
        view_id: ViewId | None,
        property_name: str | None,
    ) -> NameCollisionWarning:
        if view_id and property_name:
            return ViewPropertyNameCollisionWarning(view_id, property_name, word)
        elif view_id and word_type == "filename":
            return NameCollisionFileNameWarning(view_id, word)
        elif view_id and word_type == "data class":
            return NameCollisionDataClassNameWarning(view_id, word)
        else:
            return ParameterNameCollisionWarning(word)


class NameCollisionFileNameWarning(NameCollisionWarning):
    def __init__(self, view_id: ViewId, word: str):
        self.view_id = view_id
        self.word = word

    def __str__(self) -> str:
        return (
            f"Name collision detected in {self.view_id}: {self.word!r}. "
            f"An underscore will be added to the {self.word!r} to avoid name collision."
        )


class NameCollisionDataClassNameWarning(NameCollisionWarning):
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


class MissingReverseDirectRelationTargetWarning(PygenWarning, UserWarning):
    def __init__(self, target: str, field: str) -> None:
        self.target = target
        self.field = field

    def __str__(self) -> str:
        return f"Target {self.target} does not exists. Skipping reverse direct relation {self.field}."


class InvalidCodeGenerated(PygenWarning, UserWarning):
    def __init__(self, filepath: str | Path, error_message: str) -> None:
        self.filepath = filepath
        self.error_message = error_message

    def __str__(self) -> str:
        return f"Invalid code generated in {self.filepath}. {self.error_message}"


class PydanticNamespaceCollisionWarning(PygenWarning, UserWarning):
    def __init__(self, view_id: ViewId, names: str) -> None:
        self.view_id = view_id
        self.names = names

    def __str__(self) -> str:
        return (
            f"Field(s) {self.names} in view {self.view_id} has potential conflict "
            "with protected Pydantic namespace 'model_'"
        )


def print_warnings(warning_list: list[warnings.WarningMessage], console: Callable[[str], None]) -> None:
    for group, group_warnings in itertools.groupby(
        sorted(warning_list, key=lambda w: w.category),  # type: ignore[arg-type, return-value]
        key=lambda w: w.category,
    ):
        group_list = [w.message for w in group_warnings if isinstance(w.message, PygenWarning)]
        _print_warning(group_list, group, console)


def _print_warning(
    pygen_warnings: list[PygenWarning], group: type[PygenWarning], console: Callable[[str], None]
) -> None:
    if group is PydanticNamespaceCollisionWarning:
        return
    if group is InvalidCodeGenerated:
        _print_one_by_one(console, *pygen_warnings)


def _print_one_by_one(console: Callable[[str], None], *warning_list) -> None:
    for warning in warning_list:
        console(f"{warning!s}")
