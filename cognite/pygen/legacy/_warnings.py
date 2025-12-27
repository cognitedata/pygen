from __future__ import annotations

import itertools
import warnings
from collections.abc import Callable
from pathlib import Path
from typing import Literal, cast

from cognite.client.data_classes.data_modeling import PropertyId, ViewId


class PygenWarning(UserWarning):
    """Base class for warnings in pygen."""

    def warn(self):
        warnings.warn(self, stacklevel=2)


class NameCollisionWarning(PygenWarning, RuntimeWarning):
    @classmethod
    def create(
        cls,
        word: str,
        word_type: Literal["field", "data class", "parameter", "filename", "variable"],
        view_id: ViewId | None,
        property_name: str | None,
    ) -> NameCollisionWarning:
        if view_id and property_name:
            return NameCollisionViewPropertyWarning(view_id, property_name, word)
        elif view_id and word_type == "filename":
            return NameCollisionFileNameWarning(view_id, word)
        elif view_id and word_type == "data class":
            return NameCollisionDataClassNameWarning(view_id, word)
        else:
            return NameCollisionParameterWarning(word)


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


class NameCollisionViewPropertyWarning(NameCollisionWarning):
    def __init__(self, view_id: ViewId, property_name: str, word: str):
        self.view_id = view_id
        self.property_name = property_name
        self.word = word

    def __str__(self) -> str:
        return (
            f"Name collision detected in {self.view_id}: {self.property_name!r}. "
            f"An underscore will be added to the {self.word!r} to avoid name collision."
        )


class NameCollisionParameterWarning(NameCollisionWarning):
    def __init__(self, word: str):
        self.word = word

    def __str__(self) -> str:
        return (
            f"Name collision detected. The following filter parameter {self.word!r} name is used by pygen."
            "An underscore will be added to this parameter to avoid name collision."
        )


class MissingReverseDirectRelationTargetWarning(PygenWarning, UserWarning):
    def __init__(self, source: PropertyId, view: ViewId, property_: str) -> None:
        self.source = source
        self.view = view
        self.property_ = property_

    def __str__(self) -> str:
        return (
            f"Target {self.source!r} does not exists. "
            f"Skipping reverse direct relation {self.view.external_id}.{self.property_}."
        )


class UnknownConnectionTargetWarning(PygenWarning, UserWarning):
    def __init__(self, source: ViewId, view: ViewId, property_: str) -> None:
        self.source = source
        self.view = view
        self.property_ = property_

    def __str__(self) -> str:
        return (
            f"Target {self.source!r} does not exists in model. "
            f"Skipping connection {self.view.external_id}.{self.property_}."
        )


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


def print_warnings(
    warning_list: list[warnings.WarningMessage],
    console: Callable[[str], None],
    context: Literal["notebook", "cli"] = "cli",
) -> None:
    for group, group_warnings in itertools.groupby(
        sorted(warning_list, key=lambda w: w.category.__name__),
        key=lambda w: w.category,
    ):
        group_list = [w.message for w in group_warnings if isinstance(w.message, PygenWarning)]
        _print_warning_group(group_list, group, console, context)


def _print_warning_group(
    pygen_warnings: list[PygenWarning],
    group: type[PygenWarning],
    console: Callable[[str], None],
    context: Literal["notebook", "cli"],
) -> None:
    if group is PydanticNamespaceCollisionWarning:
        return
    elif context == "notebook" and group is NameCollisionFileNameWarning:
        return
    elif group is InvalidCodeGenerated or len(pygen_warnings) == 1:
        _print_one_by_one(console, pygen_warnings)
    elif issubclass(group, NameCollisionWarning | MissingReverseDirectRelationTargetWarning):
        _print_group(console, group, pygen_warnings)


def _print_one_by_one(console: Callable[[str], None], warning_list: list[PygenWarning]) -> None:
    for warning in warning_list:
        console(f"{warning!s}")


def _print_group(console: Callable[[str], None], group: type[PygenWarning], warning_list: list[PygenWarning]) -> None:
    console(f"{group.__name__}: {len(warning_list)}")
    indent = " " * 4
    if group in {NameCollisionFileNameWarning, NameCollisionDataClassNameWarning}:
        view_warnings = cast(list[NameCollisionFileNameWarning | NameCollisionDataClassNameWarning], warning_list)
        views_str = ", ".join(f"{warning.view_id!r}" for warning in view_warnings)
        console(f"{indent}The following views will have an underscore added to avoid name collision: {views_str}")
    elif group is NameCollisionViewPropertyWarning:
        property_warnings = cast(list[NameCollisionViewPropertyWarning], warning_list)
        for view, properties in itertools.groupby(
            sorted(property_warnings, key=lambda w: w.view_id.external_id), key=lambda w: w.view_id.external_id
        ):
            properties_list = list(properties)
            if len(properties_list) == 1:
                console(
                    f"{indent} The property {properties_list[0].property_name!r} in view {view} "
                    "will have an underscore to avoid name collision."
                )
            else:
                properties_str = ", ".join(warning.property_name for warning in properties)
                console(
                    f"{indent}The following properties in view {view} will have an underscore "
                    f"added to avoid name collision: {properties_str}"
                )
    elif group is MissingReverseDirectRelationTargetWarning:
        relation_warnings = cast(list[MissingReverseDirectRelationTargetWarning], warning_list)
        for relation_warn in relation_warnings:
            console(
                f"{indent} Skipping reverse direct "
                f"relation {relation_warn.view.external_id}.{relation_warn.property_}."
            )
    else:
        for warning in warning_list:
            console(f"{indent}{warning!s}")
