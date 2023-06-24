from __future__ import annotations

from collections.abc import Iterable

from cognite.client import data_modeling as dm


def one_to_many_properties(
    properties: Iterable[dm.MappedPropertyDefinition | dm.ConnectionDefinition],
) -> Iterable[dm.ConnectionDefinition]:
    for prop in properties:
        if isinstance(prop, dm.MappedPropertyDefinition):
            continue
        yield prop
