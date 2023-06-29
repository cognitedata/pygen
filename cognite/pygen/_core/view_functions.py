from __future__ import annotations

from collections.abc import Iterable

from cognite.client import data_modeling as dm


def edge_one_to_many_properties(
    properties: Iterable[dm.MappedProperty | dm.ConnectionDefinition],
) -> Iterable[dm.ConnectionDefinition | dm.MappedProperty]:
    for prop in properties:
        if isinstance(prop, dm.MappedProperty):
            continue
        yield prop
