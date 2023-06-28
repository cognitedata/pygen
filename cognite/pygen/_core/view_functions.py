from __future__ import annotations

from collections.abc import Iterable

from cognite.client import data_modeling as dm


def edge_properties(
    properties: Iterable[dm.MappedProperty | dm.ConnectionDefinition],
) -> Iterable[dm.ConnectionDefinition | dm.MappedProperty]:
    for prop in properties:
        if isinstance(prop, dm.MappedProperty) and not isinstance(prop.type, dm.DirectRelation):
            continue
        yield prop
