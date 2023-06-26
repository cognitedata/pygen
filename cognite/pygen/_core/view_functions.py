from __future__ import annotations

from collections.abc import Iterable

from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.views import ViewDirectRelation


def edge_properties(
    properties: Iterable[dm.MappedPropertyDefinition | dm.ConnectionDefinition],
) -> Iterable[dm.ConnectionDefinition | dm.MappedPropertyDefinition]:
    for prop in properties:
        if isinstance(prop, dm.MappedPropertyDefinition) and not isinstance(prop.type, ViewDirectRelation):
            continue
        yield prop
