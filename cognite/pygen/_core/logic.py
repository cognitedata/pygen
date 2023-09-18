from __future__ import annotations

import hashlib
import json
from typing import Any

from cognite.client.data_classes import data_modeling as dm


def _unique_properties(
    prop: dm.MappedProperty | dm.SingleHopConnectionDefinition | dm.MappedProperty | dm.ConnectionDefinition,
) -> dict[str, Any]:
    if isinstance(prop, dm.MappedProperty):
        return {
            "container": prop.container.dump(),
            "container_property_identifier": prop.container_property_identifier,
            "default_value": prop.default_value,
            "name": prop.name,
            "nullable": prop.nullable,
            "type": prop.type.dump(),
        }
    elif isinstance(prop, dm.SingleHopConnectionDefinition):
        return {
            "direction": prop.direction,
            "name": prop.name,
            "type": prop.type.dump(),
        }
    else:
        raise ValueError(f"Unknown property type {prop}")


def _unique_views_properties(view: dm.View) -> dict[str, Any]:
    """
    Returns the properties from a view that uniquely defines it.

    This is necessary as there might be two views that have different versions, but all else is the same,
    thus they can be used to create the same data classes and APIs in the generated SDK. In other words,
    a data class and API class can be reused for two views if they have the same properties.

    Args:
        view: The View

    Returns
        A dictionary with the properties that uniquely defines the view.
    """
    return {
        "space": view.space,
        "name": view.name,
        "externalId": view.external_id,
        "properties": {name: _unique_properties(prop) for name, prop in view.properties.items()},
    }


def get_unique_views(*views: dm.View) -> list[dm.View]:
    view_hashes = set()
    unique_views = []
    for view in views:
        view_hash = hashlib.shake_256(json.dumps(_unique_views_properties(view)).encode("utf-8")).hexdigest(16)
        if view_hash not in view_hashes:
            unique_views.append(view)
            view_hashes.add(view_hash)
    return unique_views
