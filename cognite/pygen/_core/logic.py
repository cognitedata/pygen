from __future__ import annotations

import hashlib
import json

from typing import Any

from cognite.client.data_classes import data_modeling as dm

from cognite.pygen._core.data_classes import APIClass
from cognite.pygen._core.generators import APIGenerator


def find_dependencies(apis: list[APIGenerator]) -> dict[APIClass, set[APIClass]]:
    class_by_data_class_name = {api.api_class.data_class: api.api_class for api in apis}
    return {
        api.api_class: {class_by_data_class_name[d] for d in dependencies}
        for api in apis
        if (dependencies := api.fields.dependencies)
    }


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
    thus they can be used to create the same data classes and apis in the generated SDK.

    Args:
        view: The View

    Returns
        A dictionary with the properties that uniquely defines the view.
    """
    return {
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
