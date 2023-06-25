from __future__ import annotations

from cognite.client.data_classes.data_modeling.instances import Properties, PropertyValue


def unpack_properties(properties: Properties) -> dict[str, PropertyValue]:
    unpacked = {}
    for view_properties in properties.values():
        for prop_name, prop_value in view_properties.items():
            if isinstance(prop_value, (str, int, float, bool, list)):
                unpacked[prop_name] = prop_value
            elif isinstance(prop_value, dict):
                # Dicts are assumed to be reference properties
                if "space" in prop_value and "externalId" in prop_value:
                    unpacked[prop_name] = prop_value["externalId"]
                else:
                    raise ValueError(f"Unexpected reference property {prop_value}")
            else:
                raise ValueError(f"Unexpected property value type {type(prop_value)}")
    return unpacked
