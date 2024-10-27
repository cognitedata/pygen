from collections.abc import Mapping

from cognite.client import data_modeling as dm

_READONLY_PROPERTIES: Mapping[dm.ContainerId, frozenset[str]] = {
    dm.ContainerId("cdf_cdm", "CogniteAsset"): frozenset(
        {"assetHierarchy_root", "assetHierarchy_path", "assetHierarchy_path_last_updated_time"}
    ),
    dm.ContainerId("cdf_cdm", "CogniteFile"): frozenset({"isUploaded", "uploadedTime"}),
}


def is_readonly_property(prop: dm.MappedProperty) -> bool:
    return prop.container in _READONLY_PROPERTIES and prop.name in _READONLY_PROPERTIES[prop.container]
