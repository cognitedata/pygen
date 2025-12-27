from collections.abc import Mapping

from cognite.client import data_modeling as dm


def is_pyodide() -> bool:
    try:
        from pyodide.ffi import IN_BROWSER  # type: ignore [import-not-found]
    except ModuleNotFoundError:
        return False
    return IN_BROWSER


_READONLY_PROPERTIES: Mapping[dm.ContainerId, frozenset[str]] = {
    dm.ContainerId("cdf_cdm", "CogniteAsset"): frozenset(
        {"assetHierarchy_root", "assetHierarchy_path", "assetHierarchy_path_last_updated_time"}
    ),
    dm.ContainerId("cdf_cdm", "CogniteFile"): frozenset({"isUploaded", "uploadedTime"}),
}

COGNITE_TIMESERIES = dm.ContainerId("cdf_cdm", "CogniteTimeSeries")
COGNITE_FILE = dm.ContainerId("cdf_cdm", "CogniteFile")


def is_readonly_property(container: dm.ContainerId, identifier: str) -> bool:
    return container in _READONLY_PROPERTIES and identifier in _READONLY_PROPERTIES[container]
