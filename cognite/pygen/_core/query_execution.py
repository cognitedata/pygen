from typing import Any, Literal

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import filters


def execute_query(
    client: CogniteClient,
    view: dm.View,
    operation: Literal["list", "aggregate", "search"],
    filter: filters.Filter,
    properties: list[str],
    groupby: str,
) -> dict[str, Any]:
    raise NotImplementedError()
