from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from markets.client.data_classes import Process, ProcessApply, ProcessList, ProcessApplyList, ProcessTextFields
from markets.client.data_classes._process import _PROCESS_TEXT_PROPERTIES_BY_FIELD


class ProcessAPI(TypeAPI[Process, ProcessApply, ProcessList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Process,
            class_apply_type=ProcessApply,
            class_list=ProcessList,
        )
        self._view_id = view_id

    def apply(self, proces: ProcessApply | Sequence[ProcessApply], replace: bool = False) -> dm.InstancesApplyResult:
        if isinstance(proces, ProcessApply):
            instances = proces.to_instances_apply()
        else:
            instances = ProcessApplyList(proces).to_instances_apply()
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space="market") -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Process:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ProcessList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Process | ProcessList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: ProcessTextFields | Sequence[ProcessTextFields] | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ProcessList:
        filter_ = _create_filter(
            self._view_id,
            bid,
            name,
            name_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _PROCESS_TEXT_PROPERTIES_BY_FIELD, properties, filter_, limit)

    def list(
        self,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ProcessList:
        filter_ = _create_filter(
            self._view_id,
            bid,
            name,
            name_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if bid and isinstance(bid, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bid"), value={"space": "market", "externalId": bid}))
    if bid and isinstance(bid, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bid"), value={"space": bid[0], "externalId": bid[1]}))
    if bid and isinstance(bid, list) and isinstance(bid[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("bid"), values=[{"space": "market", "externalId": item} for item in bid]
            )
        )
    if bid and isinstance(bid, list) and isinstance(bid[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("bid"), values=[{"space": item[0], "externalId": item[1]} for item in bid]
            )
        )
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
