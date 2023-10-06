from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from markets.client.data_classes import PygenPool, PygenPoolApply, PygenPoolList, PygenPoolApplyList


class PygenPoolAPI(TypeAPI[PygenPool, PygenPoolApply, PygenPoolList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=PygenPool,
            class_apply_type=PygenPoolApply,
            class_list=PygenPoolList,
        )
        self._view_id = view_id

    def apply(
        self, pygen_pool: PygenPoolApply | Sequence[PygenPoolApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(pygen_pool, PygenPoolApply):
            instances = pygen_pool.to_instances_apply()
        else:
            instances = PygenPoolApplyList(pygen_pool).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str], space="market") -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> PygenPool:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> PygenPoolList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> PygenPool | PygenPoolList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def list(
        self,
        min_day_of_week: int | None = None,
        max_day_of_week: int | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PygenPoolList:
        filter_ = _create_filter(
            self._view_id,
            min_day_of_week,
            max_day_of_week,
            name,
            name_prefix,
            timezone,
            timezone_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    min_day_of_week: int | None = None,
    max_day_of_week: int | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    timezone: str | list[str] | None = None,
    timezone_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_day_of_week or max_day_of_week:
        filters.append(dm.filters.Range(view_id.as_property_ref("dayOfWeek"), gte=min_day_of_week, lte=max_day_of_week))
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if timezone and isinstance(timezone, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timezone"), value=timezone))
    if timezone and isinstance(timezone, list):
        filters.append(dm.filters.In(view_id.as_property_ref("timezone"), values=timezone))
    if timezone_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("timezone"), value=timezone_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
