from __future__ import annotations

import datetime
from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from shop.client.data_classes import Case, CaseApply, CaseList, CaseApplyList


class CaseAPI(TypeAPI[Case, CaseApply, CaseList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Case,
            class_apply_type=CaseApply,
            class_list=CaseList,
        )
        self.view_id = view_id

    def apply(self, case: CaseApply | Sequence[CaseApply], replace: bool = False) -> dm.InstancesApplyResult:
        if isinstance(case, CaseApply):
            instances = case.to_instances_apply()
        else:
            instances = CaseApplyList(case).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(CaseApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(CaseApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Case:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> CaseList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Case | CaseList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(
        self,
        arguments: str | list[str] | None = None,
        arguments_prefix: str | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        run_status: str | list[str] | None = None,
        run_status_prefix: str | None = None,
        scenario: str | list[str] | None = None,
        scenario_prefix: str | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CaseList:
        filter_ = _create_filter(
            self.view_id,
            arguments,
            arguments_prefix,
            min_end_time,
            max_end_time,
            name,
            name_prefix,
            run_status,
            run_status_prefix,
            scenario,
            scenario_prefix,
            min_start_time,
            max_start_time,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    arguments: str | list[str] | None = None,
    arguments_prefix: str | None = None,
    min_end_time: datetime.datetime | None = None,
    max_end_time: datetime.datetime | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    run_status: str | list[str] | None = None,
    run_status_prefix: str | None = None,
    scenario: str | list[str] | None = None,
    scenario_prefix: str | None = None,
    min_start_time: datetime.datetime | None = None,
    max_start_time: datetime.datetime | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if arguments and isinstance(arguments, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("arguments"), value=arguments))
    if arguments and isinstance(arguments, list):
        filters.append(dm.filters.In(view_id.as_property_ref("arguments"), values=arguments))
    if arguments_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("arguments"), value=arguments_prefix))
    if min_end_time or max_end_time:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("end_time"),
                gte=min_end_time.isoformat() if min_end_time else None,
                lte=max_end_time.isoformat() if max_end_time else None,
            )
        )
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if run_status and isinstance(run_status, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("runStatus"), value=run_status))
    if run_status and isinstance(run_status, list):
        filters.append(dm.filters.In(view_id.as_property_ref("runStatus"), values=run_status))
    if run_status_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("runStatus"), value=run_status_prefix))
    if scenario and isinstance(scenario, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("scenario"), value=scenario))
    if scenario and isinstance(scenario, list):
        filters.append(dm.filters.In(view_id.as_property_ref("scenario"), values=scenario))
    if scenario_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("scenario"), value=scenario_prefix))
    if min_start_time or max_start_time:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("start_time"),
                gte=min_start_time.isoformat() if min_start_time else None,
                lte=max_start_time.isoformat() if max_start_time else None,
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
