from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from movie_domain.client.data_classes import BestDirector, BestDirectorApply, BestDirectorList


class BestDirectorAPI(TypeAPI[BestDirector, BestDirectorApply, BestDirectorList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BestDirector,
            class_apply_type=BestDirectorApply,
            class_list=BestDirectorList,
        )
        self.view_id = view_id

    def apply(self, best_director: BestDirectorApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = best_director.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(BestDirectorApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(BestDirectorApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> BestDirector:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> BestDirectorList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> BestDirector | BestDirectorList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_year: int | None = None,
        max_year: int | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BestDirectorList:
        filters = []
        if name and isinstance(name, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("name"), value=name))
        if name and isinstance(name, list):
            filters.append(dm.filters.In(self.view_id.as_property_ref("name"), values=name))
        if name_prefix:
            filters.append(dm.filters.Prefix(self.view_id.as_property_ref("name"), value=name_prefix))
        if min_year or max_year:
            filters.append(dm.filters.Range(self.view_id.as_property_ref("year"), gte=min_year, lte=max_year))
        if external_id_prefix:
            filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
        if filter:
            filters.append(filter)

        return self._list(limit=limit, filter=dm.filters.And(*filters) if filters else None)
