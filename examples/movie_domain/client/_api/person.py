from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from movie_domain.client.data_classes import (
    Person,
    PersonApply,
    PersonList,
    PersonApplyList,
    PersonFields,
    PersonTextFields,
    DomainModelApply,
)
from movie_domain.client.data_classes._person import _PERSON_PROPERTIES_BY_FIELD


class PersonRolesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Person.roles"},
        )
        if isinstance(external_id, str):
            is_person = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_person))

        else:
            is_persons = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_persons))

    def list(
        self, person_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Person.roles"},
        )
        filters.append(is_edge_type)
        if person_id:
            person_ids = [person_id] if isinstance(person_id, str) else person_id
            is_persons = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in person_ids],
            )
            filters.append(is_persons)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class PersonAPI(TypeAPI[Person, PersonApply, PersonList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[PersonApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Person,
            class_apply_type=PersonApply,
            class_list=PersonList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.roles = PersonRolesAPI(client)

    def apply(self, person: PersonApply | Sequence[PersonApply], replace: bool = False) -> dm.InstancesApplyResult:
        if isinstance(person, PersonApply):
            instances = person.to_instances_apply(self._view_by_write_class)
        else:
            instances = PersonApplyList(person).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Person:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> PersonList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Person | PersonList:
        if isinstance(external_id, str):
            person = self._retrieve((self._sources.space, external_id))

            role_edges = self.roles.retrieve(external_id)
            person.roles = [edge.end_node.external_id for edge in role_edges]

            return person
        else:
            persons = self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

            role_edges = self.roles.retrieve(external_id)
            self._set_roles(persons, role_edges)

            return persons

    def search(
        self,
        query: str,
        properties: PersonTextFields | Sequence[PersonTextFields] | None = None,
        min_birth_year: int | None = None,
        max_birth_year: int | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PersonList:
        filter_ = _create_filter(
            self._view_id,
            min_birth_year,
            max_birth_year,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _PERSON_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PersonFields | Sequence[PersonFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: PersonTextFields | Sequence[PersonTextFields] | None = None,
        min_birth_year: int | None = None,
        max_birth_year: int | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PersonFields | Sequence[PersonFields] | None = None,
        group_by: PersonFields | Sequence[PersonFields] = None,
        query: str | None = None,
        search_properties: PersonTextFields | Sequence[PersonTextFields] | None = None,
        min_birth_year: int | None = None,
        max_birth_year: int | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PersonFields | Sequence[PersonFields] | None = None,
        group_by: PersonFields | Sequence[PersonFields] | None = None,
        query: str | None = None,
        search_property: PersonTextFields | Sequence[PersonTextFields] | None = None,
        min_birth_year: int | None = None,
        max_birth_year: int | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            min_birth_year,
            max_birth_year,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _PERSON_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: PersonFields,
        interval: float,
        query: str | None = None,
        search_property: PersonTextFields | Sequence[PersonTextFields] | None = None,
        min_birth_year: int | None = None,
        max_birth_year: int | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            min_birth_year,
            max_birth_year,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _PERSON_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        min_birth_year: int | None = None,
        max_birth_year: int | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> PersonList:
        filter_ = _create_filter(
            self._view_id,
            min_birth_year,
            max_birth_year,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )

        persons = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := persons.as_external_ids()) > IN_FILTER_LIMIT:
                role_edges = self.roles.list(limit=-1)
            else:
                role_edges = self.roles.list(external_ids, limit=-1)
            self._set_roles(persons, role_edges)

        return persons

    @staticmethod
    def _set_roles(persons: Sequence[Person], role_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in role_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for person in persons:
            node_id = person.id_tuple()
            if node_id in edges_by_start_node:
                person.roles = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    min_birth_year: int | None = None,
    max_birth_year: int | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_birth_year or max_birth_year:
        filters.append(dm.filters.Range(view_id.as_property_ref("birthYear"), gte=min_birth_year, lte=max_birth_year))
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
