from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from movie_domain.client.data_classes import Person, PersonApply, PersonList


class PersonRolesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Person.roles"},
        )
        if isinstance(external_id, str):
            is_person = f.Equals(
                ["edge", "startNode"],
                {"space": "IntegrationTestsImmutable", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_person))

        else:
            is_persons = f.In(
                ["edge", "startNode"],
                [{"space": "IntegrationTestsImmutable", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_persons))

    def list(self, person_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
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
                [{"space": "IntegrationTestsImmutable", "externalId": ext_id} for ext_id in person_ids],
            )
            filters.append(is_persons)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class PersonAPI(TypeAPI[Person, PersonApply, PersonList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Person,
            class_apply_type=PersonApply,
            class_list=PersonList,
        )
        self.view_id = view_id
        self.roles = PersonRolesAPI(client)

    def apply(self, person: PersonApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = person.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(PersonApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(PersonApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Person:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> PersonList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Person | PersonList:
        if isinstance(external_id, str):
            person = self._retrieve((self.sources.space, external_id))

            role_edges = self.roles.retrieve(external_id)
            person.roles = [edge.end_node.external_id for edge in role_edges]

            return person
        else:
            persons = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            role_edges = self.roles.retrieve(external_id)
            self._set_roles(persons, role_edges)

            return persons

    def list(
        self,
        min_birth_year: int | None = None,
        max_birth_year: int | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> PersonList:
        filters = []
        if min_birth_year or max_birth_year:
            filters.append(
                dm.filters.Range(
                    self.view_id.as_property_ref("birthYear"),
                    gte=min_birth_year,
                    lte=max_birth_year,
                )
            )
        if name and isinstance(name, list):
            filters.append(dm.filters.In(self.view_id.as_property_ref("name"), values=name))
        if name and isinstance(name, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("name"), value=name))
        if name_prefix:
            filters.append(dm.filters.Prefix(self.view_id.as_property_ref("name"), value=name_prefix))
        if external_id_prefix:
            filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
        if filter:
            filters.append(filter)

        persons = self._list(limit=limit, filter=dm.filters.And(*filters) if filters else None)

        if retrieve_edges:
            role_edges = self.roles.list(persons.as_external_ids(), limit=-1)
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
