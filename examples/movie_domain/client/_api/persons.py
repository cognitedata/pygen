from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from movie_domain.client.data_classes import Person, PersonApply, PersonList

from ._core import TypeAPI


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

    def list(self, limit=INSTANCES_LIST_LIMIT_DEFAULT) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Person.roles"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class PersonsAPI(TypeAPI[Person, PersonApply, PersonList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("IntegrationTestsImmutable", "Person", "2"),
            class_type=Person,
            class_apply_type=PersonApply,
            class_list=PersonList,
        )
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

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> PersonList:
        persons = self._list(limit=limit)

        role_edges = self.roles.list(limit=-1)
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
