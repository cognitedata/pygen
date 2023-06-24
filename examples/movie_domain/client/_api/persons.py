from __future__ import annotations

from collections import defaultdict
from typing import Sequence, overload

from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from ..data_classes.ids import RoleId
from ..data_classes.persons import Person, PersonApply, PersonList
from ._core import TypeAPI


class PersonsAPI(TypeAPI[Person, PersonApply, PersonList]):
    def apply(self, person: PersonApply, replace: bool = False) -> dm.InstancesApplyResult:
        return self._client.data_modeling.instances.apply(nodes=person.to_node(), replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(PersonApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(nodes=[(PersonApply.space, id) for id in external_id])

    @overload
    def retrieve(self, external_id: str) -> Person:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> PersonList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Person | PersonList:
        f = dm.filters
        if isinstance(external_id, str):
            person = self._retrieve(("IntegrationTestsImmutable", external_id))
            is_edge_type = f.Equals(
                ["edge", "type"], {"space": "IntegrationTestsImmutable", "externalId": "Person.roles"}
            )
            is_person = f.Equals(
                ["edge", "startNode"], {"space": "IntegrationTestsImmutable", "externalId": external_id}
            )
            edges = self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_person))
            person.roles = [RoleId.from_direct_relation(edge.end_node) for edge in edges]
            return person
        else:
            persons = self._retrieve([("IntegrationTestsImmutable", id) for id in external_id])
            is_edge_type = f.Equals(
                ["edge", "type"], {"space": "IntegrationTestsImmutable", "externalId": "Person.roles"}
            )
            is_persons = f.In(
                ["edge", "startNode"], [{"space": "IntegrationTestsImmutable", "externalId": id} for id in external_id]
            )
            edges = self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_persons))
            edges_by_start_node = defaultdict(list)
            for edge in edges:
                edges_by_start_node[edge.start_node.as_tuple()].append(edge)
            for person in persons:
                node_id = person.id_tuple()
                if node_id in edges_by_start_node:
                    person.roles = [RoleId.from_direct_relation(edge.end_node) for edge in edges_by_start_node[node_id]]
            return persons

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> PersonList:
        persons = self._list(limit=limit)

        f = dm.filters
        is_edge_type = f.Equals(["edge", "type"], {"space": "IntegrationTestsImmutable", "externalId": "Person.roles"})
        edges = self._client.data_modeling.instances.list("edge", limit=-1, filter=is_edge_type)
        edges_by_start_node = defaultdict(list)
        for edge in edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for person in persons:
            node_id = person.id_tuple()
            if node_id in edges_by_start_node:
                person.roles = [RoleId.from_direct_relation(edge.end_node) for edge in edges_by_start_node[node_id]]

        return persons
