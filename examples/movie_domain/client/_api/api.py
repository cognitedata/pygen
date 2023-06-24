from __future__ import annotations

from collections import defaultdict
from typing import Sequence, overload

from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from ..data_classes import data_classes, ids, list_data_classes
from .core_api import TypeAPI

#
# class DirectorAPI(TypeAPI):
#     ...
#
#
# class MovieAPI(TypeAPI):
#     ...
#
#
# class ActorsAPI(TypeAPI):
#     ...
#
#
# class BestDirectorAPI(TypeAPI):
#     ...
#
#
# class BestLeadingActorAPI(TypeAPI):
#     ...
#
#
# class BestLeadingActressAPI(TypeAPI):
#     ...
#
#
# class RatingsAPI(TypeAPI):
#     ...


class PersonsAPI(TypeAPI[data_classes.Person, data_classes.PersonApply, list_data_classes.PersonList]):
    @overload
    def retrieve(self, external_id: str) -> data_classes.Person:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> list_data_classes.PersonList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> data_classes.Person | list_data_classes.PersonList:
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
            person.roles = [ids.RoleId.from_direct_relation(edge.end_node) for edge in edges]
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
                    person.roles = [
                        ids.RoleId.from_direct_relation(edge.end_node) for edge in edges_by_start_node[node_id]
                    ]
            return persons

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> list_data_classes.PersonList:
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
                person.roles = [ids.RoleId.from_direct_relation(edge.end_node) for edge in edges_by_start_node[node_id]]

        return persons
