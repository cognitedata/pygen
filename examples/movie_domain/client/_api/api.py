from collections import defaultdict

from cognite.client import data_modeling as dm

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
    def list(self, traversal_count: int = 0, limit: int = 25) -> list_data_classes.PersonList:
        persons = self._list(traversal_count=traversal_count, limit=limit)

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
