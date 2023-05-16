from __future__ import annotations

from typing import Sequence, Type, overload

from . import MovieClient
from .core_list import T_TypeNode, T_TypeNodeList


class TypeLocal:
    def __init__(self, class_type: Type[T_TypeNode], class_list: Type[T_TypeNodeList], data: list[T_TypeNode] = None):
        self.class_type = class_type
        self.class_list = class_list
        self._data = data

    def list(self, propagation_limit: int = 0, limit: int = 25) -> T_TypeNodeList:
        return self.class_list(self._data)

    def apply(self, node: T_TypeNode, propagation_limit: int = 0):
        ...

    @overload
    def retrieve(self, external_id: str, propagation_limit: int = 0) -> T_TypeNode:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str], propagation_limit: int = 0) -> T_TypeNodeList:
        ...

    def retrieve(self, external_id: str | Sequence[str], propagation_limit: int = 0) -> T_TypeNode | T_TypeNodeList:
        is_singular = isinstance(external_id, str)
        id_set = {external_id} if is_singular else set(external_id)
        selected_nodes = [node for node in self._data if node.externalId in id_set]
        return selected_nodes[0] if is_singular else self.class_list(selected_nodes)

    def delete(self, node_external_id: str | T_TypeNode | T_TypeNodeList, propagation_limit: int = 0):
        ...


class NominationAPI:
    def __init__(self, data, client: MovieClient):
        self.best_directors = TypeLocal(
            client.nominations.best_directors.class_type,
            client.nominations.best_directors.class_list,
            data.best_directors,
        )
        self.best_leading_actor = TypeLocal(
            client.nominations.best_leading_actor.class_type,
            client.nominations.best_leading_actor.class_list,
            data.best_leading_actors,
        )

        self.best_leading_actress = TypeLocal(
            client.nominations.best_leading_actress.class_type,
            client.nominations.best_leading_actress.class_list,
            data.best_leading_actress,
        )


class RolesAPI:
    def __init__(self, data, client: MovieClient):
        self.actors = TypeLocal(client.roles.actors.class_type, client.roles.actors.class_list, data.actors)
        self.directors = TypeLocal(client.roles.directors.class_type, client.roles.directors.class_list, data.directors)


class MovieClientLocal:
    def __init__(self, data, client: MovieClient):
        self.movies = TypeLocal(client.movies.class_type, client.movies.class_list, data.movies)
        self.persons = TypeLocal(client.persons.class_type, client.persons.class_list, data.persons)
        self.nominations = NominationAPI(data, client)
        self.roles = RolesAPI(data, client)
        # Todo Add ratings
        self.ratings = TypeLocal(client.ratings.class_type, client.ratings.class_list)
