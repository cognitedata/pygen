from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from movie_domain_pydantic_v1.client._api._core import TypeAPI
from movie_domain_pydantic_v1.client.data_classes import Role, RoleApply, RoleList


class RoleMoviesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Role.movies"},
        )
        if isinstance(external_id, str):
            is_role = f.Equals(
                ["edge", "startNode"],
                {"space": "IntegrationTestsImmutable", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_role))

        else:
            is_roles = f.In(
                ["edge", "startNode"],
                [{"space": "IntegrationTestsImmutable", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_roles))

    def list(self, limit=INSTANCES_LIST_LIMIT_DEFAULT) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Role.movies"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class RoleNominationsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Role.nomination"},
        )
        if isinstance(external_id, str):
            is_role = f.Equals(
                ["edge", "startNode"],
                {"space": "IntegrationTestsImmutable", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_role))

        else:
            is_roles = f.In(
                ["edge", "startNode"],
                [{"space": "IntegrationTestsImmutable", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_roles))

    def list(self, limit=INSTANCES_LIST_LIMIT_DEFAULT) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Role.nomination"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class RolesAPI(TypeAPI[Role, RoleApply, RoleList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("IntegrationTestsImmutable", "Role", "2"),
            class_type=Role,
            class_apply_type=RoleApply,
            class_list=RoleList,
        )
        self.movies = RoleMoviesAPI(client)
        self.nominations = RoleNominationsAPI(client)

    def apply(self, role: RoleApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = role.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(RoleApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(RoleApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Role:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> RoleList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Role | RoleList:
        if isinstance(external_id, str):
            role = self._retrieve((self.sources.space, external_id))

            movie_edges = self.movies.retrieve(external_id)
            role.movies = [edge.end_node.external_id for edge in movie_edges]
            nomination_edges = self.nominations.retrieve(external_id)
            role.nomination = [edge.end_node.external_id for edge in nomination_edges]

            return role
        else:
            roles = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            movie_edges = self.movies.retrieve(external_id)
            self._set_movies(roles, movie_edges)
            nomination_edges = self.nominations.retrieve(external_id)
            self._set_nomination(roles, nomination_edges)

            return roles

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> RoleList:
        roles = self._list(limit=limit)

        movie_edges = self.movies.list(limit=-1)
        self._set_movies(roles, movie_edges)
        nomination_edges = self.nominations.list(limit=-1)
        self._set_nomination(roles, nomination_edges)

        return roles

    @staticmethod
    def _set_movies(roles: Sequence[Role], movie_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in movie_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for role in roles:
            node_id = role.id_tuple()
            if node_id in edges_by_start_node:
                role.movies = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_nomination(roles: Sequence[Role], nomination_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in nomination_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for role in roles:
            node_id = role.id_tuple()
            if node_id in edges_by_start_node:
                role.nomination = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]
