from __future__ import annotations

from typing import Dict, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from shop.client.data_classes import Case, CaseApply, CaseList

from ._core import TypeAPI


class CaseCommandAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Case.commands"},
        )
        if isinstance(external_id, str):
            is_case = f.Equals(
                ["edge", "startNode"],
                {"space": "IntegrationTestsImmutable", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_case))

        else:
            is_cases = f.In(
                ["edge", "startNode"],
                [{"space": "IntegrationTestsImmutable", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_cases))

    def list(self, limit=INSTANCES_LIST_LIMIT_DEFAULT) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "Case.commands"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class CasesAPI(TypeAPI[Case, CaseApply, CaseList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("IntegrationTestsImmutable", "Case", "366b75cc4e699f"),
            class_type=Case,
            class_apply_type=CaseApply,
            class_list=CaseList,
        )
        self.command = CaseCommandAPI(client)

    def apply(self, case: CaseApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = case.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(CaseApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(nodes=[(CaseApply.space, id) for id in external_id])

    @overload
    def retrieve(self, external_id: str) -> Case:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> CaseList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Case | CaseList:
        if isinstance(external_id, str):
            case = self._retrieve(("IntegrationTestsImmutable", external_id))
            command_edges = self.command.retrieve(external_id)
            case.commands = command_edges[0].end_node.external_id if command_edges else None

            return case
        else:
            cases = self._retrieve([("IntegrationTestsImmutable", ext_id) for ext_id in external_id])
            command_edges = self.command.retrieve(external_id)
            self._set_command(cases, command_edges)

            return cases

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> CaseList:
        cases = self._list(limit=limit)

        command_edges = self.command.list(limit=-1)
        self._set_command(cases, command_edges)

        return cases

    @staticmethod
    def _set_commands(cases: Sequence[Case], command_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, dm.Edge] = {edge.start_node.as_tuple(): edge for edge in command_edges}

        for case in cases:
            node_id = case.id_tuple()
            if node_id in edges_by_start_node:
                case.commands = edges_by_start_node[node_id].end_node.external_id
