from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells_pydantic_v1.client.data_classes import (
    TechnicalAssurances,
    TechnicalAssurancesApply,
    TechnicalAssurancesList,
    TechnicalAssurancesApplyList,
    TechnicalAssurancesFields,
    TechnicalAssurancesTextFields,
)
from osdu_wells_pydantic_v1.client.data_classes._technical_assurances import _TECHNICALASSURANCES_PROPERTIES_BY_FIELD


class TechnicalAssurancesAcceptableUsageAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "TechnicalAssurances.AcceptableUsage"},
        )
        if isinstance(external_id, str):
            is_technical_assurance = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_technical_assurance)
            )

        else:
            is_technical_assurances = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_technical_assurances)
            )

    def list(
        self,
        technical_assurance_id: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space="IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "TechnicalAssurances.AcceptableUsage"},
        )
        filters.append(is_edge_type)
        if technical_assurance_id:
            technical_assurance_ids = (
                [technical_assurance_id] if isinstance(technical_assurance_id, str) else technical_assurance_id
            )
            is_technical_assurances = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in technical_assurance_ids],
            )
            filters.append(is_technical_assurances)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class TechnicalAssurancesReviewersAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "TechnicalAssurances.Reviewers"},
        )
        if isinstance(external_id, str):
            is_technical_assurance = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_technical_assurance)
            )

        else:
            is_technical_assurances = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_technical_assurances)
            )

    def list(
        self,
        technical_assurance_id: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space="IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "TechnicalAssurances.Reviewers"},
        )
        filters.append(is_edge_type)
        if technical_assurance_id:
            technical_assurance_ids = (
                [technical_assurance_id] if isinstance(technical_assurance_id, str) else technical_assurance_id
            )
            is_technical_assurances = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in technical_assurance_ids],
            )
            filters.append(is_technical_assurances)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class TechnicalAssurancesUnacceptableUsageAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="IntegrationTestsImmutable") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "TechnicalAssurances.UnacceptableUsage"},
        )
        if isinstance(external_id, str):
            is_technical_assurance = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_technical_assurance)
            )

        else:
            is_technical_assurances = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_technical_assurances)
            )

    def list(
        self,
        technical_assurance_id: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space="IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "TechnicalAssurances.UnacceptableUsage"},
        )
        filters.append(is_edge_type)
        if technical_assurance_id:
            technical_assurance_ids = (
                [technical_assurance_id] if isinstance(technical_assurance_id, str) else technical_assurance_id
            )
            is_technical_assurances = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in technical_assurance_ids],
            )
            filters.append(is_technical_assurances)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class TechnicalAssurancesAPI(TypeAPI[TechnicalAssurances, TechnicalAssurancesApply, TechnicalAssurancesList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=TechnicalAssurances,
            class_apply_type=TechnicalAssurancesApply,
            class_list=TechnicalAssurancesList,
        )
        self._view_id = view_id
        self.acceptable_usage = TechnicalAssurancesAcceptableUsageAPI(client)
        self.reviewers = TechnicalAssurancesReviewersAPI(client)
        self.unacceptable_usage = TechnicalAssurancesUnacceptableUsageAPI(client)

    def apply(
        self, technical_assurance: TechnicalAssurancesApply | Sequence[TechnicalAssurancesApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(technical_assurance, TechnicalAssurancesApply):
            instances = technical_assurance.to_instances_apply(self._view_id)
        else:
            instances = TechnicalAssurancesApplyList(technical_assurance).to_instances_apply(self._view_id)
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
    def retrieve(self, external_id: str) -> TechnicalAssurances:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> TechnicalAssurancesList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> TechnicalAssurances | TechnicalAssurancesList:
        if isinstance(external_id, str):
            technical_assurance = self._retrieve((self._sources.space, external_id))

            acceptable_usage_edges = self.acceptable_usage.retrieve(external_id)
            technical_assurance.acceptable_usage = [edge.end_node.external_id for edge in acceptable_usage_edges]
            reviewer_edges = self.reviewers.retrieve(external_id)
            technical_assurance.reviewers = [edge.end_node.external_id for edge in reviewer_edges]
            unacceptable_usage_edges = self.unacceptable_usage.retrieve(external_id)
            technical_assurance.unacceptable_usage = [edge.end_node.external_id for edge in unacceptable_usage_edges]

            return technical_assurance
        else:
            technical_assurances = self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

            acceptable_usage_edges = self.acceptable_usage.retrieve(external_id)
            self._set_acceptable_usage(technical_assurances, acceptable_usage_edges)
            reviewer_edges = self.reviewers.retrieve(external_id)
            self._set_reviewers(technical_assurances, reviewer_edges)
            unacceptable_usage_edges = self.unacceptable_usage.retrieve(external_id)
            self._set_unacceptable_usage(technical_assurances, unacceptable_usage_edges)

            return technical_assurances

    def search(
        self,
        query: str,
        properties: TechnicalAssurancesTextFields | Sequence[TechnicalAssurancesTextFields] | None = None,
        comment: str | list[str] | None = None,
        comment_prefix: str | None = None,
        effective_date: str | list[str] | None = None,
        effective_date_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TechnicalAssurancesList:
        filter_ = _create_filter(
            self._view_id,
            comment,
            comment_prefix,
            effective_date,
            effective_date_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _TECHNICALASSURANCES_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: TechnicalAssurancesFields | Sequence[TechnicalAssurancesFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: TechnicalAssurancesTextFields | Sequence[TechnicalAssurancesTextFields] | None = None,
        comment: str | list[str] | None = None,
        comment_prefix: str | None = None,
        effective_date: str | list[str] | None = None,
        effective_date_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
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
        property: TechnicalAssurancesFields | Sequence[TechnicalAssurancesFields] | None = None,
        group_by: TechnicalAssurancesFields | Sequence[TechnicalAssurancesFields] = None,
        query: str | None = None,
        search_properties: TechnicalAssurancesTextFields | Sequence[TechnicalAssurancesTextFields] | None = None,
        comment: str | list[str] | None = None,
        comment_prefix: str | None = None,
        effective_date: str | list[str] | None = None,
        effective_date_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
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
        property: TechnicalAssurancesFields | Sequence[TechnicalAssurancesFields] | None = None,
        group_by: TechnicalAssurancesFields | Sequence[TechnicalAssurancesFields] | None = None,
        query: str | None = None,
        search_property: TechnicalAssurancesTextFields | Sequence[TechnicalAssurancesTextFields] | None = None,
        comment: str | list[str] | None = None,
        comment_prefix: str | None = None,
        effective_date: str | list[str] | None = None,
        effective_date_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            comment,
            comment_prefix,
            effective_date,
            effective_date_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _TECHNICALASSURANCES_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: TechnicalAssurancesFields,
        interval: float,
        query: str | None = None,
        search_property: TechnicalAssurancesTextFields | Sequence[TechnicalAssurancesTextFields] | None = None,
        comment: str | list[str] | None = None,
        comment_prefix: str | None = None,
        effective_date: str | list[str] | None = None,
        effective_date_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            comment,
            comment_prefix,
            effective_date,
            effective_date_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _TECHNICALASSURANCES_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        comment: str | list[str] | None = None,
        comment_prefix: str | None = None,
        effective_date: str | list[str] | None = None,
        effective_date_prefix: str | None = None,
        technical_assurance_type_id: str | list[str] | None = None,
        technical_assurance_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> TechnicalAssurancesList:
        filter_ = _create_filter(
            self._view_id,
            comment,
            comment_prefix,
            effective_date,
            effective_date_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            external_id_prefix,
            filter,
        )

        technical_assurances = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := technical_assurances.as_external_ids()) > IN_FILTER_LIMIT:
                acceptable_usage_edges = self.acceptable_usage.list(limit=-1)
            else:
                acceptable_usage_edges = self.acceptable_usage.list(external_ids, limit=-1)
            self._set_acceptable_usage(technical_assurances, acceptable_usage_edges)
            if len(external_ids := technical_assurances.as_external_ids()) > IN_FILTER_LIMIT:
                reviewer_edges = self.reviewers.list(limit=-1)
            else:
                reviewer_edges = self.reviewers.list(external_ids, limit=-1)
            self._set_reviewers(technical_assurances, reviewer_edges)
            if len(external_ids := technical_assurances.as_external_ids()) > IN_FILTER_LIMIT:
                unacceptable_usage_edges = self.unacceptable_usage.list(limit=-1)
            else:
                unacceptable_usage_edges = self.unacceptable_usage.list(external_ids, limit=-1)
            self._set_unacceptable_usage(technical_assurances, unacceptable_usage_edges)

        return technical_assurances

    @staticmethod
    def _set_acceptable_usage(
        technical_assurances: Sequence[TechnicalAssurances], acceptable_usage_edges: Sequence[dm.Edge]
    ):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in acceptable_usage_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for technical_assurance in technical_assurances:
            node_id = technical_assurance.id_tuple()
            if node_id in edges_by_start_node:
                technical_assurance.acceptable_usage = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]

    @staticmethod
    def _set_reviewers(technical_assurances: Sequence[TechnicalAssurances], reviewer_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in reviewer_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for technical_assurance in technical_assurances:
            node_id = technical_assurance.id_tuple()
            if node_id in edges_by_start_node:
                technical_assurance.reviewers = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_unacceptable_usage(
        technical_assurances: Sequence[TechnicalAssurances], unacceptable_usage_edges: Sequence[dm.Edge]
    ):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in unacceptable_usage_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for technical_assurance in technical_assurances:
            node_id = technical_assurance.id_tuple()
            if node_id in edges_by_start_node:
                technical_assurance.unacceptable_usage = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]


def _create_filter(
    view_id: dm.ViewId,
    comment: str | list[str] | None = None,
    comment_prefix: str | None = None,
    effective_date: str | list[str] | None = None,
    effective_date_prefix: str | None = None,
    technical_assurance_type_id: str | list[str] | None = None,
    technical_assurance_type_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if comment and isinstance(comment, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Comment"), value=comment))
    if comment and isinstance(comment, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Comment"), values=comment))
    if comment_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Comment"), value=comment_prefix))
    if effective_date and isinstance(effective_date, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("EffectiveDate"), value=effective_date))
    if effective_date and isinstance(effective_date, list):
        filters.append(dm.filters.In(view_id.as_property_ref("EffectiveDate"), values=effective_date))
    if effective_date_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("EffectiveDate"), value=effective_date_prefix))
    if technical_assurance_type_id and isinstance(technical_assurance_type_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("TechnicalAssuranceTypeID"), value=technical_assurance_type_id)
        )
    if technical_assurance_type_id and isinstance(technical_assurance_type_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("TechnicalAssuranceTypeID"), values=technical_assurance_type_id)
        )
    if technical_assurance_type_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("TechnicalAssuranceTypeID"), value=technical_assurance_type_id_prefix
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
