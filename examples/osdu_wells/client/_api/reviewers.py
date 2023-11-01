from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    Reviewers,
    ReviewersApply,
    ReviewersList,
    ReviewersApplyList,
    ReviewersFields,
    ReviewersTextFields,
)
from osdu_wells.client.data_classes._reviewers import _REVIEWERS_PROPERTIES_BY_FIELD


class ReviewersAPI(TypeAPI[Reviewers, ReviewersApply, ReviewersList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Reviewers,
            class_apply_type=ReviewersApply,
            class_list=ReviewersList,
        )
        self._view_id = view_id

    def apply(
        self, reviewer: ReviewersApply | Sequence[ReviewersApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(reviewer, ReviewersApply):
            instances = reviewer.to_instances_apply(self._view_id)
        else:
            instances = ReviewersApplyList(reviewer).to_instances_apply(self._view_id)
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
    def retrieve(self, external_id: str) -> Reviewers:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ReviewersList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Reviewers | ReviewersList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: ReviewersTextFields | Sequence[ReviewersTextFields] | None = None,
        data_governance_role_type_id: str | list[str] | None = None,
        data_governance_role_type_id_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        organisation_id: str | list[str] | None = None,
        organisation_id_prefix: str | None = None,
        role_type_id: str | list[str] | None = None,
        role_type_id_prefix: str | None = None,
        workflow_persona_type_id: str | list[str] | None = None,
        workflow_persona_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ReviewersList:
        filter_ = _create_filter(
            self._view_id,
            data_governance_role_type_id,
            data_governance_role_type_id_prefix,
            name,
            name_prefix,
            organisation_id,
            organisation_id_prefix,
            role_type_id,
            role_type_id_prefix,
            workflow_persona_type_id,
            workflow_persona_type_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _REVIEWERS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ReviewersFields | Sequence[ReviewersFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: ReviewersTextFields | Sequence[ReviewersTextFields] | None = None,
        data_governance_role_type_id: str | list[str] | None = None,
        data_governance_role_type_id_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        organisation_id: str | list[str] | None = None,
        organisation_id_prefix: str | None = None,
        role_type_id: str | list[str] | None = None,
        role_type_id_prefix: str | None = None,
        workflow_persona_type_id: str | list[str] | None = None,
        workflow_persona_type_id_prefix: str | None = None,
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
        property: ReviewersFields | Sequence[ReviewersFields] | None = None,
        group_by: ReviewersFields | Sequence[ReviewersFields] = None,
        query: str | None = None,
        search_properties: ReviewersTextFields | Sequence[ReviewersTextFields] | None = None,
        data_governance_role_type_id: str | list[str] | None = None,
        data_governance_role_type_id_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        organisation_id: str | list[str] | None = None,
        organisation_id_prefix: str | None = None,
        role_type_id: str | list[str] | None = None,
        role_type_id_prefix: str | None = None,
        workflow_persona_type_id: str | list[str] | None = None,
        workflow_persona_type_id_prefix: str | None = None,
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
        property: ReviewersFields | Sequence[ReviewersFields] | None = None,
        group_by: ReviewersFields | Sequence[ReviewersFields] | None = None,
        query: str | None = None,
        search_property: ReviewersTextFields | Sequence[ReviewersTextFields] | None = None,
        data_governance_role_type_id: str | list[str] | None = None,
        data_governance_role_type_id_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        organisation_id: str | list[str] | None = None,
        organisation_id_prefix: str | None = None,
        role_type_id: str | list[str] | None = None,
        role_type_id_prefix: str | None = None,
        workflow_persona_type_id: str | list[str] | None = None,
        workflow_persona_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            data_governance_role_type_id,
            data_governance_role_type_id_prefix,
            name,
            name_prefix,
            organisation_id,
            organisation_id_prefix,
            role_type_id,
            role_type_id_prefix,
            workflow_persona_type_id,
            workflow_persona_type_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _REVIEWERS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ReviewersFields,
        interval: float,
        query: str | None = None,
        search_property: ReviewersTextFields | Sequence[ReviewersTextFields] | None = None,
        data_governance_role_type_id: str | list[str] | None = None,
        data_governance_role_type_id_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        organisation_id: str | list[str] | None = None,
        organisation_id_prefix: str | None = None,
        role_type_id: str | list[str] | None = None,
        role_type_id_prefix: str | None = None,
        workflow_persona_type_id: str | list[str] | None = None,
        workflow_persona_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            data_governance_role_type_id,
            data_governance_role_type_id_prefix,
            name,
            name_prefix,
            organisation_id,
            organisation_id_prefix,
            role_type_id,
            role_type_id_prefix,
            workflow_persona_type_id,
            workflow_persona_type_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _REVIEWERS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        data_governance_role_type_id: str | list[str] | None = None,
        data_governance_role_type_id_prefix: str | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        organisation_id: str | list[str] | None = None,
        organisation_id_prefix: str | None = None,
        role_type_id: str | list[str] | None = None,
        role_type_id_prefix: str | None = None,
        workflow_persona_type_id: str | list[str] | None = None,
        workflow_persona_type_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ReviewersList:
        filter_ = _create_filter(
            self._view_id,
            data_governance_role_type_id,
            data_governance_role_type_id_prefix,
            name,
            name_prefix,
            organisation_id,
            organisation_id_prefix,
            role_type_id,
            role_type_id_prefix,
            workflow_persona_type_id,
            workflow_persona_type_id_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    data_governance_role_type_id: str | list[str] | None = None,
    data_governance_role_type_id_prefix: str | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    organisation_id: str | list[str] | None = None,
    organisation_id_prefix: str | None = None,
    role_type_id: str | list[str] | None = None,
    role_type_id_prefix: str | None = None,
    workflow_persona_type_id: str | list[str] | None = None,
    workflow_persona_type_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if data_governance_role_type_id and isinstance(data_governance_role_type_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("DataGovernanceRoleTypeID"), value=data_governance_role_type_id)
        )
    if data_governance_role_type_id and isinstance(data_governance_role_type_id, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("DataGovernanceRoleTypeID"), values=data_governance_role_type_id)
        )
    if data_governance_role_type_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("DataGovernanceRoleTypeID"), value=data_governance_role_type_id_prefix
            )
        )
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Name"), value=name_prefix))
    if organisation_id and isinstance(organisation_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("OrganisationID"), value=organisation_id))
    if organisation_id and isinstance(organisation_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("OrganisationID"), values=organisation_id))
    if organisation_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("OrganisationID"), value=organisation_id_prefix))
    if role_type_id and isinstance(role_type_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("RoleTypeID"), value=role_type_id))
    if role_type_id and isinstance(role_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("RoleTypeID"), values=role_type_id))
    if role_type_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("RoleTypeID"), value=role_type_id_prefix))
    if workflow_persona_type_id and isinstance(workflow_persona_type_id, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("WorkflowPersonaTypeID"), value=workflow_persona_type_id)
        )
    if workflow_persona_type_id and isinstance(workflow_persona_type_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("WorkflowPersonaTypeID"), values=workflow_persona_type_id))
    if workflow_persona_type_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("WorkflowPersonaTypeID"), value=workflow_persona_type_id_prefix)
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
