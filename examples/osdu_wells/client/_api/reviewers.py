from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    Reviewers,
    ReviewersApply,
    ReviewersFields,
    ReviewersList,
    ReviewersTextFields,
)
from osdu_wells.client.data_classes._reviewers import (
    _REVIEWERS_PROPERTIES_BY_FIELD,
    _create_reviewer_filter,
)
from ._core import DEFAULT_LIMIT_READ, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .reviewers_query import ReviewersQueryAPI


class ReviewersAPI(NodeAPI[Reviewers, ReviewersApply, ReviewersList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[ReviewersApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Reviewers,
            class_apply_type=ReviewersApply,
            class_list=ReviewersList,
            class_apply_list=ReviewersApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
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
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ReviewersQueryAPI[ReviewersList]:
        """Query starting at reviewers.

        Args:
            data_governance_role_type_id: The data governance role type id to filter on.
            data_governance_role_type_id_prefix: The prefix of the data governance role type id to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            organisation_id: The organisation id to filter on.
            organisation_id_prefix: The prefix of the organisation id to filter on.
            role_type_id: The role type id to filter on.
            role_type_id_prefix: The prefix of the role type id to filter on.
            workflow_persona_type_id: The workflow persona type id to filter on.
            workflow_persona_type_id_prefix: The prefix of the workflow persona type id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of reviewers to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for reviewers.

        """
        filter_ = _create_reviewer_filter(
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
            space,
            filter,
        )
        builder = QueryBuilder(
            ReviewersList,
            [
                QueryStep(
                    name="reviewer",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_REVIEWERS_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=Reviewers,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return ReviewersQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(self, reviewer: ReviewersApply | Sequence[ReviewersApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) reviewers.

        Args:
            reviewer: Reviewer or sequence of reviewers to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new reviewer:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import ReviewersApply
                >>> client = OSDUClient()
                >>> reviewer = ReviewersApply(external_id="my_reviewer", ...)
                >>> result = client.reviewers.apply(reviewer)

        """
        return self._apply(reviewer, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more reviewer.

        Args:
            external_id: External id of the reviewer to delete.
            space: The space where all the reviewer are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete reviewer by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.reviewers.delete("my_reviewer")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> Reviewers:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> ReviewersList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> Reviewers | ReviewersList:
        """Retrieve one or more reviewers by id(s).

        Args:
            external_id: External id or list of external ids of the reviewers.
            space: The space where all the reviewers are located.

        Returns:
            The requested reviewers.

        Examples:

            Retrieve reviewer by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> reviewer = client.reviewers.retrieve("my_reviewer")

        """
        return self._retrieve(external_id, space)

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
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ReviewersList:
        """Search reviewers

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            data_governance_role_type_id: The data governance role type id to filter on.
            data_governance_role_type_id_prefix: The prefix of the data governance role type id to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            organisation_id: The organisation id to filter on.
            organisation_id_prefix: The prefix of the organisation id to filter on.
            role_type_id: The role type id to filter on.
            role_type_id_prefix: The prefix of the role type id to filter on.
            workflow_persona_type_id: The workflow persona type id to filter on.
            workflow_persona_type_id_prefix: The prefix of the workflow persona type id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of reviewers to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results reviewers matching the query.

        Examples:

           Search for 'my_reviewer' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> reviewers = client.reviewers.search('my_reviewer')

        """
        filter_ = _create_reviewer_filter(
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
            space,
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
        space: str | list[str] | None = None,
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
        space: str | list[str] | None = None,
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
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across reviewers

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            data_governance_role_type_id: The data governance role type id to filter on.
            data_governance_role_type_id_prefix: The prefix of the data governance role type id to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            organisation_id: The organisation id to filter on.
            organisation_id_prefix: The prefix of the organisation id to filter on.
            role_type_id: The role type id to filter on.
            role_type_id_prefix: The prefix of the role type id to filter on.
            workflow_persona_type_id: The workflow persona type id to filter on.
            workflow_persona_type_id_prefix: The prefix of the workflow persona type id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of reviewers to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count reviewers in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.reviewers.aggregate("count", space="my_space")

        """

        filter_ = _create_reviewer_filter(
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
            space,
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
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for reviewers

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            data_governance_role_type_id: The data governance role type id to filter on.
            data_governance_role_type_id_prefix: The prefix of the data governance role type id to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            organisation_id: The organisation id to filter on.
            organisation_id_prefix: The prefix of the organisation id to filter on.
            role_type_id: The role type id to filter on.
            role_type_id_prefix: The prefix of the role type id to filter on.
            workflow_persona_type_id: The workflow persona type id to filter on.
            workflow_persona_type_id_prefix: The prefix of the workflow persona type id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of reviewers to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_reviewer_filter(
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
            space,
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
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ReviewersList:
        """List/filter reviewers

        Args:
            data_governance_role_type_id: The data governance role type id to filter on.
            data_governance_role_type_id_prefix: The prefix of the data governance role type id to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            organisation_id: The organisation id to filter on.
            organisation_id_prefix: The prefix of the organisation id to filter on.
            role_type_id: The role type id to filter on.
            role_type_id_prefix: The prefix of the role type id to filter on.
            workflow_persona_type_id: The workflow persona type id to filter on.
            workflow_persona_type_id_prefix: The prefix of the workflow persona type id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of reviewers to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested reviewers

        Examples:

            List reviewers and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> reviewers = client.reviewers.list(limit=5)

        """
        filter_ = _create_reviewer_filter(
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
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
