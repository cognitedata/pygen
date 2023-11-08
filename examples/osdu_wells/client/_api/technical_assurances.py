from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from osdu_wells.client.data_classes import (
    TechnicalAssurances,
    TechnicalAssurancesApply,
    TechnicalAssurancesList,
    TechnicalAssurancesApplyList,
    TechnicalAssurancesFields,
    TechnicalAssurancesTextFields,
    DomainModelApply,
)
from osdu_wells.client.data_classes._technical_assurances import _TECHNICALASSURANCES_PROPERTIES_BY_FIELD


class TechnicalAssurancesAcceptableUsageAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """Retrieve one or more acceptable_usage edges by id(s) of a technical assurance.

        Args:
            external_id: External id or list of external ids source technical assurance.
            space: The space where all the acceptable usage edges are located.

        Returns:
            The requested acceptable usage edges.

        Examples:

            Retrieve acceptable_usage edge by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> technical_assurance = client.technical_assurances.acceptable_usage.retrieve("my_acceptable_usage")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "TechnicalAssurances.AcceptableUsage"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_technical_assurances = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_technical_assurances = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list(
            "edge", limit=-1, filter=f.And(is_edge_type, is_technical_assurances)
        )

    def list(
        self,
        technical_assurance_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List acceptable_usage edges of a technical assurance.

        Args:
            technical_assurance_id: ID of the source technical assurance.
            limit: Maximum number of acceptable usage edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the acceptable usage edges are located.

        Returns:
            The requested acceptable usage edges.

        Examples:

            List 5 acceptable_usage edges connected to "my_technical_assurance":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> technical_assurance = client.technical_assurances.acceptable_usage.list("my_technical_assurance", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "TechnicalAssurances.AcceptableUsage"},
            )
        ]
        if technical_assurance_id:
            technical_assurance_ids = (
                technical_assurance_id if isinstance(technical_assurance_id, list) else [technical_assurance_id]
            )
            is_technical_assurances = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in technical_assurance_ids
                ],
            )
            filters.append(is_technical_assurances)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class TechnicalAssurancesReviewersAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """Retrieve one or more reviewers edges by id(s) of a technical assurance.

        Args:
            external_id: External id or list of external ids source technical assurance.
            space: The space where all the reviewer edges are located.

        Returns:
            The requested reviewer edges.

        Examples:

            Retrieve reviewers edge by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> technical_assurance = client.technical_assurances.reviewers.retrieve("my_reviewers")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "TechnicalAssurances.Reviewers"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_technical_assurances = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_technical_assurances = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list(
            "edge", limit=-1, filter=f.And(is_edge_type, is_technical_assurances)
        )

    def list(
        self,
        technical_assurance_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List reviewers edges of a technical assurance.

        Args:
            technical_assurance_id: ID of the source technical assurance.
            limit: Maximum number of reviewer edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the reviewer edges are located.

        Returns:
            The requested reviewer edges.

        Examples:

            List 5 reviewers edges connected to "my_technical_assurance":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> technical_assurance = client.technical_assurances.reviewers.list("my_technical_assurance", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "TechnicalAssurances.Reviewers"},
            )
        ]
        if technical_assurance_id:
            technical_assurance_ids = (
                technical_assurance_id if isinstance(technical_assurance_id, list) else [technical_assurance_id]
            )
            is_technical_assurances = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in technical_assurance_ids
                ],
            )
            filters.append(is_technical_assurances)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class TechnicalAssurancesUnacceptableUsageAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self, external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId], space: str = "IntegrationTestsImmutable"
    ) -> dm.EdgeList:
        """Retrieve one or more unacceptable_usage edges by id(s) of a technical assurance.

        Args:
            external_id: External id or list of external ids source technical assurance.
            space: The space where all the unacceptable usage edges are located.

        Returns:
            The requested unacceptable usage edges.

        Examples:

            Retrieve unacceptable_usage edge by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> technical_assurance = client.technical_assurances.unacceptable_usage.retrieve("my_unacceptable_usage")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "IntegrationTestsImmutable", "externalId": "TechnicalAssurances.UnacceptableUsage"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_technical_assurances = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_technical_assurances = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list(
            "edge", limit=-1, filter=f.And(is_edge_type, is_technical_assurances)
        )

    def list(
        self,
        technical_assurance_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "IntegrationTestsImmutable",
    ) -> dm.EdgeList:
        """List unacceptable_usage edges of a technical assurance.

        Args:
            technical_assurance_id: ID of the source technical assurance.
            limit: Maximum number of unacceptable usage edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the unacceptable usage edges are located.

        Returns:
            The requested unacceptable usage edges.

        Examples:

            List 5 unacceptable_usage edges connected to "my_technical_assurance":

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> technical_assurance = client.technical_assurances.unacceptable_usage.list("my_technical_assurance", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "IntegrationTestsImmutable", "externalId": "TechnicalAssurances.UnacceptableUsage"},
            )
        ]
        if technical_assurance_id:
            technical_assurance_ids = (
                technical_assurance_id if isinstance(technical_assurance_id, list) else [technical_assurance_id]
            )
            is_technical_assurances = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in technical_assurance_ids
                ],
            )
            filters.append(is_technical_assurances)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class TechnicalAssurancesAPI(TypeAPI[TechnicalAssurances, TechnicalAssurancesApply, TechnicalAssurancesList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[TechnicalAssurancesApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=TechnicalAssurances,
            class_apply_type=TechnicalAssurancesApply,
            class_list=TechnicalAssurancesList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.acceptable_usage = TechnicalAssurancesAcceptableUsageAPI(client)
        self.reviewers = TechnicalAssurancesReviewersAPI(client)
        self.unacceptable_usage = TechnicalAssurancesUnacceptableUsageAPI(client)

    def apply(
        self, technical_assurance: TechnicalAssurancesApply | Sequence[TechnicalAssurancesApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) technical assurances.

        Note: This method iterates through all nodes linked to technical_assurance and create them including the edges
        between the nodes. For example, if any of `acceptable_usage`, `reviewers` or `unacceptable_usage` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            technical_assurance: Technical assurance or sequence of technical assurances to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new technical_assurance:

                >>> from osdu_wells.client import OSDUClient
                >>> from osdu_wells.client.data_classes import TechnicalAssurancesApply
                >>> client = OSDUClient()
                >>> technical_assurance = TechnicalAssurancesApply(external_id="my_technical_assurance", ...)
                >>> result = client.technical_assurances.apply(technical_assurance)

        """
        if isinstance(technical_assurance, TechnicalAssurancesApply):
            instances = technical_assurance.to_instances_apply(self._view_by_write_class)
        else:
            instances = TechnicalAssurancesApplyList(technical_assurance).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more technical assurance.

        Args:
            external_id: External id of the technical assurance to delete.
            space: The space where all the technical assurance are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete technical_assurance by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.technical_assurances.delete("my_technical_assurance")
        """
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

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> TechnicalAssurances | TechnicalAssurancesList:
        """Retrieve one or more technical assurances by id(s).

        Args:
            external_id: External id or list of external ids of the technical assurances.
            space: The space where all the technical assurances are located.

        Returns:
            The requested technical assurances.

        Examples:

            Retrieve technical_assurance by id:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> technical_assurance = client.technical_assurances.retrieve("my_technical_assurance")

        """
        if isinstance(external_id, str):
            technical_assurance = self._retrieve((space, external_id))

            acceptable_usage_edges = self.acceptable_usage.retrieve(external_id, space=space)
            technical_assurance.acceptable_usage = [edge.end_node.external_id for edge in acceptable_usage_edges]
            reviewer_edges = self.reviewers.retrieve(external_id, space=space)
            technical_assurance.reviewers = [edge.end_node.external_id for edge in reviewer_edges]
            unacceptable_usage_edges = self.unacceptable_usage.retrieve(external_id, space=space)
            technical_assurance.unacceptable_usage = [edge.end_node.external_id for edge in unacceptable_usage_edges]

            return technical_assurance
        else:
            technical_assurances = self._retrieve([(space, ext_id) for ext_id in external_id])

            acceptable_usage_edges = self.acceptable_usage.retrieve(technical_assurances.as_node_ids())
            self._set_acceptable_usage(technical_assurances, acceptable_usage_edges)
            reviewer_edges = self.reviewers.retrieve(technical_assurances.as_node_ids())
            self._set_reviewers(technical_assurances, reviewer_edges)
            unacceptable_usage_edges = self.unacceptable_usage.retrieve(technical_assurances.as_node_ids())
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
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TechnicalAssurancesList:
        """Search technical assurances

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            comment: The comment to filter on.
            comment_prefix: The prefix of the comment to filter on.
            effective_date: The effective date to filter on.
            effective_date_prefix: The prefix of the effective date to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of technical assurances to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `acceptable_usage`, `reviewers` or `unacceptable_usage` external ids for the technical assurances. Defaults to True.

        Returns:
            Search results technical assurances matching the query.

        Examples:

           Search for 'my_technical_assurance' in all text properties:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> technical_assurances = client.technical_assurances.search('my_technical_assurance')

        """
        filter_ = _create_filter(
            self._view_id,
            comment,
            comment_prefix,
            effective_date,
            effective_date_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            external_id_prefix,
            space,
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
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across technical assurances

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            comment: The comment to filter on.
            comment_prefix: The prefix of the comment to filter on.
            effective_date: The effective date to filter on.
            effective_date_prefix: The prefix of the effective date to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of technical assurances to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `acceptable_usage`, `reviewers` or `unacceptable_usage` external ids for the technical assurances. Defaults to True.

        Returns:
            Aggregation results.

        Examples:

            Count technical assurances in space `my_space`:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.technical_assurances.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            comment,
            comment_prefix,
            effective_date,
            effective_date_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            external_id_prefix,
            space,
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
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for technical assurances

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            comment: The comment to filter on.
            comment_prefix: The prefix of the comment to filter on.
            effective_date: The effective date to filter on.
            effective_date_prefix: The prefix of the effective date to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of technical assurances to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `acceptable_usage`, `reviewers` or `unacceptable_usage` external ids for the technical assurances. Defaults to True.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
            comment,
            comment_prefix,
            effective_date,
            effective_date_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            external_id_prefix,
            space,
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
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> TechnicalAssurancesList:
        """List/filter technical assurances

        Args:
            comment: The comment to filter on.
            comment_prefix: The prefix of the comment to filter on.
            effective_date: The effective date to filter on.
            effective_date_prefix: The prefix of the effective date to filter on.
            technical_assurance_type_id: The technical assurance type id to filter on.
            technical_assurance_type_id_prefix: The prefix of the technical assurance type id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of technical assurances to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `acceptable_usage`, `reviewers` or `unacceptable_usage` external ids for the technical assurances. Defaults to True.

        Returns:
            List of requested technical assurances

        Examples:

            List technical assurances and limit to 5:

                >>> from osdu_wells.client import OSDUClient
                >>> client = OSDUClient()
                >>> technical_assurances = client.technical_assurances.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            comment,
            comment_prefix,
            effective_date,
            effective_date_prefix,
            technical_assurance_type_id,
            technical_assurance_type_id_prefix,
            external_id_prefix,
            space,
            filter,
        )

        technical_assurances = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            space_arg = {"space": space} if space else {}
            if len(ids := technical_assurances.as_node_ids()) > IN_FILTER_LIMIT:
                acceptable_usage_edges = self.acceptable_usage.list(limit=-1, **space_arg)
            else:
                acceptable_usage_edges = self.acceptable_usage.list(ids, limit=-1)
            self._set_acceptable_usage(technical_assurances, acceptable_usage_edges)
            if len(ids := technical_assurances.as_node_ids()) > IN_FILTER_LIMIT:
                reviewer_edges = self.reviewers.list(limit=-1, **space_arg)
            else:
                reviewer_edges = self.reviewers.list(ids, limit=-1)
            self._set_reviewers(technical_assurances, reviewer_edges)
            if len(ids := technical_assurances.as_node_ids()) > IN_FILTER_LIMIT:
                unacceptable_usage_edges = self.unacceptable_usage.list(limit=-1, **space_arg)
            else:
                unacceptable_usage_edges = self.unacceptable_usage.list(ids, limit=-1)
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
    space: str | list[str] | None = None,
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
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
