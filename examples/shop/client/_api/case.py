from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from shop.client.data_classes._core import DEFAULT_INSTANCE_SPACE
from shop.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    Case,
    CaseApply,
    CaseFields,
    CaseList,
    CaseApplyList,
    CaseTextFields,
)
from shop.client.data_classes._case import (
    _CASE_PROPERTIES_BY_FIELD,
    _create_case_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_QUERY_LIMIT,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
    QueryStep,
    QueryBuilder,
)
from .case_query import CaseQueryAPI


class CaseAPI(NodeAPI[Case, CaseApply, CaseList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[CaseApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Case,
            class_apply_type=CaseApply,
            class_list=CaseList,
            class_apply_list=CaseApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        scenario: str | list[str] | None = None,
        scenario_prefix: str | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        run_status: str | list[str] | None = None,
        run_status_prefix: str | None = None,
        arguments: str | list[str] | None = None,
        arguments_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CaseQueryAPI[CaseList]:
        """Query starting at cases.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            scenario: The scenario to filter on.
            scenario_prefix: The prefix of the scenario to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            commands: The command to filter on.
            run_status: The run status to filter on.
            run_status_prefix: The prefix of the run status to filter on.
            arguments: The argument to filter on.
            arguments_prefix: The prefix of the argument to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cases to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for cases.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_case_filter(
            self._view_id,
            name,
            name_prefix,
            scenario,
            scenario_prefix,
            min_start_time,
            max_start_time,
            min_end_time,
            max_end_time,
            commands,
            run_status,
            run_status_prefix,
            arguments,
            arguments_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(CaseList)
        return CaseQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(self, case: CaseApply | Sequence[CaseApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) cases.

        Args:
            case: Case or sequence of cases to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new case:

                >>> from shop.client import ShopClient
                >>> from shop.client.data_classes import CaseApply
                >>> client = ShopClient()
                >>> case = CaseApply(external_id="my_case", ...)
                >>> result = client.case.apply(case)

        """
        return self._apply(case, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more case.

        Args:
            external_id: External id of the case to delete.
            space: The space where all the case are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete case by id:

                >>> from shop.client import ShopClient
                >>> client = ShopClient()
                >>> client.case.delete("my_case")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Case | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> CaseList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Case | CaseList | None:
        """Retrieve one or more cases by id(s).

        Args:
            external_id: External id or list of external ids of the cases.
            space: The space where all the cases are located.

        Returns:
            The requested cases.

        Examples:

            Retrieve case by id:

                >>> from shop.client import ShopClient
                >>> client = ShopClient()
                >>> case = client.case.retrieve("my_case")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: CaseTextFields | Sequence[CaseTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        scenario: str | list[str] | None = None,
        scenario_prefix: str | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        run_status: str | list[str] | None = None,
        run_status_prefix: str | None = None,
        arguments: str | list[str] | None = None,
        arguments_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CaseList:
        """Search cases

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            scenario: The scenario to filter on.
            scenario_prefix: The prefix of the scenario to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            commands: The command to filter on.
            run_status: The run status to filter on.
            run_status_prefix: The prefix of the run status to filter on.
            arguments: The argument to filter on.
            arguments_prefix: The prefix of the argument to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cases to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results cases matching the query.

        Examples:

           Search for 'my_case' in all text properties:

                >>> from shop.client import ShopClient
                >>> client = ShopClient()
                >>> cases = client.case.search('my_case')

        """
        filter_ = _create_case_filter(
            self._view_id,
            name,
            name_prefix,
            scenario,
            scenario_prefix,
            min_start_time,
            max_start_time,
            min_end_time,
            max_end_time,
            commands,
            run_status,
            run_status_prefix,
            arguments,
            arguments_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _CASE_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: CaseFields | Sequence[CaseFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: CaseTextFields | Sequence[CaseTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        scenario: str | list[str] | None = None,
        scenario_prefix: str | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        run_status: str | list[str] | None = None,
        run_status_prefix: str | None = None,
        arguments: str | list[str] | None = None,
        arguments_prefix: str | None = None,
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
        property: CaseFields | Sequence[CaseFields] | None = None,
        group_by: CaseFields | Sequence[CaseFields] = None,
        query: str | None = None,
        search_properties: CaseTextFields | Sequence[CaseTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        scenario: str | list[str] | None = None,
        scenario_prefix: str | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        run_status: str | list[str] | None = None,
        run_status_prefix: str | None = None,
        arguments: str | list[str] | None = None,
        arguments_prefix: str | None = None,
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
        property: CaseFields | Sequence[CaseFields] | None = None,
        group_by: CaseFields | Sequence[CaseFields] | None = None,
        query: str | None = None,
        search_property: CaseTextFields | Sequence[CaseTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        scenario: str | list[str] | None = None,
        scenario_prefix: str | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        run_status: str | list[str] | None = None,
        run_status_prefix: str | None = None,
        arguments: str | list[str] | None = None,
        arguments_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across cases

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            scenario: The scenario to filter on.
            scenario_prefix: The prefix of the scenario to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            commands: The command to filter on.
            run_status: The run status to filter on.
            run_status_prefix: The prefix of the run status to filter on.
            arguments: The argument to filter on.
            arguments_prefix: The prefix of the argument to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cases to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count cases in space `my_space`:

                >>> from shop.client import ShopClient
                >>> client = ShopClient()
                >>> result = client.case.aggregate("count", space="my_space")

        """

        filter_ = _create_case_filter(
            self._view_id,
            name,
            name_prefix,
            scenario,
            scenario_prefix,
            min_start_time,
            max_start_time,
            min_end_time,
            max_end_time,
            commands,
            run_status,
            run_status_prefix,
            arguments,
            arguments_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _CASE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: CaseFields,
        interval: float,
        query: str | None = None,
        search_property: CaseTextFields | Sequence[CaseTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        scenario: str | list[str] | None = None,
        scenario_prefix: str | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        run_status: str | list[str] | None = None,
        run_status_prefix: str | None = None,
        arguments: str | list[str] | None = None,
        arguments_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for cases

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            scenario: The scenario to filter on.
            scenario_prefix: The prefix of the scenario to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            commands: The command to filter on.
            run_status: The run status to filter on.
            run_status_prefix: The prefix of the run status to filter on.
            arguments: The argument to filter on.
            arguments_prefix: The prefix of the argument to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cases to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_case_filter(
            self._view_id,
            name,
            name_prefix,
            scenario,
            scenario_prefix,
            min_start_time,
            max_start_time,
            min_end_time,
            max_end_time,
            commands,
            run_status,
            run_status_prefix,
            arguments,
            arguments_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _CASE_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        scenario: str | list[str] | None = None,
        scenario_prefix: str | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        run_status: str | list[str] | None = None,
        run_status_prefix: str | None = None,
        arguments: str | list[str] | None = None,
        arguments_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CaseList:
        """List/filter cases

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            scenario: The scenario to filter on.
            scenario_prefix: The prefix of the scenario to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            commands: The command to filter on.
            run_status: The run status to filter on.
            run_status_prefix: The prefix of the run status to filter on.
            arguments: The argument to filter on.
            arguments_prefix: The prefix of the argument to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cases to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested cases

        Examples:

            List cases and limit to 5:

                >>> from shop.client import ShopClient
                >>> client = ShopClient()
                >>> cases = client.case.list(limit=5)

        """
        filter_ = _create_case_filter(
            self._view_id,
            name,
            name_prefix,
            scenario,
            scenario_prefix,
            min_start_time,
            max_start_time,
            min_end_time,
            max_end_time,
            commands,
            run_status,
            run_status_prefix,
            arguments,
            arguments_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
