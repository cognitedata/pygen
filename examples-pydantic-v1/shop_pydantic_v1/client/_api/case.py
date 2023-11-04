from __future__ import annotations

import datetime
from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT
from shop_pydantic_v1.client.data_classes import (
    Case,
    CaseApply,
    CaseList,
    CaseApplyList,
    CaseFields,
    CaseTextFields,
    DomainModelApply,
)
from shop_pydantic_v1.client.data_classes._case import _CASE_PROPERTIES_BY_FIELD


class CaseAPI(TypeAPI[Case, CaseApply, CaseList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[CaseApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Case,
            class_apply_type=CaseApply,
            class_list=CaseList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(self, case: CaseApply | Sequence[CaseApply], replace: bool = False) -> dm.InstancesApplyResult:
        """Add or update (upsert) cases.

        Args:
            case: Case or sequence of cases to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new case:

                >>> from shop_pydantic_v1.client import ShopClient
                >>> from shop_pydantic_v1.client.data_classes import CaseApply
                >>> client = ShopClient()
                >>> case = CaseApply(external_id="my_case", ...)
                >>> result = client.case.apply(case)

        """
        if isinstance(case, CaseApply):
            instances = case.to_instances_apply(self._view_by_write_class)
        else:
            instances = CaseApplyList(case).to_instances_apply(self._view_by_write_class)
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
        """Delete one or more case.

        Args:
            external_id: External id of the case to delete.
            space: The space where all the case are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete case by id:

                >>> from shop_pydantic_v1.client import ShopClient
                >>> client = ShopClient()
                >>> client.case.delete("my_case")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Case:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> CaseList:
        ...

    def retrieve(self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable") -> Case | CaseList:
        """Retrieve one or more cases by id(s).

        Args:
            external_id: External id or list of external ids of the cases.
            space: The space where all the cases are located.

        Returns:
            The requested cases.

        Examples:

            Retrieve case by id:

                >>> from shop_pydantic_v1.client import ShopClient
                >>> client = ShopClient()
                >>> case = client.case.retrieve("my_case")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: CaseTextFields | Sequence[CaseTextFields] | None = None,
        arguments: str | list[str] | None = None,
        arguments_prefix: str | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        run_status: str | list[str] | None = None,
        run_status_prefix: str | None = None,
        scenario: str | list[str] | None = None,
        scenario_prefix: str | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CaseList:
        """Search cases

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            arguments: The argument to filter on.
            arguments_prefix: The prefix of the argument to filter on.
            commands: The command to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            run_status: The run status to filter on.
            run_status_prefix: The prefix of the run status to filter on.
            scenario: The scenario to filter on.
            scenario_prefix: The prefix of the scenario to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cases to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results cases matching the query.

        Examples:

           Search for 'my_case' in all text properties:

                >>> from shop_pydantic_v1.client import ShopClient
                >>> client = ShopClient()
                >>> cases = client.case.search('my_case')

        """
        filter_ = _create_filter(
            self._view_id,
            arguments,
            arguments_prefix,
            commands,
            min_end_time,
            max_end_time,
            name,
            name_prefix,
            run_status,
            run_status_prefix,
            scenario,
            scenario_prefix,
            min_start_time,
            max_start_time,
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
        arguments: str | list[str] | None = None,
        arguments_prefix: str | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        run_status: str | list[str] | None = None,
        run_status_prefix: str | None = None,
        scenario: str | list[str] | None = None,
        scenario_prefix: str | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
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
        arguments: str | list[str] | None = None,
        arguments_prefix: str | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        run_status: str | list[str] | None = None,
        run_status_prefix: str | None = None,
        scenario: str | list[str] | None = None,
        scenario_prefix: str | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
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
        arguments: str | list[str] | None = None,
        arguments_prefix: str | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        run_status: str | list[str] | None = None,
        run_status_prefix: str | None = None,
        scenario: str | list[str] | None = None,
        scenario_prefix: str | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
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
            arguments: The argument to filter on.
            arguments_prefix: The prefix of the argument to filter on.
            commands: The command to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            run_status: The run status to filter on.
            run_status_prefix: The prefix of the run status to filter on.
            scenario: The scenario to filter on.
            scenario_prefix: The prefix of the scenario to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cases to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count cases in space `my_space`:

                >>> from shop_pydantic_v1.client import ShopClient
                >>> client = ShopClient()
                >>> result = client.case.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            arguments,
            arguments_prefix,
            commands,
            min_end_time,
            max_end_time,
            name,
            name_prefix,
            run_status,
            run_status_prefix,
            scenario,
            scenario_prefix,
            min_start_time,
            max_start_time,
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
        arguments: str | list[str] | None = None,
        arguments_prefix: str | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        run_status: str | list[str] | None = None,
        run_status_prefix: str | None = None,
        scenario: str | list[str] | None = None,
        scenario_prefix: str | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
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
            arguments: The argument to filter on.
            arguments_prefix: The prefix of the argument to filter on.
            commands: The command to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            run_status: The run status to filter on.
            run_status_prefix: The prefix of the run status to filter on.
            scenario: The scenario to filter on.
            scenario_prefix: The prefix of the scenario to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cases to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
            arguments,
            arguments_prefix,
            commands,
            min_end_time,
            max_end_time,
            name,
            name_prefix,
            run_status,
            run_status_prefix,
            scenario,
            scenario_prefix,
            min_start_time,
            max_start_time,
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
        arguments: str | list[str] | None = None,
        arguments_prefix: str | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        run_status: str | list[str] | None = None,
        run_status_prefix: str | None = None,
        scenario: str | list[str] | None = None,
        scenario_prefix: str | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CaseList:
        """List/filter cases

        Args:
            arguments: The argument to filter on.
            arguments_prefix: The prefix of the argument to filter on.
            commands: The command to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            run_status: The run status to filter on.
            run_status_prefix: The prefix of the run status to filter on.
            scenario: The scenario to filter on.
            scenario_prefix: The prefix of the scenario to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of cases to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested cases

        Examples:

            List cases and limit to 5:

                >>> from shop_pydantic_v1.client import ShopClient
                >>> client = ShopClient()
                >>> cases = client.case.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            arguments,
            arguments_prefix,
            commands,
            min_end_time,
            max_end_time,
            name,
            name_prefix,
            run_status,
            run_status_prefix,
            scenario,
            scenario_prefix,
            min_start_time,
            max_start_time,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    arguments: str | list[str] | None = None,
    arguments_prefix: str | None = None,
    commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    min_end_time: datetime.datetime | None = None,
    max_end_time: datetime.datetime | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    run_status: str | list[str] | None = None,
    run_status_prefix: str | None = None,
    scenario: str | list[str] | None = None,
    scenario_prefix: str | None = None,
    min_start_time: datetime.datetime | None = None,
    max_start_time: datetime.datetime | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if arguments and isinstance(arguments, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("arguments"), value=arguments))
    if arguments and isinstance(arguments, list):
        filters.append(dm.filters.In(view_id.as_property_ref("arguments"), values=arguments))
    if arguments_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("arguments"), value=arguments_prefix))
    if commands and isinstance(commands, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("commands"),
                value={"space": "IntegrationTestsImmutable", "externalId": commands},
            )
        )
    if commands and isinstance(commands, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("commands"), value={"space": commands[0], "externalId": commands[1]}
            )
        )
    if commands and isinstance(commands, list) and isinstance(commands[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("commands"),
                values=[{"space": "IntegrationTestsImmutable", "externalId": item} for item in commands],
            )
        )
    if commands and isinstance(commands, list) and isinstance(commands[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("commands"),
                values=[{"space": item[0], "externalId": item[1]} for item in commands],
            )
        )
    if min_end_time or max_end_time:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("end_time"),
                gte=min_end_time.isoformat(timespec="milliseconds") if min_end_time else None,
                lte=max_end_time.isoformat(timespec="milliseconds") if max_end_time else None,
            )
        )
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if run_status and isinstance(run_status, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("runStatus"), value=run_status))
    if run_status and isinstance(run_status, list):
        filters.append(dm.filters.In(view_id.as_property_ref("runStatus"), values=run_status))
    if run_status_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("runStatus"), value=run_status_prefix))
    if scenario and isinstance(scenario, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("scenario"), value=scenario))
    if scenario and isinstance(scenario, list):
        filters.append(dm.filters.In(view_id.as_property_ref("scenario"), values=scenario))
    if scenario_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("scenario"), value=scenario_prefix))
    if min_start_time or max_start_time:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("start_time"),
                gte=min_start_time.isoformat(timespec="milliseconds") if min_start_time else None,
                lte=max_start_time.isoformat(timespec="milliseconds") if max_start_time else None,
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
