from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from scenario_instance.client.data_classes._core import DEFAULT_INSTANCE_SPACE
from scenario_instance.client.data_classes import (
    DomainModelCore,
    DomainModelApply,
    ResourcesApplyResult,
    ScenarioInstance,
    ScenarioInstanceApply,
    ScenarioInstanceFields,
    ScenarioInstanceList,
    ScenarioInstanceApplyList,
    ScenarioInstanceTextFields,
)
from scenario_instance.client.data_classes._scenario_instance import (
    _SCENARIOINSTANCE_PROPERTIES_BY_FIELD,
    _create_scenario_instance_filter,
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
from .scenario_instance_price_forecast import ScenarioInstancePriceForecastAPI
from .scenario_instance_query import ScenarioInstanceQueryAPI


class ScenarioInstanceAPI(NodeAPI[ScenarioInstance, ScenarioInstanceApply, ScenarioInstanceList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[ScenarioInstance]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ScenarioInstance,
            class_list=ScenarioInstanceList,
            class_apply_list=ScenarioInstanceApplyList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.price_forecast = ScenarioInstancePriceForecastAPI(client, view_id)

    def __call__(
        self,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        country: str | list[str] | None = None,
        country_prefix: str | None = None,
        min_instance: datetime.datetime | None = None,
        max_instance: datetime.datetime | None = None,
        market: str | list[str] | None = None,
        market_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        scenario: str | list[str] | None = None,
        scenario_prefix: str | None = None,
        min_start: datetime.datetime | None = None,
        max_start: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ScenarioInstanceQueryAPI[ScenarioInstanceList]:
        """Query starting at scenario instances.

        Args:
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            country: The country to filter on.
            country_prefix: The prefix of the country to filter on.
            min_instance: The minimum value of the instance to filter on.
            max_instance: The maximum value of the instance to filter on.
            market: The market to filter on.
            market_prefix: The prefix of the market to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            scenario: The scenario to filter on.
            scenario_prefix: The prefix of the scenario to filter on.
            min_start: The minimum value of the start to filter on.
            max_start: The maximum value of the start to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenario instances to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for scenario instances.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_scenario_instance_filter(
            self._view_id,
            aggregation,
            aggregation_prefix,
            country,
            country_prefix,
            min_instance,
            max_instance,
            market,
            market_prefix,
            price_area,
            price_area_prefix,
            scenario,
            scenario_prefix,
            min_start,
            max_start,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(ScenarioInstanceList)
        return ScenarioInstanceQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self, scenario_instance: ScenarioInstanceApply | Sequence[ScenarioInstanceApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) scenario instances.

        Args:
            scenario_instance: Scenario instance or sequence of scenario instances to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new scenario_instance:

                >>> from scenario_instance.client import ScenarioInstanceClient
                >>> from scenario_instance.client.data_classes import ScenarioInstanceApply
                >>> client = ScenarioInstanceClient()
                >>> scenario_instance = ScenarioInstanceApply(external_id="my_scenario_instance", ...)
                >>> result = client.scenario_instance.apply(scenario_instance)

        """
        return self._apply(scenario_instance, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more scenario instance.

        Args:
            external_id: External id of the scenario instance to delete.
            space: The space where all the scenario instance are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete scenario_instance by id:

                >>> from scenario_instance.client import ScenarioInstanceClient
                >>> client = ScenarioInstanceClient()
                >>> client.scenario_instance.delete("my_scenario_instance")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> ScenarioInstance | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> ScenarioInstanceList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ScenarioInstance | ScenarioInstanceList | None:
        """Retrieve one or more scenario instances by id(s).

        Args:
            external_id: External id or list of external ids of the scenario instances.
            space: The space where all the scenario instances are located.

        Returns:
            The requested scenario instances.

        Examples:

            Retrieve scenario_instance by id:

                >>> from scenario_instance.client import ScenarioInstanceClient
                >>> client = ScenarioInstanceClient()
                >>> scenario_instance = client.scenario_instance.retrieve("my_scenario_instance")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: ScenarioInstanceTextFields | Sequence[ScenarioInstanceTextFields] | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        country: str | list[str] | None = None,
        country_prefix: str | None = None,
        min_instance: datetime.datetime | None = None,
        max_instance: datetime.datetime | None = None,
        market: str | list[str] | None = None,
        market_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        scenario: str | list[str] | None = None,
        scenario_prefix: str | None = None,
        min_start: datetime.datetime | None = None,
        max_start: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ScenarioInstanceList:
        """Search scenario instances

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            country: The country to filter on.
            country_prefix: The prefix of the country to filter on.
            min_instance: The minimum value of the instance to filter on.
            max_instance: The maximum value of the instance to filter on.
            market: The market to filter on.
            market_prefix: The prefix of the market to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            scenario: The scenario to filter on.
            scenario_prefix: The prefix of the scenario to filter on.
            min_start: The minimum value of the start to filter on.
            max_start: The maximum value of the start to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenario instances to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results scenario instances matching the query.

        Examples:

           Search for 'my_scenario_instance' in all text properties:

                >>> from scenario_instance.client import ScenarioInstanceClient
                >>> client = ScenarioInstanceClient()
                >>> scenario_instances = client.scenario_instance.search('my_scenario_instance')

        """
        filter_ = _create_scenario_instance_filter(
            self._view_id,
            aggregation,
            aggregation_prefix,
            country,
            country_prefix,
            min_instance,
            max_instance,
            market,
            market_prefix,
            price_area,
            price_area_prefix,
            scenario,
            scenario_prefix,
            min_start,
            max_start,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _SCENARIOINSTANCE_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ScenarioInstanceFields | Sequence[ScenarioInstanceFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: ScenarioInstanceTextFields | Sequence[ScenarioInstanceTextFields] | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        country: str | list[str] | None = None,
        country_prefix: str | None = None,
        min_instance: datetime.datetime | None = None,
        max_instance: datetime.datetime | None = None,
        market: str | list[str] | None = None,
        market_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        scenario: str | list[str] | None = None,
        scenario_prefix: str | None = None,
        min_start: datetime.datetime | None = None,
        max_start: datetime.datetime | None = None,
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
        property: ScenarioInstanceFields | Sequence[ScenarioInstanceFields] | None = None,
        group_by: ScenarioInstanceFields | Sequence[ScenarioInstanceFields] = None,
        query: str | None = None,
        search_properties: ScenarioInstanceTextFields | Sequence[ScenarioInstanceTextFields] | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        country: str | list[str] | None = None,
        country_prefix: str | None = None,
        min_instance: datetime.datetime | None = None,
        max_instance: datetime.datetime | None = None,
        market: str | list[str] | None = None,
        market_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        scenario: str | list[str] | None = None,
        scenario_prefix: str | None = None,
        min_start: datetime.datetime | None = None,
        max_start: datetime.datetime | None = None,
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
        property: ScenarioInstanceFields | Sequence[ScenarioInstanceFields] | None = None,
        group_by: ScenarioInstanceFields | Sequence[ScenarioInstanceFields] | None = None,
        query: str | None = None,
        search_property: ScenarioInstanceTextFields | Sequence[ScenarioInstanceTextFields] | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        country: str | list[str] | None = None,
        country_prefix: str | None = None,
        min_instance: datetime.datetime | None = None,
        max_instance: datetime.datetime | None = None,
        market: str | list[str] | None = None,
        market_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        scenario: str | list[str] | None = None,
        scenario_prefix: str | None = None,
        min_start: datetime.datetime | None = None,
        max_start: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across scenario instances

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            country: The country to filter on.
            country_prefix: The prefix of the country to filter on.
            min_instance: The minimum value of the instance to filter on.
            max_instance: The maximum value of the instance to filter on.
            market: The market to filter on.
            market_prefix: The prefix of the market to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            scenario: The scenario to filter on.
            scenario_prefix: The prefix of the scenario to filter on.
            min_start: The minimum value of the start to filter on.
            max_start: The maximum value of the start to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenario instances to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count scenario instances in space `my_space`:

                >>> from scenario_instance.client import ScenarioInstanceClient
                >>> client = ScenarioInstanceClient()
                >>> result = client.scenario_instance.aggregate("count", space="my_space")

        """

        filter_ = _create_scenario_instance_filter(
            self._view_id,
            aggregation,
            aggregation_prefix,
            country,
            country_prefix,
            min_instance,
            max_instance,
            market,
            market_prefix,
            price_area,
            price_area_prefix,
            scenario,
            scenario_prefix,
            min_start,
            max_start,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _SCENARIOINSTANCE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ScenarioInstanceFields,
        interval: float,
        query: str | None = None,
        search_property: ScenarioInstanceTextFields | Sequence[ScenarioInstanceTextFields] | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        country: str | list[str] | None = None,
        country_prefix: str | None = None,
        min_instance: datetime.datetime | None = None,
        max_instance: datetime.datetime | None = None,
        market: str | list[str] | None = None,
        market_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        scenario: str | list[str] | None = None,
        scenario_prefix: str | None = None,
        min_start: datetime.datetime | None = None,
        max_start: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for scenario instances

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            country: The country to filter on.
            country_prefix: The prefix of the country to filter on.
            min_instance: The minimum value of the instance to filter on.
            max_instance: The maximum value of the instance to filter on.
            market: The market to filter on.
            market_prefix: The prefix of the market to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            scenario: The scenario to filter on.
            scenario_prefix: The prefix of the scenario to filter on.
            min_start: The minimum value of the start to filter on.
            max_start: The maximum value of the start to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenario instances to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_scenario_instance_filter(
            self._view_id,
            aggregation,
            aggregation_prefix,
            country,
            country_prefix,
            min_instance,
            max_instance,
            market,
            market_prefix,
            price_area,
            price_area_prefix,
            scenario,
            scenario_prefix,
            min_start,
            max_start,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _SCENARIOINSTANCE_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        country: str | list[str] | None = None,
        country_prefix: str | None = None,
        min_instance: datetime.datetime | None = None,
        max_instance: datetime.datetime | None = None,
        market: str | list[str] | None = None,
        market_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        scenario: str | list[str] | None = None,
        scenario_prefix: str | None = None,
        min_start: datetime.datetime | None = None,
        max_start: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ScenarioInstanceList:
        """List/filter scenario instances

        Args:
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            country: The country to filter on.
            country_prefix: The prefix of the country to filter on.
            min_instance: The minimum value of the instance to filter on.
            max_instance: The maximum value of the instance to filter on.
            market: The market to filter on.
            market_prefix: The prefix of the market to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            scenario: The scenario to filter on.
            scenario_prefix: The prefix of the scenario to filter on.
            min_start: The minimum value of the start to filter on.
            max_start: The maximum value of the start to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenario instances to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested scenario instances

        Examples:

            List scenario instances and limit to 5:

                >>> from scenario_instance.client import ScenarioInstanceClient
                >>> client = ScenarioInstanceClient()
                >>> scenario_instances = client.scenario_instance.list(limit=5)

        """
        filter_ = _create_scenario_instance_filter(
            self._view_id,
            aggregation,
            aggregation_prefix,
            country,
            country_prefix,
            min_instance,
            max_instance,
            market,
            market_prefix,
            price_area,
            price_area_prefix,
            scenario,
            scenario_prefix,
            min_start,
            max_start,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
