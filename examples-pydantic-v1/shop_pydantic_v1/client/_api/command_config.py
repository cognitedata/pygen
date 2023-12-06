from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from shop_pydantic_v1.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    CommandConfig,
    CommandConfigApply,
    CommandConfigFields,
    CommandConfigList,
    CommandConfigApplyList,
    CommandConfigTextFields,
)
from shop_pydantic_v1.client.data_classes._command_config import (
    _COMMANDCONFIG_PROPERTIES_BY_FIELD,
    _create_command_config_filter,
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
from .command_config_query import CommandConfigQueryAPI


class CommandConfigAPI(NodeAPI[CommandConfig, CommandConfigApply, CommandConfigList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[CommandConfigApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=CommandConfig,
            class_apply_type=CommandConfigApply,
            class_list=CommandConfigList,
            class_apply_list=CommandConfigApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> CommandConfigQueryAPI[CommandConfigList]:
        """Query starting at command configs.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of command configs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for command configs.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_command_config_filter(
            self._view_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(CommandConfigList)
        return CommandConfigQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self, command_config: CommandConfigApply | Sequence[CommandConfigApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) command configs.

        Args:
            command_config: Command config or sequence of command configs to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new command_config:

                >>> from shop_pydantic_v1.client import ShopClient
                >>> from shop_pydantic_v1.client.data_classes import CommandConfigApply
                >>> client = ShopClient()
                >>> command_config = CommandConfigApply(external_id="my_command_config", ...)
                >>> result = client.command_config.apply(command_config)

        """
        return self._apply(command_config, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more command config.

        Args:
            external_id: External id of the command config to delete.
            space: The space where all the command config are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete command_config by id:

                >>> from shop_pydantic_v1.client import ShopClient
                >>> client = ShopClient()
                >>> client.command_config.delete("my_command_config")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> CommandConfig | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> CommandConfigList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> CommandConfig | CommandConfigList | None:
        """Retrieve one or more command configs by id(s).

        Args:
            external_id: External id or list of external ids of the command configs.
            space: The space where all the command configs are located.

        Returns:
            The requested command configs.

        Examples:

            Retrieve command_config by id:

                >>> from shop_pydantic_v1.client import ShopClient
                >>> client = ShopClient()
                >>> command_config = client.command_config.retrieve("my_command_config")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: CommandConfigTextFields | Sequence[CommandConfigTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CommandConfigList:
        """Search command configs

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of command configs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results command configs matching the query.

        Examples:

           Search for 'my_command_config' in all text properties:

                >>> from shop_pydantic_v1.client import ShopClient
                >>> client = ShopClient()
                >>> command_configs = client.command_config.search('my_command_config')

        """
        filter_ = _create_command_config_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _COMMANDCONFIG_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: CommandConfigFields | Sequence[CommandConfigFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: CommandConfigTextFields | Sequence[CommandConfigTextFields] | None = None,
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
        property: CommandConfigFields | Sequence[CommandConfigFields] | None = None,
        group_by: CommandConfigFields | Sequence[CommandConfigFields] = None,
        query: str | None = None,
        search_properties: CommandConfigTextFields | Sequence[CommandConfigTextFields] | None = None,
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
        property: CommandConfigFields | Sequence[CommandConfigFields] | None = None,
        group_by: CommandConfigFields | Sequence[CommandConfigFields] | None = None,
        query: str | None = None,
        search_property: CommandConfigTextFields | Sequence[CommandConfigTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across command configs

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of command configs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count command configs in space `my_space`:

                >>> from shop_pydantic_v1.client import ShopClient
                >>> client = ShopClient()
                >>> result = client.command_config.aggregate("count", space="my_space")

        """

        filter_ = _create_command_config_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _COMMANDCONFIG_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: CommandConfigFields,
        interval: float,
        query: str | None = None,
        search_property: CommandConfigTextFields | Sequence[CommandConfigTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for command configs

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of command configs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_command_config_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _COMMANDCONFIG_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> CommandConfigList:
        """List/filter command configs

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of command configs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested command configs

        Examples:

            List command configs and limit to 5:

                >>> from shop_pydantic_v1.client import ShopClient
                >>> client = ShopClient()
                >>> command_configs = client.command_config.list(limit=5)

        """
        filter_ = _create_command_config_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
