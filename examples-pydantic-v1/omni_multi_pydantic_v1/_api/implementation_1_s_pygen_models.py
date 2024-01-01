from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from omni_multi_pydantic_v1.data_classes._core import DEFAULT_INSTANCE_SPACE
from omni_multi_pydantic_v1.data_classes import (
    DomainModelCore,
    DomainModelApply,
    ResourcesApplyResult,
    Implementation1sPygenModels,
    Implementation1sPygenModelsApply,
    Implementation1sPygenModelsFields,
    Implementation1sPygenModelsList,
    Implementation1sPygenModelsApplyList,
    Implementation1sPygenModelsTextFields,
)
from omni_multi_pydantic_v1.data_classes._implementation_1_s_pygen_models import (
    _IMPLEMENTATION1SPYGENMODELS_PROPERTIES_BY_FIELD,
    _create_implementation_1_s_pygen_model_filter,
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
from .implementation_1_s_pygen_models_query import Implementation1sPygenModelsQueryAPI


class Implementation1sPygenModelsAPI(
    NodeAPI[Implementation1sPygenModels, Implementation1sPygenModelsApply, Implementation1sPygenModelsList]
):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[Implementation1sPygenModels]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Implementation1sPygenModels,
            class_list=Implementation1sPygenModelsList,
            class_apply_list=Implementation1sPygenModelsApplyList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        value_2: str | list[str] | None = None,
        value_2_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> Implementation1sPygenModelsQueryAPI[Implementation1sPygenModelsList]:
        """Query starting at implementation 1 s pygen models.

        Args:
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            sub_value: The sub value to filter on.
            sub_value_prefix: The prefix of the sub value to filter on.
            value_1: The value 1 to filter on.
            value_1_prefix: The prefix of the value 1 to filter on.
            value_2: The value 2 to filter on.
            value_2_prefix: The prefix of the value 2 to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 1 s pygen models to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for implementation 1 s pygen models.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_implementation_1_s_pygen_model_filter(
            self._view_id,
            main_value,
            main_value_prefix,
            sub_value,
            sub_value_prefix,
            value_1,
            value_1_prefix,
            value_2,
            value_2_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(Implementation1sPygenModelsList)
        return Implementation1sPygenModelsQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        implementation_1_s_pygen_model: Implementation1sPygenModelsApply | Sequence[Implementation1sPygenModelsApply],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) implementation 1 s pygen models.

        Args:
            implementation_1_s_pygen_model: Implementation 1 s pygen model or sequence of implementation 1 s pygen models to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): Should we write None values to the API? If False, None values will be ignored. If True, None values will be written to the API.
                Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new implementation_1_s_pygen_model:

                >>> from omni_multi_pydantic_v1 import OmniMultiClient
                >>> from omni_multi_pydantic_v1.data_classes import Implementation1sPygenModelsApply
                >>> client = OmniMultiClient()
                >>> implementation_1_s_pygen_model = Implementation1sPygenModelsApply(external_id="my_implementation_1_s_pygen_model", ...)
                >>> result = client.implementation_1_s_pygen_models.apply(implementation_1_s_pygen_model)

        """
        return self._apply(implementation_1_s_pygen_model, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more implementation 1 s pygen model.

        Args:
            external_id: External id of the implementation 1 s pygen model to delete.
            space: The space where all the implementation 1 s pygen model are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete implementation_1_s_pygen_model by id:

                >>> from omni_multi_pydantic_v1 import OmniMultiClient
                >>> client = OmniMultiClient()
                >>> client.implementation_1_s_pygen_models.delete("my_implementation_1_s_pygen_model")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Implementation1sPygenModels | None:
        ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Implementation1sPygenModelsList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Implementation1sPygenModels | Implementation1sPygenModelsList | None:
        """Retrieve one or more implementation 1 s pygen models by id(s).

        Args:
            external_id: External id or list of external ids of the implementation 1 s pygen models.
            space: The space where all the implementation 1 s pygen models are located.

        Returns:
            The requested implementation 1 s pygen models.

        Examples:

            Retrieve implementation_1_s_pygen_model by id:

                >>> from omni_multi_pydantic_v1 import OmniMultiClient
                >>> client = OmniMultiClient()
                >>> implementation_1_s_pygen_model = client.implementation_1_s_pygen_models.retrieve("my_implementation_1_s_pygen_model")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: Implementation1sPygenModelsTextFields
        | Sequence[Implementation1sPygenModelsTextFields]
        | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        value_2: str | list[str] | None = None,
        value_2_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> Implementation1sPygenModelsList:
        """Search implementation 1 s pygen models

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            sub_value: The sub value to filter on.
            sub_value_prefix: The prefix of the sub value to filter on.
            value_1: The value 1 to filter on.
            value_1_prefix: The prefix of the value 1 to filter on.
            value_2: The value 2 to filter on.
            value_2_prefix: The prefix of the value 2 to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 1 s pygen models to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results implementation 1 s pygen models matching the query.

        Examples:

           Search for 'my_implementation_1_s_pygen_model' in all text properties:

                >>> from omni_multi_pydantic_v1 import OmniMultiClient
                >>> client = OmniMultiClient()
                >>> implementation_1_s_pygen_models = client.implementation_1_s_pygen_models.search('my_implementation_1_s_pygen_model')

        """
        filter_ = _create_implementation_1_s_pygen_model_filter(
            self._view_id,
            main_value,
            main_value_prefix,
            sub_value,
            sub_value_prefix,
            value_1,
            value_1_prefix,
            value_2,
            value_2_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _IMPLEMENTATION1SPYGENMODELS_PROPERTIES_BY_FIELD, properties, filter_, limit
        )

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: Implementation1sPygenModelsFields | Sequence[Implementation1sPygenModelsFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: Implementation1sPygenModelsTextFields
        | Sequence[Implementation1sPygenModelsTextFields]
        | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        value_2: str | list[str] | None = None,
        value_2_prefix: str | None = None,
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
        property: Implementation1sPygenModelsFields | Sequence[Implementation1sPygenModelsFields] | None = None,
        group_by: Implementation1sPygenModelsFields | Sequence[Implementation1sPygenModelsFields] = None,
        query: str | None = None,
        search_properties: Implementation1sPygenModelsTextFields
        | Sequence[Implementation1sPygenModelsTextFields]
        | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        value_2: str | list[str] | None = None,
        value_2_prefix: str | None = None,
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
        property: Implementation1sPygenModelsFields | Sequence[Implementation1sPygenModelsFields] | None = None,
        group_by: Implementation1sPygenModelsFields | Sequence[Implementation1sPygenModelsFields] | None = None,
        query: str | None = None,
        search_property: Implementation1sPygenModelsTextFields
        | Sequence[Implementation1sPygenModelsTextFields]
        | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        value_2: str | list[str] | None = None,
        value_2_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across implementation 1 s pygen models

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            sub_value: The sub value to filter on.
            sub_value_prefix: The prefix of the sub value to filter on.
            value_1: The value 1 to filter on.
            value_1_prefix: The prefix of the value 1 to filter on.
            value_2: The value 2 to filter on.
            value_2_prefix: The prefix of the value 2 to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 1 s pygen models to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count implementation 1 s pygen models in space `my_space`:

                >>> from omni_multi_pydantic_v1 import OmniMultiClient
                >>> client = OmniMultiClient()
                >>> result = client.implementation_1_s_pygen_models.aggregate("count", space="my_space")

        """

        filter_ = _create_implementation_1_s_pygen_model_filter(
            self._view_id,
            main_value,
            main_value_prefix,
            sub_value,
            sub_value_prefix,
            value_1,
            value_1_prefix,
            value_2,
            value_2_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _IMPLEMENTATION1SPYGENMODELS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: Implementation1sPygenModelsFields,
        interval: float,
        query: str | None = None,
        search_property: Implementation1sPygenModelsTextFields
        | Sequence[Implementation1sPygenModelsTextFields]
        | None = None,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        value_2: str | list[str] | None = None,
        value_2_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for implementation 1 s pygen models

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            sub_value: The sub value to filter on.
            sub_value_prefix: The prefix of the sub value to filter on.
            value_1: The value 1 to filter on.
            value_1_prefix: The prefix of the value 1 to filter on.
            value_2: The value 2 to filter on.
            value_2_prefix: The prefix of the value 2 to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 1 s pygen models to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_implementation_1_s_pygen_model_filter(
            self._view_id,
            main_value,
            main_value_prefix,
            sub_value,
            sub_value_prefix,
            value_1,
            value_1_prefix,
            value_2,
            value_2_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _IMPLEMENTATION1SPYGENMODELS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        main_value: str | list[str] | None = None,
        main_value_prefix: str | None = None,
        sub_value: str | list[str] | None = None,
        sub_value_prefix: str | None = None,
        value_1: str | list[str] | None = None,
        value_1_prefix: str | None = None,
        value_2: str | list[str] | None = None,
        value_2_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> Implementation1sPygenModelsList:
        """List/filter implementation 1 s pygen models

        Args:
            main_value: The main value to filter on.
            main_value_prefix: The prefix of the main value to filter on.
            sub_value: The sub value to filter on.
            sub_value_prefix: The prefix of the sub value to filter on.
            value_1: The value 1 to filter on.
            value_1_prefix: The prefix of the value 1 to filter on.
            value_2: The value 2 to filter on.
            value_2_prefix: The prefix of the value 2 to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of implementation 1 s pygen models to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested implementation 1 s pygen models

        Examples:

            List implementation 1 s pygen models and limit to 5:

                >>> from omni_multi_pydantic_v1 import OmniMultiClient
                >>> client = OmniMultiClient()
                >>> implementation_1_s_pygen_models = client.implementation_1_s_pygen_models.list(limit=5)

        """
        filter_ = _create_implementation_1_s_pygen_model_filter(
            self._view_id,
            main_value,
            main_value_prefix,
            sub_value,
            sub_value_prefix,
            value_1,
            value_1_prefix,
            value_2,
            value_2_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
