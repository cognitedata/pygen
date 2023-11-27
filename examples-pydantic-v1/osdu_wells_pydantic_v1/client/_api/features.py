from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from osdu_wells_pydantic_v1.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    Features,
    FeaturesApply,
    FeaturesFields,
    FeaturesList,
    FeaturesApplyList,
    FeaturesTextFields,
)
from osdu_wells_pydantic_v1.client.data_classes._features import (
    _FEATURES_PROPERTIES_BY_FIELD,
    _create_feature_filter,
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
from .features_query import FeaturesQueryAPI


class FeaturesAPI(NodeAPI[Features, FeaturesApply, FeaturesList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[FeaturesApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Features,
            class_apply_type=FeaturesApply,
            class_list=FeaturesList,
            class_apply_list=FeaturesApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        geometry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> FeaturesQueryAPI[FeaturesList]:
        """Query starting at features.

        Args:
            geometry: The geometry to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of features to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for features.

        """
        filter_ = _create_feature_filter(
            self._view_id,
            geometry,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            FeaturesList,
            [
                QueryStep(
                    name="feature",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_FEATURES_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=Features,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return FeaturesQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(self, feature: FeaturesApply | Sequence[FeaturesApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) features.

        Args:
            feature: Feature or sequence of features to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new feature:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> from osdu_wells_pydantic_v1.client.data_classes import FeaturesApply
                >>> client = OSDUClient()
                >>> feature = FeaturesApply(external_id="my_feature", ...)
                >>> result = client.features.apply(feature)

        """
        return self._apply(feature, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more feature.

        Args:
            external_id: External id of the feature to delete.
            space: The space where all the feature are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete feature by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> client.features.delete("my_feature")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> Features | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> FeaturesList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "IntegrationTestsImmutable"
    ) -> Features | FeaturesList | None:
        """Retrieve one or more features by id(s).

        Args:
            external_id: External id or list of external ids of the features.
            space: The space where all the features are located.

        Returns:
            The requested features.

        Examples:

            Retrieve feature by id:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> feature = client.features.retrieve("my_feature")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: FeaturesTextFields | Sequence[FeaturesTextFields] | None = None,
        geometry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> FeaturesList:
        """Search features

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            geometry: The geometry to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of features to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results features matching the query.

        Examples:

           Search for 'my_feature' in all text properties:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> features = client.features.search('my_feature')

        """
        filter_ = _create_feature_filter(
            self._view_id,
            geometry,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _FEATURES_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: FeaturesFields | Sequence[FeaturesFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: FeaturesTextFields | Sequence[FeaturesTextFields] | None = None,
        geometry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
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
        property: FeaturesFields | Sequence[FeaturesFields] | None = None,
        group_by: FeaturesFields | Sequence[FeaturesFields] = None,
        query: str | None = None,
        search_properties: FeaturesTextFields | Sequence[FeaturesTextFields] | None = None,
        geometry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
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
        property: FeaturesFields | Sequence[FeaturesFields] | None = None,
        group_by: FeaturesFields | Sequence[FeaturesFields] | None = None,
        query: str | None = None,
        search_property: FeaturesTextFields | Sequence[FeaturesTextFields] | None = None,
        geometry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across features

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            geometry: The geometry to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of features to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count features in space `my_space`:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> result = client.features.aggregate("count", space="my_space")

        """

        filter_ = _create_feature_filter(
            self._view_id,
            geometry,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _FEATURES_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: FeaturesFields,
        interval: float,
        query: str | None = None,
        search_property: FeaturesTextFields | Sequence[FeaturesTextFields] | None = None,
        geometry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for features

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            geometry: The geometry to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of features to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_feature_filter(
            self._view_id,
            geometry,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _FEATURES_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        geometry: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        type_: str | list[str] | None = None,
        type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> FeaturesList:
        """List/filter features

        Args:
            geometry: The geometry to filter on.
            type_: The type to filter on.
            type_prefix: The prefix of the type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of features to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested features

        Examples:

            List features and limit to 5:

                >>> from osdu_wells_pydantic_v1.client import OSDUClient
                >>> client = OSDUClient()
                >>> features = client.features.list(limit=5)

        """
        filter_ = _create_feature_filter(
            self._view_id,
            geometry,
            type_,
            type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
