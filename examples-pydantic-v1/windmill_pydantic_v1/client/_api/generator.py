from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from windmill_pydantic_v1.client.data_classes._core import DEFAULT_INSTANCE_SPACE
from windmill_pydantic_v1.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    Generator,
    GeneratorApply,
    GeneratorFields,
    GeneratorList,
    GeneratorApplyList,
)
from windmill_pydantic_v1.client.data_classes._generator import (
    _GENERATOR_PROPERTIES_BY_FIELD,
    _create_generator_filter,
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
from .generator_generator_speed_controller import GeneratorGeneratorSpeedControllerAPI
from .generator_generator_speed_controller_reference import GeneratorGeneratorSpeedControllerReferenceAPI
from .generator_query import GeneratorQueryAPI


class GeneratorAPI(NodeAPI[Generator, GeneratorApply, GeneratorList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[GeneratorApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Generator,
            class_apply_type=GeneratorApply,
            class_list=GeneratorList,
            class_apply_list=GeneratorApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.generator_speed_controller = GeneratorGeneratorSpeedControllerAPI(client, view_id)
        self.generator_speed_controller_reference = GeneratorGeneratorSpeedControllerReferenceAPI(client, view_id)

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> GeneratorQueryAPI[GeneratorList]:
        """Query starting at generators.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generators to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for generators.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_generator_filter(
            self._view_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(GeneratorList)
        return GeneratorQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self, generator: GeneratorApply | Sequence[GeneratorApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) generators.

        Args:
            generator: Generator or sequence of generators to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new generator:

                >>> from windmill_pydantic_v1.client import WindmillClient
                >>> from windmill_pydantic_v1.client.data_classes import GeneratorApply
                >>> client = WindmillClient()
                >>> generator = GeneratorApply(external_id="my_generator", ...)
                >>> result = client.generator.apply(generator)

        """
        return self._apply(generator, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more generator.

        Args:
            external_id: External id of the generator to delete.
            space: The space where all the generator are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete generator by id:

                >>> from windmill_pydantic_v1.client import WindmillClient
                >>> client = WindmillClient()
                >>> client.generator.delete("my_generator")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Generator | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> GeneratorList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Generator | GeneratorList | None:
        """Retrieve one or more generators by id(s).

        Args:
            external_id: External id or list of external ids of the generators.
            space: The space where all the generators are located.

        Returns:
            The requested generators.

        Examples:

            Retrieve generator by id:

                >>> from windmill_pydantic_v1.client import WindmillClient
                >>> client = WindmillClient()
                >>> generator = client.generator.retrieve("my_generator")

        """
        return self._retrieve(external_id, space)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: GeneratorFields | Sequence[GeneratorFields] | None = None,
        group_by: None = None,
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
        property: GeneratorFields | Sequence[GeneratorFields] | None = None,
        group_by: GeneratorFields | Sequence[GeneratorFields] = None,
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
        property: GeneratorFields | Sequence[GeneratorFields] | None = None,
        group_by: GeneratorFields | Sequence[GeneratorFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across generators

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generators to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count generators in space `my_space`:

                >>> from windmill_pydantic_v1.client import WindmillClient
                >>> client = WindmillClient()
                >>> result = client.generator.aggregate("count", space="my_space")

        """

        filter_ = _create_generator_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _GENERATOR_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: GeneratorFields,
        interval: float,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for generators

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generators to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_generator_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _GENERATOR_PROPERTIES_BY_FIELD,
            None,
            None,
            limit,
            filter_,
        )

    def list(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> GeneratorList:
        """List/filter generators

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generators to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested generators

        Examples:

            List generators and limit to 5:

                >>> from windmill_pydantic_v1.client import WindmillClient
                >>> client = WindmillClient()
                >>> generators = client.generator.list(limit=5)

        """
        filter_ = _create_generator_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)