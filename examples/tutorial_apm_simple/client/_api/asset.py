from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from tutorial_apm_simple.client.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    Asset,
    AssetApply,
    AssetFields,
    AssetList,
    AssetTextFields,
    CdfConnectionProperties,
    CdfConnectionPropertiesApply,
    CdfConnectionPropertiesList,
)
from tutorial_apm_simple.client.data_classes._asset import (
    _ASSET_PROPERTIES_BY_FIELD,
    _create_asset_filter,
)
from ._core import DEFAULT_LIMIT_READ, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .asset_children import AssetChildrenAPI
from .asset_in_model_3_d import AssetInModelAPI
from .asset_pressure import AssetPressureAPI
from .asset_query import AssetQueryAPI


class AssetAPI(NodeAPI[Asset, AssetApply, AssetList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[AssetApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Asset,
            class_apply_type=AssetApply,
            class_list=AssetList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.children_edge = AssetChildrenAPI(client)
        self.in_model_3_d_edge = AssetInModelAPI(
            client,
            view_by_write_class,
            CdfConnectionProperties,
            CdfConnectionPropertiesApply,
            CdfConnectionPropertiesList,
        )
        self.pressure = AssetPressureAPI(client, view_id)

    def __call__(
        self,
        min_area_id: int | None = None,
        max_area_id: int | None = None,
        min_category_id: int | None = None,
        max_category_id: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_active: bool | None = None,
        is_critical_line: bool | None = None,
        parent: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source_db: str | list[str] | None = None,
        source_db_prefix: str | None = None,
        tag: str | list[str] | None = None,
        tag_prefix: str | None = None,
        min_updated_date: datetime.datetime | None = None,
        max_updated_date: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> AssetQueryAPI[AssetList]:
        """Query starting at assets.

        Args:
            min_area_id: The minimum value of the area id to filter on.
            max_area_id: The maximum value of the area id to filter on.
            min_category_id: The minimum value of the category id to filter on.
            max_category_id: The maximum value of the category id to filter on.
            min_created_date: The minimum value of the created date to filter on.
            max_created_date: The maximum value of the created date to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            is_active: The is active to filter on.
            is_critical_line: The is critical line to filter on.
            parent: The parent to filter on.
            source_db: The source db to filter on.
            source_db_prefix: The prefix of the source db to filter on.
            tag: The tag to filter on.
            tag_prefix: The prefix of the tag to filter on.
            min_updated_date: The minimum value of the updated date to filter on.
            max_updated_date: The maximum value of the updated date to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of assets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for assets.

        """
        filter_ = _create_asset_filter(
            self._view_id,
            min_area_id,
            max_area_id,
            min_category_id,
            max_category_id,
            min_created_date,
            max_created_date,
            description,
            description_prefix,
            is_active,
            is_critical_line,
            parent,
            source_db,
            source_db_prefix,
            tag,
            tag_prefix,
            min_updated_date,
            max_updated_date,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            AssetList,
            [
                QueryStep(
                    name="asset",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_ASSET_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=Asset,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return AssetQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(self, asset: AssetApply | Sequence[AssetApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) assets.

        Note: This method iterates through all nodes and timeseries linked to asset and creates them including the edges
        between the nodes. For example, if any of `children` or `in_model_3_d` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            asset: Asset or sequence of assets to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new asset:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> from tutorial_apm_simple.client.data_classes import AssetApply
                >>> client = ApmSimpleClient()
                >>> asset = AssetApply(external_id="my_asset", ...)
                >>> result = client.asset.apply(asset)

        """
        return self._apply(asset, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "tutorial_apm_simple"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more asset.

        Args:
            external_id: External id of the asset to delete.
            space: The space where all the asset are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete asset by id:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> client.asset.delete("my_asset")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> Asset:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> AssetList:
        ...

    def retrieve(self, external_id: str | SequenceNotStr[str], space: str = "tutorial_apm_simple") -> Asset | AssetList:
        """Retrieve one or more assets by id(s).

        Args:
            external_id: External id or list of external ids of the assets.
            space: The space where all the assets are located.

        Returns:
            The requested assets.

        Examples:

            Retrieve asset by id:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> asset = client.asset.retrieve("my_asset")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_pairs=[
                (self.children_edge, "children"),
                (self.in_model_3_d_edge, "in_model_3_d"),
            ],
        )

    def search(
        self,
        query: str,
        properties: AssetTextFields | Sequence[AssetTextFields] | None = None,
        min_area_id: int | None = None,
        max_area_id: int | None = None,
        min_category_id: int | None = None,
        max_category_id: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_active: bool | None = None,
        is_critical_line: bool | None = None,
        parent: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source_db: str | list[str] | None = None,
        source_db_prefix: str | None = None,
        tag: str | list[str] | None = None,
        tag_prefix: str | None = None,
        min_updated_date: datetime.datetime | None = None,
        max_updated_date: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> AssetList:
        """Search assets

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            min_area_id: The minimum value of the area id to filter on.
            max_area_id: The maximum value of the area id to filter on.
            min_category_id: The minimum value of the category id to filter on.
            max_category_id: The maximum value of the category id to filter on.
            min_created_date: The minimum value of the created date to filter on.
            max_created_date: The maximum value of the created date to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            is_active: The is active to filter on.
            is_critical_line: The is critical line to filter on.
            parent: The parent to filter on.
            source_db: The source db to filter on.
            source_db_prefix: The prefix of the source db to filter on.
            tag: The tag to filter on.
            tag_prefix: The prefix of the tag to filter on.
            min_updated_date: The minimum value of the updated date to filter on.
            max_updated_date: The maximum value of the updated date to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of assets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results assets matching the query.

        Examples:

           Search for 'my_asset' in all text properties:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> assets = client.asset.search('my_asset')

        """
        filter_ = _create_asset_filter(
            self._view_id,
            min_area_id,
            max_area_id,
            min_category_id,
            max_category_id,
            min_created_date,
            max_created_date,
            description,
            description_prefix,
            is_active,
            is_critical_line,
            parent,
            source_db,
            source_db_prefix,
            tag,
            tag_prefix,
            min_updated_date,
            max_updated_date,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _ASSET_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: AssetFields | Sequence[AssetFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: AssetTextFields | Sequence[AssetTextFields] | None = None,
        min_area_id: int | None = None,
        max_area_id: int | None = None,
        min_category_id: int | None = None,
        max_category_id: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_active: bool | None = None,
        is_critical_line: bool | None = None,
        parent: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source_db: str | list[str] | None = None,
        source_db_prefix: str | None = None,
        tag: str | list[str] | None = None,
        tag_prefix: str | None = None,
        min_updated_date: datetime.datetime | None = None,
        max_updated_date: datetime.datetime | None = None,
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
        property: AssetFields | Sequence[AssetFields] | None = None,
        group_by: AssetFields | Sequence[AssetFields] = None,
        query: str | None = None,
        search_properties: AssetTextFields | Sequence[AssetTextFields] | None = None,
        min_area_id: int | None = None,
        max_area_id: int | None = None,
        min_category_id: int | None = None,
        max_category_id: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_active: bool | None = None,
        is_critical_line: bool | None = None,
        parent: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source_db: str | list[str] | None = None,
        source_db_prefix: str | None = None,
        tag: str | list[str] | None = None,
        tag_prefix: str | None = None,
        min_updated_date: datetime.datetime | None = None,
        max_updated_date: datetime.datetime | None = None,
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
        property: AssetFields | Sequence[AssetFields] | None = None,
        group_by: AssetFields | Sequence[AssetFields] | None = None,
        query: str | None = None,
        search_property: AssetTextFields | Sequence[AssetTextFields] | None = None,
        min_area_id: int | None = None,
        max_area_id: int | None = None,
        min_category_id: int | None = None,
        max_category_id: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_active: bool | None = None,
        is_critical_line: bool | None = None,
        parent: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source_db: str | list[str] | None = None,
        source_db_prefix: str | None = None,
        tag: str | list[str] | None = None,
        tag_prefix: str | None = None,
        min_updated_date: datetime.datetime | None = None,
        max_updated_date: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across assets

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_area_id: The minimum value of the area id to filter on.
            max_area_id: The maximum value of the area id to filter on.
            min_category_id: The minimum value of the category id to filter on.
            max_category_id: The maximum value of the category id to filter on.
            min_created_date: The minimum value of the created date to filter on.
            max_created_date: The maximum value of the created date to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            is_active: The is active to filter on.
            is_critical_line: The is critical line to filter on.
            parent: The parent to filter on.
            source_db: The source db to filter on.
            source_db_prefix: The prefix of the source db to filter on.
            tag: The tag to filter on.
            tag_prefix: The prefix of the tag to filter on.
            min_updated_date: The minimum value of the updated date to filter on.
            max_updated_date: The maximum value of the updated date to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of assets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count assets in space `my_space`:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> result = client.asset.aggregate("count", space="my_space")

        """

        filter_ = _create_asset_filter(
            self._view_id,
            min_area_id,
            max_area_id,
            min_category_id,
            max_category_id,
            min_created_date,
            max_created_date,
            description,
            description_prefix,
            is_active,
            is_critical_line,
            parent,
            source_db,
            source_db_prefix,
            tag,
            tag_prefix,
            min_updated_date,
            max_updated_date,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _ASSET_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: AssetFields,
        interval: float,
        query: str | None = None,
        search_property: AssetTextFields | Sequence[AssetTextFields] | None = None,
        min_area_id: int | None = None,
        max_area_id: int | None = None,
        min_category_id: int | None = None,
        max_category_id: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_active: bool | None = None,
        is_critical_line: bool | None = None,
        parent: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source_db: str | list[str] | None = None,
        source_db_prefix: str | None = None,
        tag: str | list[str] | None = None,
        tag_prefix: str | None = None,
        min_updated_date: datetime.datetime | None = None,
        max_updated_date: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for assets

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_area_id: The minimum value of the area id to filter on.
            max_area_id: The maximum value of the area id to filter on.
            min_category_id: The minimum value of the category id to filter on.
            max_category_id: The maximum value of the category id to filter on.
            min_created_date: The minimum value of the created date to filter on.
            max_created_date: The maximum value of the created date to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            is_active: The is active to filter on.
            is_critical_line: The is critical line to filter on.
            parent: The parent to filter on.
            source_db: The source db to filter on.
            source_db_prefix: The prefix of the source db to filter on.
            tag: The tag to filter on.
            tag_prefix: The prefix of the tag to filter on.
            min_updated_date: The minimum value of the updated date to filter on.
            max_updated_date: The maximum value of the updated date to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of assets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_asset_filter(
            self._view_id,
            min_area_id,
            max_area_id,
            min_category_id,
            max_category_id,
            min_created_date,
            max_created_date,
            description,
            description_prefix,
            is_active,
            is_critical_line,
            parent,
            source_db,
            source_db_prefix,
            tag,
            tag_prefix,
            min_updated_date,
            max_updated_date,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _ASSET_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        min_area_id: int | None = None,
        max_area_id: int | None = None,
        min_category_id: int | None = None,
        max_category_id: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_active: bool | None = None,
        is_critical_line: bool | None = None,
        parent: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source_db: str | list[str] | None = None,
        source_db_prefix: str | None = None,
        tag: str | list[str] | None = None,
        tag_prefix: str | None = None,
        min_updated_date: datetime.datetime | None = None,
        max_updated_date: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> AssetList:
        """List/filter assets

        Args:
            min_area_id: The minimum value of the area id to filter on.
            max_area_id: The maximum value of the area id to filter on.
            min_category_id: The minimum value of the category id to filter on.
            max_category_id: The maximum value of the category id to filter on.
            min_created_date: The minimum value of the created date to filter on.
            max_created_date: The maximum value of the created date to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            is_active: The is active to filter on.
            is_critical_line: The is critical line to filter on.
            parent: The parent to filter on.
            source_db: The source db to filter on.
            source_db_prefix: The prefix of the source db to filter on.
            tag: The tag to filter on.
            tag_prefix: The prefix of the tag to filter on.
            min_updated_date: The minimum value of the updated date to filter on.
            max_updated_date: The maximum value of the updated date to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of assets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `children` or `in_model_3_d` external ids for the assets. Defaults to True.

        Returns:
            List of requested assets

        Examples:

            List assets and limit to 5:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> assets = client.asset.list(limit=5)

        """
        filter_ = _create_asset_filter(
            self._view_id,
            min_area_id,
            max_area_id,
            min_category_id,
            max_category_id,
            min_created_date,
            max_created_date,
            description,
            description_prefix,
            is_active,
            is_critical_line,
            parent,
            source_db,
            source_db_prefix,
            tag,
            tag_prefix,
            min_updated_date,
            max_updated_date,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_pairs=[
                (self.children_edge, "children"),
                (self.in_model_3_d_edge, "in_model_3_d"),
            ],
        )
