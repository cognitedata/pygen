from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from markets_pydantic_v1.client.data_classes._core import DEFAULT_INSTANCE_SPACE


class DateTransformationPairStartAPI(EdgeAPI):
    def list(
        self,
        from_date_transformation_pair: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_date_transformation_pair_space: str = DEFAULT_INSTANCE_SPACE,
        to_date_transformation: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_date_transformation_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List start edges of a date transformation pair.

        Args:
            from_date_transformation_pair: ID of the source date transformation pair.
            from_date_transformation_pair_space: Location of the date transformation pairs.
            to_date_transformation: ID of the target date transformation.
            to_date_transformation_space: Location of the date transformations.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of start edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested start edges.

        Examples:

            List 5 start edges connected to "my_date_transformation_pair":

                >>> from markets_pydantic_v1.client import MarketClient
                >>> client = MarketClient()
                >>> date_transformation_pair = client.date_transformation_pair.start_edge.list("my_date_transformation_pair", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("market", "DateTransformationPair.start"),
            from_date_transformation_pair,
            from_date_transformation_pair_space,
            to_date_transformation,
            to_date_transformation_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
