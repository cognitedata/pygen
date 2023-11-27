from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class DateTransformationPairEndAPI(EdgeAPI):
    def list(
        self,
        date_transformation_pair: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        date_transformation_pair_space: str = "market",
        date_transformation: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        date_transformation_space: str = "market",
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List end edges of a date transformation pair.

        Args:
            date_transformation_pair: ID of the source date transformation pairs.
            date_transformation_pair_space: Location of the date transformation pairs.
            date_transformation: ID of the target date transformations.
            date_transformation_space: Location of the date transformations.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of end edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested end edges.

        Examples:

            List 5 end edges connected to "my_date_transformation_pair":

                >>> from markets.client import MarketClient
                >>> client = MarketClient()
                >>> date_transformation_pair = client.date_transformation_pair.end_edge.list("my_date_transformation_pair", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("market", "DateTransformationPair.end"),
            date_transformation_pair,
            date_transformation_pair_space,
            date_transformation,
            date_transformation_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
