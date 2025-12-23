from collections.abc import Sequence

from cognite.pygen._generation.python._instance_api._api import InstanceAPI
from cognite.pygen._generation.python._instance_api._instance import InstanceId, Page
from cognite.pygen._generation.python.example._data_class import PrimitiveNullable, PrimitiveNullableWrite


class PrimitiveNullableAPI(InstanceAPI[PrimitiveNullableWrite, PrimitiveNullable]):
    """API client for PrimitiveNullable instances in the example space."""

    def retrieve(self, ids: Sequence[InstanceId]) -> list[PrimitiveNullable]:
        """Retrieve specific PrimitiveNullable instances by their IDs.

        Args:
            ids: A sequence of instance IDs identifying the PrimitiveNullable instances to retrieve.

        Returns:
            A list of PrimitiveNullable instances. Instances that don't exist are not included.
        """
        return self._retrieve(ids)

    def iterate(self) -> Page[PrimitiveNullable]:
        """Fetch a single page of PrimitiveNullable instances.

        Returns:
            A Page containing the PrimitiveNullable instances and the cursor for the next page.
        """
        raise NotImplementedError()

    def list(self) -> list[PrimitiveNullable]:
        """List all PrimitiveNullable instances, handling pagination automatically.

        This method lazily iterates over all PrimitiveNullable instances, fetching pages as needed.

        Returns:
            A list of all PrimitiveNullable instances.
        """
        raise NotImplementedError()
