"""Client for the generated SDK.

This module contains the ExamplePygenClient that composes view-specific APIs.
"""

from cognite.pygen._python.instance_api._client import InstanceClient
from cognite.pygen._python.instance_api.config import PygenClientConfig

from ._api import CategoryNodeApi, ProductNodeApi, RelatesToApi


class ExamplePygenClient(InstanceClient):
    """Generated client for interacting with the data model.

    This client provides access to the following views:
    - category_node: CategoryNodeApi
    - product_node: ProductNodeApi
    - relates_to: RelatesToApi
    """

    def __init__(self, config: PygenClientConfig) -> None:
        """Initialize the client.

        Args:
            config: Configuration for the client including URL, project, and credentials.
        """
        super().__init__(config)

        # Initialize view-specific APIs
        self.category_node = CategoryNodeApi(self._http_client)
        self.product_node = ProductNodeApi(self._http_client)
        self.relates_to = RelatesToApi(self._http_client)
