"""Example client for the pygen example SDK.

This module contains the ExampleClient that composes the view-specific APIs
for ProductNode, CategoryNode, and RelatesTo views.
"""

from cognite.pygen._python.instance_api._client import InstanceClient
from cognite.pygen._python.instance_api.config import PygenClientConfig

from ._api import CategoryNodeAPI, ProductNodeAPI, RelatesToAPI


class ExampleClient(InstanceClient):
    """Example client for the pygen example data model.

    This client provides access to the three views in the example data model:
    - product_node: ProductNode view (nodes with various property types)
    - category_node: CategoryNode view (nodes with reverse direct relation)
    - relates_to: RelatesTo view (edges between nodes)
    """

    def __init__(self, config: PygenClientConfig) -> None:
        """Initialize the ExampleClient.

        Args:
            config: Configuration for the client including URL, project, and credentials.
        """
        super().__init__(config)

        # Initialize view-specific APIs
        self.product_node = ProductNodeAPI(self._http_client)
        self.category_node = CategoryNodeAPI(self._http_client)
        self.relates_to = RelatesToAPI(self._http_client)
