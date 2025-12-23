from cognite.pygen._client import PygenClientConfig
from cognite.pygen._generation.python._instance_api._client import InstanceClient
from cognite.pygen._generation.python.example._api import PrimitiveNullableAPI


class ExampleClient(InstanceClient):
    def __init__(
        self,
        config: PygenClientConfig,
    ) -> None:
        """Initialize the Pygen client.

        Args:
            config: Configuration for the client including URL, project, and credentials.
        """
        super().__init__(config)
        self.primitive_nullable = PrimitiveNullableAPI(self._http_client)
