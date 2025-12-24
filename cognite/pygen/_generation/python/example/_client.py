from cognite.pygen._client import PygenClientConfig
from cognite.pygen._generation.python.example._api import PrimitiveNullableAPI
from cognite.pygen._generation.python.instance_api._client import InstanceClient
from cognite.pygen._generation.python.instance_api.models import ViewReference


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
        self.primitive_nullable = PrimitiveNullableAPI(
            self._http_client,
            view_ref=ViewReference(space="example_space", external_id="PrimitiveNullable", version="v1"),
            instance_type="node",
        )
