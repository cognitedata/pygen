from cognite.pygen._client import PygenClientConfig
from cognite.pygen._client.http_client import HTTPClient
from cognite.pygen._generation.python.example._api import PrimitiveNullableAPI

class ExampleClient:
    def __init__(
        self,
        config: PygenClientConfig,
    ) -> None:
        """Initialize the Pygen client.

        Args:
            config: Configuration for the client including URL, project, and credentials.
        """
        self._config = config
        self._http_client = HTTPClient(config)

        self.primitive_nullable = PrimitiveNullableAPI(self._http_client)


    def upsert(self, items: list[dict]) -> list[dict]:
        ...

    def delete(self, items: list[dict]) -> list[dict]:
        ...



