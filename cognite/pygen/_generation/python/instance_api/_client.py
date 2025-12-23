from collections.abc import Sequence
from typing import Literal

from cognite.pygen._client import PygenClientConfig
from cognite.pygen._generation.python.instance_api.http_client import HTTPClient

from ._instance import InstanceId, InstanceResult, InstanceWrite


class InstanceClient:
    def __init__(self, config: PygenClientConfig) -> None:
        """Initialize the Pygen client.

        Args:
            config: Configuration for the client including URL, project, and credentials.
        """
        self._config = config
        self._http_client = HTTPClient(config)

    def upsert(
        self,
        items: Sequence[InstanceWrite],
        mode: Literal["update", "create"],
    ) -> InstanceResult:
        raise NotImplementedError

    def delete(self, items: Sequence[InstanceWrite | InstanceId]) -> InstanceResult:
        raise NotImplementedError
