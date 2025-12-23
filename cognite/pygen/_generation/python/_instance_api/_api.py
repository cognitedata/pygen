from collections.abc import Sequence
from typing import Generic

from cognite.pygen._client.http_client import HTTPClient
from cognite.pygen._generation.python._instance_api._instance import (
    InstanceId,
    T_Instance,
    T_InstanceWrite,
)


class InstanceAPI(Generic[T_InstanceWrite, T_Instance]):
    """Generic resource API for CDF Data Modeling resources.

    This class provides common CRUD operations for CDF Data Modeling resources.
    It is designed to be used via composition, not inheritance.
    """

    ENDPOINT = "/models/instances"

    def __init__(self, http_client: HTTPClient) -> None:
        """Initialize the resource API.

        Args:
            http_client: The HTTP client to use for API requests.
        """
        self._http_client = http_client

    def _retrieve(self, ids: Sequence[InstanceId]) -> list[T_Instance]:
        raise NotImplementedError

    def _aggregate(self) -> None:
        raise NotImplementedError

    def _search(self) -> None:
        raise NotImplementedError

    def _sync(self) -> None:
        raise NotImplementedError

    def _query(self) -> None:
        raise NotImplementedError

    def _list(self) -> None:
        raise NotImplementedError
