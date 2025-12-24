from typing import Generic, Literal, overload

from cognite.pygen._generation.python.instance_api.http_client import HTTPClient
from cognite.pygen._generation.python.instance_api.models import (
    InstanceId,
    T_Instance,
    T_InstanceList,
    T_InstanceWrite,
    ViewReference,
)


class InstanceAPI(Generic[T_InstanceWrite, T_Instance, T_InstanceList]):
    """Generic resource API for CDF Data Modeling resources.

    This class provides common CRUD operations for CDF Data Modeling resources.
    It is designed to be used via composition, not inheritance.
    """

    ENDPOINT = "/models/instances"

    def __init__(
        self, http_client: HTTPClient, view_ref: ViewReference, instance_type: Literal["node", "edge"]
    ) -> None:
        """Initialize the resource API.

        Args:
            http_client: The HTTP client to use for API requests.
        """
        self._http_client = http_client
        self._view_ref = view_ref
        self._instance_type = instance_type

    @overload
    def _retrieve(
        self,
        id: str | InstanceId | tuple[str, str],
        space: str | None = None,
    ) -> T_Instance | None: ...

    @overload
    def _retrieve(
        self,
        id: list[str | InstanceId | tuple[str, str]],
        space: str | None = None,
    ) -> T_InstanceList: ...

    def _retrieve(
        self,
        id: str | InstanceId | tuple[str, str] | list[str | InstanceId | tuple[str, str]],
        space: str | None = None,
    ) -> T_Instance | T_InstanceList | None:
        raise NotImplementedError

    def _aggregate(self) -> None:
        raise NotImplementedError

    def _search(
        self,
        query: str | None = None,
        properties: list[str] | None = None,
    ) -> None:
        raise NotImplementedError

    def _sync(self) -> None:
        raise NotImplementedError

    def _query(self) -> None:
        raise NotImplementedError

    def _list(self) -> T_InstanceList:
        raise NotImplementedError
