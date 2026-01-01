import sys
from collections.abc import Set
from dataclasses import dataclass

from cognite.pygen._python.instance_api.auth.credentials import Credentials

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


@dataclass
class PygenClientConfig:
    cdf_url: str
    project: str
    credentials: Credentials
    client_name: str | None = None
    max_retries: int = 10
    #  The number of connection pools to cache. Default is 10.
    pool_connections: int = 10
    # The maximum number of connections to save in the pool. Default is 20.
    pool_maxsize: int = 20
    # HTTP status codes that should trigger a retry.
    retry_status_codes: Set[int] = frozenset({408, 429, 502, 503, 504})
    # The maximum backoff time in seconds between retries. Default is 60 seconds.
    max_retry_backoff: int = 60

    api_subversion: str = "20230101"
    timeout: float = 30.0

    @property
    def base_api_url(self) -> str:
        """Construct the base API URL.

        Returns:
            str: The base API URL.

        Examples:
            >>> config = PygenClientConfig("https://bluefield.cognitedata.com", "my_project", ...)
            >>> config.base_api_url
            'https://bluefield.cognitedata.com/api/v1/projects/my_project'
        """
        return f"{self.cdf_url}/api/v1/projects/{self.project}"

    def create_api_url(self, endpoint: str) -> str:
        """Create a full API URL for the given endpoint.

        Args:
            endpoint (str): The API endpoint to append to the base URL.

        Returns:
            str: The full API URL.

        Examples:
            >>> config = PygenClientConfig(
            ...     cdf_url="https://bluefield.cognitedata.com",
            ...     project="my_project",
            ...     credentials=...,
            ... )
            >>> config.create_api_url("/models/instances")
            'https://bluefield.cognitedata.com/api/v1/projects/my_project/models/instances'
        """
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"
        return f"{self.base_api_url}{endpoint}"

    @classmethod
    def default(cls) -> Self:
        # Creates the config from the default (environment, repo root)
        raise NotImplementedError()
