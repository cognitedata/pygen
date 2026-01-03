import sys
from collections.abc import Set

from pydantic import BaseModel, ConfigDict, Field

from cognite.pygen._python.instance_api.auth import Credentials

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class PygenClientConfig(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        frozen=False,
    )

    cdf_url: str
    project: str
    credentials: Credentials
    client_name: str | None = None
    max_retries: int = 10
    pool_connections: int = Field(default=10, description="The number of connection pools to cache.")
    pool_maxsize: int = Field(default=20, description="The maximum number of connections to save in the pool.")
    retry_status_codes: Set[int] = Field(
        # 408 - Request Timeout
        # 429 - Too Many Requests
        # 502 - Bad Gateway
        # 503 - Service Unavailable
        # 504 - Gateway Timeout
        default=frozenset({408, 429, 502, 503, 504}),
        description="HTTP status codes that should trigger a retry.",
    )
    max_retry_backoff: int = Field(default=60, description="The maximum backoff time in seconds between retries.")
    write_workers: int = 1
    delete_workers: int = 1
    retrieve_workers: int = 1

    api_subversion: str = "20230101"
    timeout: float = 30.0

    @property
    def base_api_url(self) -> str:
        """Construct the base API URL.

        Returns:
            str: The base API URL.

        Examples:
            >>> config = PygenClientConfig(
            ...     cdf_url="https://bluefield.cognitedata.com",
            ...     project="my_project",
            ...     credentials=TokenCredentials(token="my_token"),
            ... )
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
            ...     credentials=TokenCredentials(token="my_token"),
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
