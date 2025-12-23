from dataclasses import dataclass

from cognite.pygen._client.auth.credentials import Credentials
from cognite.pygen._version import __version__


@dataclass
class PygenClientConfig:
    cdf_url: str
    project: str
    credentials: Credentials
    client_name: str = f"CognitePygen:{__version__}:GenerateSDK"

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
