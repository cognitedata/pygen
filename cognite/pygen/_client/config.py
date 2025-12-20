from dataclasses import dataclass

from cognite.pygen._client.auth.credentials import Credentials
from cognite.pygen._version import __version__


@dataclass
class PygenClientConfig:
    credentials: Credentials
    client_name: str = f"CognitePygen:{__version__}:GenerateSDK"
    api_subversion: str = "v1"
    timeout: float = 30.0
