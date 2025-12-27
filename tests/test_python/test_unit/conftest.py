import pytest

from cognite.pygen._generation.python.instance_api.auth.credentials import Credentials
from cognite.pygen._generation.python.instance_api.config import PygenClientConfig


class MockCredentials(Credentials):
    def authorization_header(self) -> tuple[str, str]:
        return "Authorization", "Bearer dummy_token"


@pytest.fixture(scope="session")
def pygen_client_config() -> PygenClientConfig:
    return PygenClientConfig("https://example.com", "test_project", MockCredentials())
