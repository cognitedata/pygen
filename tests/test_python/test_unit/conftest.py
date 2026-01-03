import pytest

from cognite.pygen._python.instance_api.auth.credentials import Credentials
from cognite.pygen._python.instance_api.config import PygenClientConfig


class MockCredentials(Credentials):
    def authorization_header(self) -> tuple[str, str]:
        return "Authorization", "Bearer dummy_token"


@pytest.fixture()
def pygen_client_config() -> PygenClientConfig:
    return PygenClientConfig(cdf_url="https://example.com", project="test_project", credentials=MockCredentials())
