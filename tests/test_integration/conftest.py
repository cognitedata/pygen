import os
import subprocess
from collections.abc import Iterable
from contextlib import suppress
from pathlib import Path

import pytest
from dotenv import load_dotenv

from cognite.pygen._client.auth import OAuth2ClientCredentials
from cognite.pygen._generation.python.instance_api.config import PygenClientConfig
from cognite.pygen._generation.python.instance_api.http_client import HTTPClient, RequestMessage, SuccessResponse


@pytest.fixture(scope="session")
def pygen_client_config() -> PygenClientConfig:
    repo_root = _repo_root()
    if repo_root is not None:
        dotenv_path = repo_root / ".env"
        if dotenv_path.exists():
            load_dotenv(dotenv_path, override=True)
    cluster = os.environ["CDF_CLUSTER"]
    project = os.environ["CDF_PROJECT"]
    return PygenClientConfig(
        credentials=OAuth2ClientCredentials(
            token_url=f"https://login.microsoftonline.com/{os.environ['IDP_TENANT_ID']}/oauth2/v2.0/token",
            client_id=os.environ["IDP_CLIENT_ID"],
            client_secret=os.environ["IDP_CLIENT_SECRET"],
            scopes=[f"https://{cluster}.cognitedata.com/.default"],
        ),
        cdf_url=f"https://{cluster}.cognitedata.com",
        project=project,
    )


@pytest.fixture(scope="session")
def http_client(pygen_client_config: PygenClientConfig) -> Iterable[HTTPClient]:
    with HTTPClient(pygen_client_config) as client:
        result = client.request_with_retries(
            RequestMessage(
                endpoint_url=f"{client.config.cdf_url}/api/v1/token/inspect",
                method="GET",
            )
        )
        if not isinstance(result, SuccessResponse):
            pytest.skip("Could not authenticate with the provided credentials")

        yield client


def test_is_authenticated(pygen_client_config: PygenClientConfig) -> None:
    with HTTPClient(pygen_client_config) as client:
        result = client.request_with_retries(
            RequestMessage(
                endpoint_url=f"{client.config.cdf_url}/api/v1/token/inspect",
                method="GET",
            )
        )
    assert isinstance(result, SuccessResponse)


def _repo_root() -> Path | None:
    with suppress(Exception):
        result = subprocess.run("git rev-parse --show-toplevel".split(), stdout=subprocess.PIPE)
        if (output := result.stdout.decode().strip()) != "":
            return Path(output)
    return None
