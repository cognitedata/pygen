import pytest

from tests.constants import IS_PYDANTIC_V1

if IS_PYDANTIC_V1:
    from movie_domain_pydantic_v1.client import MovieClient
else:
    from movie_domain.client import MovieClient


@pytest.fixture()
def movie_client(client_config) -> MovieClient:
    return MovieClient.azure_project(**client_config)
