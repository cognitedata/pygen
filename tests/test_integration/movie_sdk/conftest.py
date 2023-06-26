import pytest

from examples.movie_domain.client import MovieClient


@pytest.fixture()
def movie_client(client_config) -> MovieClient:
    return MovieClient.azure_project(**client_config)
