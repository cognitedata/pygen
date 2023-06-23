import pytest
from examples.movie_domain.sdk import MovieClient


@pytest.fixture()
def movie_client() -> MovieClient:
    return MovieClient()
