import pytest

from examples.movie_domain.client import MovieClient


@pytest.fixture()
def movie_client() -> MovieClient:
    return MovieClient()
