import pytest

from cognite.fdm.cdf.client_fdm_v3 import DataModelStorageAPI
from tests.test_fdm.test_cdf._utils import *  # noqa


@pytest.fixture
def make_api(mocker, make_cognite_client):
    def _make_api(responses):
        return DataModelStorageAPI(
            config=mocker.MagicMock(project="mock_proj"),
            api_version="mock_api_version",
            cognite_client=make_cognite_client(responses),
        )

    return _make_api


def test_post_to_endpoint(make_api):
    mock_api = make_api([{"mock": "post response"}])
    value = mock_api._post_to_endpoint({"mock": "data"}, "/mock_endpoint")
    assert value == {"mock": "post response"}
    mock_api._cognite_client.post.assert_called_once_with(
        "/api/v1/projects/mock_proj/mock_endpoint", json={"mock": "data"}
    )


def test_get_from_endpoint(make_api):
    mock_api = make_api([{"mock": "get response"}])
    value = mock_api._get_from_endpoint({"mock": "data"}, "/mock_endpoint")
    assert value == {"mock": "get response"}
    mock_api._cognite_client.get.assert_called_once_with("/api/v1/projects/mock_proj/mock_endpoint?mock=data")


def test_post_to_endpoint_w_cursor(make_api):
    mock_api = make_api([{"items": ["A", "B"], "nextCursor": "neeextAbC"}, {"items": ["C"]}])
    value = mock_api._post_to_endpoint({"mock": "data"}, "/mock_endpoint")
    assert value == {"items": ["A", "B", "C"]}
    assert mock_api._cognite_client.post.call_count == 2
    mock_api._cognite_client.post.assert_any_call("/api/v1/projects/mock_proj/mock_endpoint", json={"mock": "data"})
    mock_api._cognite_client.post.assert_any_call(
        "/api/v1/projects/mock_proj/mock_endpoint", json={"cursor": "neeextAbC", "mock": "data"}
    )


def test_get_from_endpoint_w_cursor(make_api):
    mock_api = make_api([{"items": ["A", "B"], "nextCursor": "neeextAbC"}, {"items": ["C"]}])
    value = mock_api._get_from_endpoint({"mock": "data"}, "/mock_endpoint")
    assert value == {"items": ["A", "B", "C"]}
    assert mock_api._cognite_client.get.call_count == 2
    mock_api._cognite_client.get.assert_any_call("/api/v1/projects/mock_proj/mock_endpoint?mock=data")
    mock_api._cognite_client.get.assert_any_call("/api/v1/projects/mock_proj/mock_endpoint?mock=data&cursor=neeextAbC")
