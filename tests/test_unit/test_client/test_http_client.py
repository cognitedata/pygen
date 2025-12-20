from collections.abc import Iterator
from unittest.mock import patch

import httpx
import pytest
import respx

from cognite.pygen._client.auth.credentials import Credentials
from cognite.pygen._client.config import PygenClientConfig
from cognite.pygen._client.exceptions import PygenAPIError
from cognite.pygen._client.http_client import FailedRequest, FailedResponse, HTTPClient, RequestMessage, SuccessResponse


class DummyCredentials(Credentials):
    def authorization_header(self) -> tuple[str, str]:
        return "Authorization", "Bearer dummy_token"


@pytest.fixture(scope="module")
def pygen_client_config() -> PygenClientConfig:
    return PygenClientConfig(DummyCredentials())


@pytest.fixture
def http_client(pygen_client_config: PygenClientConfig) -> Iterator[HTTPClient]:
    with HTTPClient(pygen_client_config) as client:
        yield client


@pytest.fixture
def http_client_one_retry(pygen_client_config: PygenClientConfig) -> Iterator[HTTPClient]:
    with HTTPClient(pygen_client_config, max_retries=1) as client:
        yield client


class TestHTTPClient:
    def test_get_request(self, respx_mock: respx.MockRouter, http_client: HTTPClient) -> None:
        respx_mock.get("https://example.com/api/resource").respond(json={"key": "value"}, status_code=200)
        response = http_client.request(
            RequestMessage(endpoint_url="https://example.com/api/resource", method="GET", parameters={"query": "test"})
        )
        assert isinstance(response, SuccessResponse)
        assert response.status_code == 200
        assert response.body == '{"key":"value"}'
        assert respx_mock.calls[-1].request.url == "https://example.com/api/resource?query=test"

    def test_post_request(self, respx_mock: respx.MockRouter, http_client: HTTPClient) -> None:
        respx_mock.post("https://example.com/api/resource").respond(
            json={"id": 123, "status": "created"}, status_code=201
        )
        response = http_client.request(
            RequestMessage(
                endpoint_url="https://example.com/api/resource",
                method="POST",
                body_content={"values": [float("nan")], "other": float("inf")},
                disable_gzip=True,
            )
        )
        assert isinstance(response, SuccessResponse)
        assert response.status_code == 201
        assert response.body == '{"id":123,"status":"created"}'
        assert respx_mock.calls[-1].request.content == b'{"values":[null],"other":null}'

    def test_failed_request(self, respx_mock: respx.MockRouter, http_client: HTTPClient) -> None:
        respx_mock.get("https://example.com/api/resource").respond(
            json={"error": {"message": "bad request", "code": 400}}, status_code=400
        )
        response = http_client.request(
            RequestMessage(endpoint_url="https://example.com/api/resource", method="GET", parameters={"query": "fail"})
        )
        assert isinstance(response, FailedResponse)
        assert response.status_code == 400
        assert response.error.message == "bad request"

    def test_retry_then_success(self, respx_mock: respx.MockRouter, http_client: HTTPClient) -> None:
        url = "https://example.com/api/resource"
        respx_mock.get(url).respond(json={"error": "service unavailable"}, status_code=503)
        respx_mock.get(url).respond(json={"key": "value"}, status_code=200)
        response = http_client.request_with_retries(RequestMessage(endpoint_url=url, method="GET", disable_gzip=True))
        assert isinstance(response, SuccessResponse)
        assert response.status_code == 200
        assert response.body == '{"key":"value"}'

    def test_retry_exhausted(self, http_client_one_retry: HTTPClient, respx_mock: respx.MockRouter) -> None:
        client = http_client_one_retry
        for _ in range(2):
            respx_mock.get("https://example.com/api/resource").respond(
                json={"error": {"message": "service unavailable", "code": 503}}, status_code=503
            )
        with patch("time.sleep"):  # Patch sleep to speed up the test
            response = client.request_with_retries(
                RequestMessage(endpoint_url="https://example.com/api/resource", method="GET")
            )

        assert isinstance(response, FailedResponse)
        assert response.status_code == 503
        assert response.error.message == "service unavailable"

    def test_connection_error(self, http_client_one_retry: HTTPClient, respx_mock: respx.MockRouter) -> None:
        http_client = http_client_one_retry
        respx_mock.get("http://nonexistent.domain/api/resource").mock(
            side_effect=httpx.ConnectError("Simulated connection error")
        )
        with patch(f"{HTTPClient.__module__}.time"):
            # Patch time to avoid actual sleep
            response = http_client.request_with_retries(
                RequestMessage(endpoint_url="http://nonexistent.domain/api/resource", method="GET")
            )
        assert isinstance(response, FailedRequest)
        assert "RequestException after 1 attempts (connect error): Simulated connection error" == response.error

    def test_read_timeout_error(self, http_client_one_retry: HTTPClient, respx_mock: respx.MockRouter) -> None:
        http_client = http_client_one_retry
        respx_mock.get("https://example.com/api/resource").mock(side_effect=httpx.ReadTimeout("Simulated read timeout"))
        with patch(f"{HTTPClient.__module__}.time"):
            # Patch time to avoid actual sleep
            response = http_client.request_with_retries(
                RequestMessage(endpoint_url="https://example.com/api/resource", method="GET")
            )
        assert isinstance(response, FailedRequest)
        assert "RequestException after 1 attempts (read error): Simulated read timeout" == response.error

    def test_zero_retries(self, pygen_client_config: PygenClientConfig, respx_mock: respx.MockRouter) -> None:
        client = HTTPClient(pygen_client_config, max_retries=0)
        respx_mock.get("https://example.com/api/resource").respond(
            json={"error": {"message": "service unavailable", "code": 503}}, status_code=503
        )
        with patch("time.sleep"):  # Patch sleep to speed up the test
            response = client.request_with_retries(
                RequestMessage(endpoint_url="https://example.com/api/resource", method="GET")
            )
        assert isinstance(response, FailedResponse)
        assert response.status_code == 503
        assert response.error.message == "service unavailable"
        assert len(respx_mock.calls) == 1

    def test_raise_if_already_retied(self, http_client_one_retry: HTTPClient) -> None:
        http_client = http_client_one_retry
        bad_request = RequestMessage(endpoint_url="https://example.com/api/resource", method="GET", status_attempt=3)
        with pytest.raises(RuntimeError, match=r"RequestMessage has already been attempted 3 times."):
            http_client.request_with_retries(bad_request)

    def test_error_text(self, http_client: HTTPClient, respx_mock: respx.MockRouter) -> None:
        respx_mock.get("https://example.com/api/resource").respond(json={"message": "plain_text"}, status_code=401)
        response = http_client.request(RequestMessage(endpoint_url="https://example.com/api/resource", method="GET"))
        assert isinstance(response, FailedResponse)
        assert response.status_code == 401
        assert response.error.message == '{"message":"plain_text"}'

    def test_request_alpha(self, http_client: HTTPClient, respx_mock: respx.MockRouter) -> None:
        respx_mock.get("https://example.com/api/alpha/endpoint").respond(json={"key": "value"}, status_code=200)
        response = http_client.request(
            RequestMessage(
                endpoint_url="https://example.com/api/alpha/endpoint",
                method="GET",
                parameters={"query": "test"},
                api_version="alpha",
            )
        )
        assert isinstance(response, SuccessResponse)
        assert response.status_code == 200
        assert respx_mock.calls[-1].request.headers["cdf-version"] == "alpha"

    def test_request_gzip(self, http_client: HTTPClient, respx_mock: respx.MockRouter) -> None:
        respx_mock.post("https://example.com/api/resource").respond(json={"key": "value"}, status_code=200)
        response = http_client.request(
            RequestMessage(
                endpoint_url="https://example.com/api/resource",
                method="POST",
                disable_gzip=False,
                body_content={"data": "test"},
            )
        )
        assert isinstance(response, SuccessResponse)
        assert response.status_code == 200
        assert respx_mock.calls[-1].request.headers.get("accept-encoding") == "gzip, deflate"
        request_content = respx_mock.calls[-1].request.content
        assert isinstance(request_content, bytes)
        assert request_content.startswith(b"\x1f\x8b")  # Gzip magic number

    def test_get_success_or_raise(self, respx_mock: respx.MockRouter, http_client_one_retry: HTTPClient) -> None:
        respx_mock.get("https://example.com/api/resource").respond(
            json={"error": {"message": "bad request", "code": 400}}, status_code=400
        )

        result = http_client_one_retry.request_with_retries(
            RequestMessage(endpoint_url="https://example.com/api/resource", method="GET")
        )
        with pytest.raises(PygenAPIError) as exc_info:
            result.get_success_or_raise()

        assert str(exc_info.value) == "Request failed with status code 400: bad request"

    def test_get_success_or_raise_failed_request(
        self, respx_mock: respx.MockRouter, http_client_one_retry: HTTPClient
    ) -> None:
        respx_mock.get("https://example.com/api/resource").mock(
            side_effect=httpx.ConnectError("Simulated connection error")
        )

        with patch(f"{HTTPClient.__module__}.time"):
            # Patch time to avoid actual sleep
            result = http_client_one_retry.request_with_retries(
                RequestMessage(endpoint_url="https://example.com/api/resource", method="GET")
            )
        with pytest.raises(PygenAPIError) as exc_info:
            result.get_success_or_raise()

        assert (
            str(exc_info.value) == "Request failed with error: RequestException after 1 attempts (connect error): "
            "Simulated connection error"
        )
