import random
import sys
import time
from collections.abc import MutableMapping, Set
from typing import Literal

import httpx

from cognite.pygen._generation.python.instance_api.config import PygenClientConfig

from ._data_classes import (
    ErrorDetails,
    FailedRequest,
    FailedResponse,
    HTTPResult,
    RequestMessage,
    SuccessResponse,
)
from ._header import get_current_pygen_version, get_user_agent

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class HTTPClient:
    """An HTTP client.

    This class handles rate limiting, retries, and error handling for HTTP requests.

    Args:
        config (PygenClientConfig): Configuration for the Toolkit client.
        pool_connections (int): The number of connection pools to cache. Default is 10.
        pool_maxsize (int): The maximum number of connections to save in the pool. Default
            is 20.
        max_retries (int): The maximum number of retries for a request. Default is 10.
        retry_status_codes (frozenset[int]): HTTP status codes that should trigger a retry.
            Default is {408, 429, 502, 503, 504}.

    """

    def __init__(
        self,
        config: PygenClientConfig,
        max_retries: int = 10,
        pool_connections: int = 10,
        pool_maxsize: int = 20,
        retry_status_codes: Set[int] = frozenset({408, 429, 502, 503, 504}),
        max_retry_backoff: int = 60,
    ):
        self.config = config
        self._max_retries = max_retries
        self._pool_connections = pool_connections
        self._pool_maxsize = pool_maxsize
        self._retry_status_codes = retry_status_codes
        self._max_retry_backoff = max_retry_backoff

        # Thread-safe session for connection pooling
        self.session = self._create_thread_safe_session()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: object | None
    ) -> Literal[False]:
        """Close the session when exiting the context."""
        self.session.close()
        return False  # Do not suppress exceptions

    def _create_thread_safe_session(self) -> httpx.Client:
        return httpx.Client(
            limits=httpx.Limits(
                max_connections=self._pool_maxsize,
                max_keepalive_connections=self._pool_connections,
            ),
            timeout=self.config.timeout,
        )

    def _create_headers(
        self,
        api_version: str | None = None,
        content_type: str = "application/json",
        accept: str = "application/json",
        content_length: int | None = None,
        disable_gzip: bool = False,
    ) -> MutableMapping[str, str]:
        headers: MutableMapping[str, str] = {}
        headers["User-Agent"] = f"httpx/{httpx.__version__} {get_user_agent()}"
        auth_name, auth_value = self.config.credentials.authorization_header()
        headers[auth_name] = auth_value
        headers["Content-Type"] = content_type
        if content_length is not None:
            headers["Content-Length"] = str(content_length)
        headers["accept"] = accept
        headers["x-cdp-sdk"] = f"CognitePygen:{get_current_pygen_version()}"
        headers["x-cdp-app"] = self.config.client_name
        headers["cdf-version"] = api_version or self.config.api_subversion
        if not disable_gzip and content_length is None:
            headers["Content-Encoding"] = "gzip"
        return headers

    @staticmethod
    def _get_retry_after_in_header(response: httpx.Response) -> float | None:
        if "Retry-After" not in response.headers:
            return None
        try:
            return float(response.headers["Retry-After"])
        except ValueError:
            # Ignore invalid Retry-After header
            return None

    def _backoff_time(self, attempts: int) -> float:
        backoff_time = 0.5 * (2**attempts)
        return min(backoff_time, self._max_retry_backoff) * random.uniform(0, 1.0)

    def request(self, message: RequestMessage) -> RequestMessage | HTTPResult:
        """Send an HTTP request and return the response.

        Args:
            message (RequestMessage): The request message to send.
        Returns:
            HTTPMessage: The response message.
        """
        try:
            response = self._make_request(message)
            result = self._handle_response(response, message)
        except Exception as e:
            result = self._handle_error(e, message)
        return result

    def request_with_retries(self, message: RequestMessage) -> HTTPResult:
        """Send an HTTP request and handle retries.

        This method will keep retrying the request until it either succeeds or
        exhausts the maximum number of retries.

        Note this method will use the current thread to process all request, thus
        it is blocking.

        Args:
            message (RequestMessage): The request message to send.
        Returns:
            HTTPMessage: The final response message, which can be either successful response or failed request.
        """
        if message.total_attempts > 0:
            raise RuntimeError(f"RequestMessage has already been attempted {message.total_attempts} times.")
        current_request = message
        while True:
            result = self.request(current_request)
            if isinstance(result, RequestMessage):
                current_request = result
            elif isinstance(result, HTTPResult):
                return result
            else:
                raise TypeError(f"Unexpected result type: {type(result)}")

    def _make_request(self, message: RequestMessage) -> httpx.Response:
        headers = self._create_headers(
            message.api_version, message.content_type, message.accept, disable_gzip=message.disable_gzip
        )
        return self.session.request(
            method=message.method,
            url=message.endpoint_url,
            content=message.content,
            headers=headers,
            params=message.parameters,
            timeout=self.config.timeout,
            follow_redirects=False,
        )

    def _handle_response(self, response: httpx.Response, request: RequestMessage) -> RequestMessage | HTTPResult:
        if 200 <= response.status_code < 300:
            return SuccessResponse(
                status_code=response.status_code,
                body=response.text,
                content=response.content,
            )
        if retry_request := self._retry_request(response, request):
            return retry_request
        else:
            # Permanent failure
            return FailedResponse(
                status_code=response.status_code,
                body=response.text,
                error=ErrorDetails.from_response(response),
            )

    def _retry_request(self, response: httpx.Response, request: RequestMessage) -> RequestMessage | None:
        retry_after = self._get_retry_after_in_header(response)
        if retry_after is not None and response.status_code == 429 and request.status_attempt < self._max_retries:
            request.status_attempt += 1
            time.sleep(retry_after)
            return request

        if request.status_attempt < self._max_retries and response.status_code in self._retry_status_codes:
            request.status_attempt += 1
            time.sleep(self._backoff_time(request.total_attempts))
            return request
        return None

    def _handle_error(self, e: Exception, request: RequestMessage) -> RequestMessage | HTTPResult:
        if isinstance(e, httpx.ReadTimeout | httpx.TimeoutException):
            error_type = "read"
            request.read_attempt += 1
            attempts = request.read_attempt
        elif isinstance(e, ConnectionError | httpx.ConnectError | httpx.ConnectTimeout):
            error_type = "connect"
            request.connect_attempt += 1
            attempts = request.connect_attempt
        else:
            error_msg = f"Unexpected exception: {e!s}"
            return FailedRequest(error=error_msg)

        if attempts <= self._max_retries:
            time.sleep(self._backoff_time(request.total_attempts))
            return request
        else:
            # We have already incremented the attempt count, so we subtract 1 here
            error_msg = f"RequestException after {request.total_attempts - 1} attempts ({error_type} error): {e!s}"

            return FailedRequest(error=error_msg)
