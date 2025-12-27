from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .http_client import FailedRequest, FailedResponse
    from .models import UpsertResult


class PygenAPIError(Exception):
    """Base class for all exceptions raised by the Pygen API client."""

    pass


class OAuth2Error(PygenAPIError):
    """Exception raised for OAuth2 authentication errors."""

    pass


class MultiRequestError(PygenAPIError):
    """Exception raised when multiple requests are executed and at least one fails.

    Attributes:
        failed_responses: List of failed responses.
        failed_requests: List of failed requests.
        result: The successful part of the operation.
    """

    def __init__(
        self, failed_responses: "list[FailedResponse]", failed_requests: "list[FailedRequest]", result: "UpsertResult"
    ) -> None:
        self.failed_responses = failed_responses
        self.failed_requests = failed_requests
        self.result = result
        super().__init__(self._create_message())

    def _create_message(self) -> str:
        parts = []
        if self.failed_responses:
            parts.append(f"{len(self.failed_responses)} failed responses")
        if self.failed_requests:
            parts.append(f"{len(self.failed_requests)} failed requests")
        return f"{type(self).__name__}: {'; '.join(parts)}"
