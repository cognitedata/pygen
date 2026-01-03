"""Base credentials interface for authentication."""

from typing import Protocol


class Credentials(Protocol):
    """
    Abstract base class for authentication credentials.

    All credential types must inherit from this class and implement
    the get_headers method to provide authentication headers for HTTP requests.
    """

    def authorization_header(self) -> tuple[str, str]:
        """
        Get the authorization header for HTTP requests.

        Returns:
            A tuple containing the header name and header value.
        """
        raise NotImplementedError()
