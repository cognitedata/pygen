"""Base credentials interface for authentication."""

from abc import ABC, abstractmethod


class Credentials(ABC):
    """
    Abstract base class for authentication credentials.

    All credential types must inherit from this class and implement
    the get_headers method to provide authentication headers for HTTP requests.
    """

    @abstractmethod
    def authorization_header(self) -> tuple[str, str]:
        """
        Get the authorization header for HTTP requests.

        Returns:
            A tuple containing the header name and header value.
        """
        ...

    @abstractmethod
    def refresh_if_needed(self) -> None:
        """
        Refresh credentials if they are expired or about to expire.

        This method should be called before making API requests to ensure
        credentials are valid.
        """
        ...
