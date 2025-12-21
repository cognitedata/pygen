class PygenAPIError(Exception):
    """Base class for all exceptions raised by the Pygen API client."""

    pass


class OAuth2Error(PygenAPIError):
    """Exception raised for OAuth2 authentication errors."""

    pass
