import inspect
import sys
from abc import ABC
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Literal, TypeVar
from unittest.mock import MagicMock

from cognite.pygen._client import ContainersAPI, DataModelsAPI, PygenClient, SpacesAPI, ViewsAPI
from cognite.pygen._python.instance_api.config import PygenClientConfig

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


T_Cls = TypeVar("T_Cls")


def get_concrete_subclasses(base_cls: type[T_Cls], exclude_direct_abc_inheritance: bool = True) -> list[type[T_Cls]]:
    """
    Returns a list of all concrete subclasses of the given base class.
    Args:
        base_cls (type[T_Cls]): The base class to find subclasses for.
        exclude_direct_abc_inheritance (bool): If True, excludes classes that directly inherit from `abc.ABC`.
            This is used as a marker to filter out intermediate base classes. Defaults to True.
    Returns:
        list[type[T_Cls]]: A list of concrete subclasses of the base class.
    """
    to_check = [base_cls]
    subclasses: list[type[T_Cls]] = []
    seen: set[type[T_Cls]] = {base_cls}
    while to_check:
        current_cls = to_check.pop()
        for subclass in current_cls.__subclasses__():
            if subclass in seen:
                continue
            if (not inspect.isabstract(subclass)) and (
                not exclude_direct_abc_inheritance or ABC not in subclass.__bases__
            ):
                subclasses.append(subclass)
            seen.add(subclass)
            to_check.append(subclass)
    return subclasses


class PygenClientMock:
    """Mock for ToolkitClient object

    All APIs are replaced with specked MagicMock objects.
    """

    def __init__(self, config: PygenClientConfig | None = None, max_retries: int = 10) -> None:
        """Initialize the Pygen client.

        Args:
            config: Configuration for the client including URL, project, and credentials.
            max_retries: Maximum number of retries for failed requests. Default is 10.
        """
        self.config = config or MagicMock(spec=PygenClientConfig)

        self.spaces = MagicMock(spec=SpacesAPI)
        self.data_models = MagicMock(spec_set=DataModelsAPI)
        self.views = MagicMock(spec_set=ViewsAPI)
        self.containers = MagicMock(spec_set=ContainersAPI)

    def __enter__(self) -> Self:
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> Literal[False]:
        """Exit context manager and close the HTTP client."""
        self.close()
        return False

    def close(self) -> None:
        """Close the client and release resources.

        This method should be called when the client is no longer needed
        to properly close HTTP connections.
        """
        ...


@contextmanager
def monkeypatch_pygen_client() -> Iterator[PygenClientMock]:
    pygen_client_mock = PygenClientMock()
    try:
        PygenClient.__new__ = lambda *args, **kwargs: pygen_client_mock  # type: ignore[assignment, method-assign]
        yield pygen_client_mock
    finally:
        PygenClient.__new__ = lambda cls, *args, **kwargs: object.__new__(cls)  # type: ignore[assignment, method-assign]
