import warnings
from unittest.mock import MagicMock

from cognite.client.data_classes.data_modeling import ViewId

from cognite.pygen._warnings import PydanticNamespaceCollisionWarning, print_warnings


class TestPygenWarnings:
    def test_skip_pydantic_namespace_warning(self) -> None:
        console = MagicMock(spec=print)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            PydanticNamespaceCollisionWarning(ViewId("namespace", "name", "version"), "name").warn()

        print_warnings(w, console)
        assert console.call_count == 0
