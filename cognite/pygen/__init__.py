"""
This is the main entry point for the pygen package. It contains the main functions for
generating SDKs.
"""

from cognite.pygen._generator.generator_functions import generate_sdk as generate_sdk_v2
from cognite.pygen._generator.generator_functions import generate_sdk_notebook as generate_sdk_notebook_v2
from cognite.pygen._legacy._build import build_wheel
from cognite.pygen._legacy._generator import generate_sdk, generate_sdk_notebook
from cognite.pygen._legacy._query import QueryExecutor as _QueryExecutor
from cognite.pygen._legacy.utils.cdf import load_cognite_client_from_toml

from ._version import __version__

__all__ = [
    "__version__",
    "generate_sdk",
    "generate_sdk_v2",
    "generate_sdk_notebook",
    "generate_sdk_notebook_v2",
    "build_wheel",
    "load_cognite_client_from_toml",
    "_QueryExecutor",
]
