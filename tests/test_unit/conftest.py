import pytest

from cognite.pygen._generator import CodeFormatter
from cognite.pygen.config import PygenConfig


@pytest.fixture(scope="session")
def code_formatter() -> CodeFormatter:
    return CodeFormatter(format_code=True, logger=print, default_line_length=120)


@pytest.fixture(scope="session")
def pygen_config() -> PygenConfig:
    return PygenConfig()
