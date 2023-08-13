import pytest

from cognite.pygen._generator import CodeFormatter


@pytest.fixture(scope="session")
def code_formatter() -> CodeFormatter:
    return CodeFormatter(format_code=True, logger=print, default_line_length=120)
