"""Basic tests to verify v2 test infrastructure."""

import pytest


def test_basic_import():
    """Test that we can import the v2 pygen package."""
    import cognite.pygen

    assert cognite.pygen.__version__ == "2.0.0-dev"


def test_basic_assertion():
    """Test that basic assertions work."""
    assert 1 + 1 == 2


@pytest.mark.v2
def test_v2_marker():
    """Test that v2 marker works."""
    assert True
