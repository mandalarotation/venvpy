"""Test venvpy module."""

from venvpy import FOO


def test_init():
    """Test __init__ core module."""
    assert FOO == "FOO"
