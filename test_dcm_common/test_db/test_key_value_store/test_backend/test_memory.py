"""
Test module for the class `MemoryStore` of the `db`-subpackage.
"""

import pytest

from dcm_common.db import KeyValueStore, MemoryStore


@pytest.fixture(name="mkvs")
def _mkvs():
    return MemoryStore()


def test_interface_compliance():
    """
    Test whether `MemoryStore` implements a `KeyValueStore`.
    """
    assert issubclass(MemoryStore, KeyValueStore)
    MemoryStore()


def test_write(mkvs: MemoryStore):
    """Test method `write` of class `MemoryStore`."""
    mkvs.write("key", "value")
    mkvs.write("key", 1)
    mkvs.write("key", {})
    mkvs.write("key", lambda x: None)


def test_read_non_existing(mkvs: MemoryStore):
    """
    Test method `read` of class `MemoryStore` without existing
    key.
    """
    assert mkvs.read("key") is None


def test_read(mkvs: MemoryStore):
    """Test method `read` of class `MemoryStore`."""
    mkvs.write("key", "value")
    assert mkvs.read("key") == "value"


def test_delete_non_existing(mkvs: MemoryStore):
    """
    Test method `delete` of class `MemoryStore` without existing
    key.
    """
    mkvs.delete("key")


def test_delete(mkvs: MemoryStore):
    """Test method `delete` of class `MemoryStore`."""
    mkvs.write("key", "value")
    mkvs.delete("key")
    assert mkvs.read("key") is None


def test_keys(mkvs: MemoryStore):
    """Test method `keys` of class `MemoryStore`."""
    assert mkvs.keys() == ()
    mkvs.write("key", "value")
    assert mkvs.keys() == ("key",)
