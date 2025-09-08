"""
Test module for the class `HTTPKeyValueStoreAdapter` of the
`db`-subpackage.
"""

import pytest

from dcm_common.services.tests.fixtures import run_service, external_service
from dcm_common.db import (
    KeyValueStoreAdapter,
    HTTPKeyValueStoreAdapter,
    MemoryStore,
    key_value_store_app_factory,
)


@pytest.fixture(name="db_port")
def _db_port():
    return 8080


@pytest.fixture(name="db")
def _db(db_port):
    return HTTPKeyValueStoreAdapter(f"http://localhost:{db_port}")


@pytest.fixture(name="db_app")
def _db_app(db_port, run_service):
    run_service(
        key_value_store_app_factory(MemoryStore(), "test-db"), port=db_port
    )


def test_interface_compliance():
    """
    Test whether `HTTPKeyValueStoreAdapter` implements a
    `KeyValueStoreAdapter`.
    """
    assert issubclass(HTTPKeyValueStoreAdapter, KeyValueStoreAdapter)
    HTTPKeyValueStoreAdapter("")


def test_write(db: HTTPKeyValueStoreAdapter, db_app):
    """Test method `write` of class `HTTPKeyValueStoreAdapter`."""
    db.write("key", "value")


def test_write_null(db: HTTPKeyValueStoreAdapter, db_app):
    """
    Test method `write` of class `HTTPKeyValueStoreAdapter` when writing
    null.
    """
    db.write("key", None)
    assert "key" in db.keys()
    assert db.read("key") is None


def test_read_non_existing(db: HTTPKeyValueStoreAdapter, db_app):
    """
    Test method `read` of class `HTTPKeyValueStoreAdapter` without existing
    key.
    """
    assert db.read("key") is None


def test_read(db: HTTPKeyValueStoreAdapter, db_app):
    """Test method `read` of class `HTTPKeyValueStoreAdapter`."""
    db.write("key", "value")
    assert db.read("key") == "value"


def test_read_pop(db: HTTPKeyValueStoreAdapter, db_app):
    """
    Test method `read` of class `HTTPKeyValueStoreAdapter` with
    `pop=True`.
    """
    db.write("key", "value")
    assert db.read("key", pop=True) == "value"
    assert db.read("key") is None


def test_next(db: HTTPKeyValueStoreAdapter, db_app):
    """Test method `next` of class `HTTPKeyValueStoreAdapter`."""
    db.write("key", "value")
    assert db.next() == ("key", "value")
    assert db.next(True) == ("key", "value")
    assert db.next(True) is None


def test_push(db: HTTPKeyValueStoreAdapter, db_app):
    """Test method `push` of class `HTTPKeyValueStoreAdapter`."""
    assert len(db.keys()) == 0
    key = db.push("value")
    assert key
    assert db.read(key) == "value"


def test_push_null(db: HTTPKeyValueStoreAdapter, db_app):
    """
    Test method `push` of class `HTTPKeyValueStoreAdapter` when writing
    null.
    """
    assert len(db.keys()) == 0
    key = db.push(None)
    assert key
    assert "<!doctype html>" not in key
    assert db.read(key) is None


def test_delete_non_existing(db: HTTPKeyValueStoreAdapter, db_app):
    """
    Test method `delete` of class `HTTPKeyValueStoreAdapter` without existing
    key.
    """
    db.delete("key")


def test_delete(db: HTTPKeyValueStoreAdapter, db_app):
    """Test method `delete` of class `HTTPKeyValueStoreAdapter`."""
    db.write("key", "value")
    db.delete("key")
    assert db.read("key") is None


def test_keys(db: HTTPKeyValueStoreAdapter, db_app):
    """Test method `keys` of class `HTTPKeyValueStoreAdapter`."""
    assert db.keys() == ()
    db.write("key", "value")
    assert db.keys() == ("key",)
