"""
Test module for the class `NativeKeyValueStoreAdapter` of the
`db`-subpackage.
"""

from time import sleep, time
from threading import Thread

import pytest

from dcm_common.db import (
    KeyValueStoreAdapter, NativeKeyValueStoreAdapter, MemoryStore
)
from dcm_common.db.key_value_store.adapter.native import NonThreadedNativeKeyValueStoreAdapter


@pytest.fixture(name="db")
def _db():
    return NonThreadedNativeKeyValueStoreAdapter(MemoryStore())


def test_interface_compliance():
    """
    Test whether `NativeKeyValueStoreAdapter` implements a
    `KeyValueStoreAdapter`.
    """
    assert issubclass(NativeKeyValueStoreAdapter, KeyValueStoreAdapter)
    NativeKeyValueStoreAdapter(MemoryStore())


def test_write(db: NativeKeyValueStoreAdapter):
    """Test method `write` of class `NativeKeyValueStoreAdapter`."""
    db.write("key", "value")


def test_read_non_existing(db: NativeKeyValueStoreAdapter):
    """
    Test method `read` of class `NativeKeyValueStoreAdapter` without existing
    key.
    """
    assert db.read("key") is None


def test_read(db: NativeKeyValueStoreAdapter):
    """Test method `read` of class `NativeKeyValueStoreAdapter`."""
    db.write("key", "value")
    assert db.read("key") == "value"


def test_read_pop(db: NativeKeyValueStoreAdapter):
    """
    Test method `read` of class `NativeKeyValueStoreAdapter` with
    `pop=True`.
    """
    db.write("key", "value")
    assert db.read("key", pop=True) == "value"
    assert db.read("key") is None


def test_next(db: NativeKeyValueStoreAdapter):
    """Test method `next` of class `NativeKeyValueStoreAdapter`."""
    db.write("key", "value")
    assert db.next() == ("key", "value")
    assert db.next(True) == ("key", "value")
    assert db.next(True) is None


def test_push(db: NativeKeyValueStoreAdapter):
    """Test method `push` of class `NativeKeyValueStoreAdapter`."""
    assert len(db.keys()) == 0
    key = db.push("value")
    assert key
    assert db.read(key) == "value"


def test_delete_non_existing(db: NativeKeyValueStoreAdapter):
    """
    Test method `delete` of class `NativeKeyValueStoreAdapter` without existing
    key.
    """
    db.delete("key")


def test_delete(db: NativeKeyValueStoreAdapter):
    """Test method `delete` of class `NativeKeyValueStoreAdapter`."""
    db.write("key", "value")
    db.delete("key")
    assert db.read("key") is None


def test_keys(db: NativeKeyValueStoreAdapter):
    """Test method `keys` of class `NativeKeyValueStoreAdapter`."""
    assert db.keys() == ()
    db.write("key", "value")
    assert db.keys() == ("key", )


@pytest.mark.parametrize(
    "adapter",
    [NonThreadedNativeKeyValueStoreAdapter, NativeKeyValueStoreAdapter],
    ids=["bad", "ok"]
)
def test_concurrency(adapter):
    """
    Test concurrency handling of `MemoryStore`.

    Start many threads that pop a value from the store and increment by
    one. Force concurrency issues by faking a slow backend, where
    reading and deleting (pop=True) are (sufficiently) far apart.
    """

    class SlowBackend(MemoryStore):
        def _delete(self, key):
            if key in self._database:
                sleep(0.01)
                del self._database[key]

    db = adapter(SlowBackend())

    n = 10
    db.write("key", 0)

    def increment():
        sleep(0.1)
        while (value := db.read("key", pop=True)) is None:
            pass
        db.write("key", value + 1)

    threads = [Thread(target=increment) for i in range(n)]

    for t in threads:
        t.start()

    assert all(t.is_alive() for t in threads)

    time0 = time()
    while any(t.is_alive() for t in threads) and time() - time0 < 2:
        sleep(0.01)

    assert all(not t.is_alive() for t in threads)
    if adapter == NativeKeyValueStoreAdapter:
        assert db.read("key") == n
    else:
        assert db.read("key") < n
