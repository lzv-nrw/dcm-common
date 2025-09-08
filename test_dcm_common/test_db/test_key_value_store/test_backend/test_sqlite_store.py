"""
Test module for the class `SQLiteStore` of the `db`-subpackage.
"""

from uuid import uuid4
import threading

import pytest

from dcm_common.orchestra import DilledProcess
from dcm_common.db import KeyValueStore, SQLiteStore


@pytest.fixture(name="store")
def _store():
    return SQLiteStore()


def test_interface_compliance():
    """
    Test whether `SQLiteStore` implements a `KeyValueStore`.
    """
    assert issubclass(SQLiteStore, KeyValueStore)
    SQLiteStore()


def test_write(store: SQLiteStore):
    """Test method `write` of class `SQLiteStore`."""
    store.write("key", "value")
    store.write("key", 1)
    store.write("key", {})


def test_read_non_existing(store: SQLiteStore):
    """
    Test method `read` of class `SQLiteStore` without existing
    key.
    """
    assert store.read("key") is None


def test_read(store: SQLiteStore):
    """Test method `read` of class `SQLiteStore`."""
    store.write("key", "value")
    assert store.read("key") == "value"
    store.write("key2", {"inner": "value"})
    assert store.read("key2") == {"inner": "value"}


def test_delete_non_existing(store: SQLiteStore):
    """
    Test method `delete` of class `SQLiteStore` without existing
    key.
    """
    store.delete("key")


def test_delete(store: SQLiteStore):
    """Test method `delete` of class `SQLiteStore`."""
    store.write("key", "value")
    store.delete("key")
    assert store.read("key") is None


def test_keys(store: SQLiteStore):
    """Test method `keys` of class `SQLiteStore`."""
    assert store.keys() == ()
    store.write("key", "value")
    assert store.keys() == ("key",)


def test_write_update(store: SQLiteStore):
    """Test updating records in `SQLiteStore`."""
    store.write("key", "value")
    assert store.read("key") == "value"
    store.write("key", 1)
    assert store.read("key") == 1
    assert len(store.keys()) == 1


def test_multiple_stores_mem():
    """Test multiple `SQLiteStore`s on same db (in memory)."""
    store1 = SQLiteStore(memory_id="test")
    store2 = SQLiteStore(memory_id="test")
    store1.write("key", {})
    assert store2.keys() == ("key",)
    store2.write("key2", {})
    assert sorted(list(store1.keys())) == list(("key", "key2"))
    store3 = SQLiteStore(memory_id="test2")
    assert len(store3.keys()) == 0


def test_multiple_stores_file(temporary_directory):
    """Test multiple `SQLiteStore`s on same db (on disk)."""
    db_file = temporary_directory / str(uuid4())
    store1 = SQLiteStore(db_file)
    assert db_file.is_file()
    store2 = SQLiteStore(db_file)
    store1.write("key", {})
    assert store2.keys() == ("key",)
    store2.write("key2", {})
    assert sorted(list(store1.keys())) == list(("key", "key2"))


def test_concurrency_threading(store: SQLiteStore):
    """Test handling of `threading`-concurrency in `SQLiteStore`."""
    n_records = 50
    n_threads = 10

    def insert_data():
        for i in range(n_records):
            store.write(str(i), i)

    threads = [threading.Thread(target=insert_data) for i in range(n_threads)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert len(store.keys()) == n_records


def test_concurrency_multiprocessing(temporary_directory):
    """
    Test handling of `multiprocessing`-concurrency in `SQLiteStore`.
    """
    db_file = temporary_directory / str(uuid4())
    store = SQLiteStore(db_file)
    n_records = 50
    n_processes = 10

    def insert_data():
        process_store = SQLiteStore(db_file)
        for i in range(n_records):
            process_store.write(str(i), i)

    processes = [DilledProcess(target=insert_data) for i in range(n_processes)]

    for process in processes:
        process.start()

    for process in processes:
        process.join()

    assert len(store.keys()) == n_records
