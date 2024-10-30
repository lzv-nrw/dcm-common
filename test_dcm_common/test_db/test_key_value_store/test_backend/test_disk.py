"""
Test module for the class `JSONFileStore` of the `db`-subpackage.
"""

from uuid import uuid4

import pytest

from dcm_common.util import list_directory_content
from dcm_common.db import KeyValueStore, JSONFileStore


@pytest.fixture(name="db_dir")
def _db_dir(temporary_directory):
    return temporary_directory / "db"


@pytest.fixture(name="fkvs")
def _fkvs(db_dir):
    return JSONFileStore(db_dir / str(uuid4()))


@pytest.fixture(name="db_file_storage")
def _db_file_storage(db_dir):
    """
    Generates non-empty JSONFileStore-persistent storage directory.
    """
    dir_ = db_dir / str(uuid4())
    fkvs = JSONFileStore(dir_)
    fkvs.write("key", "value")
    return dir_


def test_interface_compliance(db_dir):
    """
    Test whether `JSONFileStore` implements a `KeyValueStore`.
    """
    assert issubclass(JSONFileStore, KeyValueStore)
    JSONFileStore(db_dir / str(uuid4()))


def test_write(fkvs: JSONFileStore):
    """Test method `write` of class `JSONFileStore`."""
    fkvs.write("key", "value")
    fkvs.write("key", 1)
    fkvs.write("key", {})
    with pytest.raises(TypeError):
        fkvs.write("key", lambda x: None)


def test_read_non_existing(fkvs: JSONFileStore):
    """
    Test method `read` of class `JSONFileStore` without existing
    key.
    """
    assert fkvs.read("key") is None


def test_read(fkvs: JSONFileStore):
    """Test method `read` of class `JSONFileStore`."""
    for value in (
        "value", 1, 1.25, True, ["value", True], {"p1": 1, "p2": "value"},
    ):
        fkvs.write("key", value)
        assert fkvs.read("key") == value


def test_delete_non_existing(fkvs: JSONFileStore):
    """
    Test method `delete` of class `JSONFileStore` without existing
    key.
    """
    fkvs.delete("key")


def test_delete(fkvs: JSONFileStore):
    """Test method `delete` of class `JSONFileStore`."""
    fkvs.write("key", "value")
    fkvs.delete("key")
    assert fkvs.read("key") is None


def test_keys(fkvs: JSONFileStore):
    """Test method `keys` of class `JSONFileStore`."""
    assert fkvs.keys() == ()
    fkvs.write("key", "value")
    assert fkvs.keys() == ("key", )


def test_persistence_read(db_file_storage):
    """
    Test persistent storage of class `JSONFileStore` by instantiating
    two instances (reading from disk).
    """
    fkvs = JSONFileStore(db_file_storage)
    assert fkvs.read("key") == "value"


def test_persistence_delete(db_file_storage):
    """
    Test persistent storage of class `JSONFileStore` by instantiating
    two instances (deleting from disk).
    """
    file = list_directory_content(db_file_storage)[0]
    assert file.is_file()
    fkvs = JSONFileStore(db_file_storage)
    fkvs.delete("key")
    assert not file.is_file()
    assert fkvs.read("key") is None


def test_persistence_keys(db_file_storage):
    """
    Test persistent storage of class `JSONFileStore` by instantiating
    two instances (keys from disk).
    """
    fkvs = JSONFileStore(db_file_storage)
    assert fkvs.keys() == ("key", )


def test_persistence_keys_caching(db_file_storage):
    """
    Test use of persistent storage of class `JSONFileStore` by
    manipulation of records after fully loading cache.
    """
    fkvs = JSONFileStore(db_file_storage)
    fkvs.write("key", "another-value")
    # cache all records
    fkvs.keys()
    # manipulate
    for file in list_directory_content(db_file_storage):
        text = file.read_text(encoding="utf-8")
        file.write_text(text.replace("another-value", "yet-another-value"))
    # assert correct bahavior
    assert fkvs.read("key") == "another-value"  # from cache
    fkvs.keys()
    assert fkvs.read("key") == "another-value"  # still from cache


def test_bad_files(db_dir):
    """
    Test behavior of class `JSONFileStore` for bad data in working
    directory.
    """
    dir_ = db_dir / str(uuid4())
    (dir_ / "another-dir").mkdir(parents=True)
    (dir_ / "another-dir" / "ok-format").write_text(
        '{"key": "bad-key", "value": "value"}', encoding="utf-8"
    )
    (dir_ / "empty-file").touch()
    (dir_ / "bad-format").write_text("something different", encoding="utf-8")
    (dir_ / "ok-format").write_text(
        '{"key": "key", "value": "value"}', encoding="utf-8"
    )
    assert JSONFileStore(dir_).keys() == ("key", )
