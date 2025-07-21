"""
Test module for the class `SQLiteAdapter3` of the `db`-subpackage.
"""

from uuid import uuid4
from sqlite3 import OperationalError

import pytest

from dcm_common.db import SQLiteAdapter3


@pytest.fixture(name="fixture_db_path")
def _fixture_db_path(fixtures):
    return fixtures / "test_data/sample_sqlite.db"


@pytest.fixture(name="sql_sample_schema")
def _sql_sample_schema(fixtures):
    return fixtures / "test_data/sample.sql"


def test_sqlite_constructor(fixture_db_path):
    """Test error-behavior of `SQLiteAdapter3`-constructor."""
    with pytest.raises(ValueError):  # bad args for in-memory
        SQLiteAdapter3()
    with pytest.raises(ValueError):  # bad args for in-memory
        SQLiteAdapter3(pool_size=2, allow_overflow=False)
    with pytest.raises(ValueError):  # bad args
        SQLiteAdapter3(fixture_db_path, pool_size=0, allow_overflow=False)

    SQLiteAdapter3(pool_size=1, allow_overflow=False)


def test_sqlite_initialize_existing_db(fixture_db_path):
    """
    Test proper initialization of an existing database using
    `SQLiteAdapter3`.
    """
    db = SQLiteAdapter3(fixture_db_path)

    assert sorted(db.get_table_names().eval()) == sorted(["table1", "table2"])

    rows_table1 = db.get_rows("table1")
    rows_table2 = db.get_rows("table2")

    assert len(rows_table1.data) == 1
    assert len(rows_table2.data) == 1
    assert rows_table1.data[0] == {
        "id": "5c5ed7d1-93bd-4443-a9d6-16bd49e77006",
        "name": "name table1",
    }
    assert rows_table2.data[0] == {
        "id": "961fa728-9548-4893-b1e2-ce84dfb44989",
        "name": "name table2",
        "table1_id": "5c5ed7d1-93bd-4443-a9d6-16bd49e77006",
    }

    assert db.get_primary_key("table1").data == "id"
    assert db.get_primary_key("table2").data == "id"


def test_sqlite_multiple_connections(temporary_directory):
    """
    Test proper initialization of an existing database using
    `SQLiteAdapter3`.
    """
    db = SQLiteAdapter3(
        db_file=temporary_directory / str(uuid4()), pool_size=2
    )

    c1 = db.pool.get_claim()
    c2 = db.pool.get_claim()

    # immediately visible - no error
    c1.execute("CREATE TABLE test_table (id text, value text)")
    c2.execute("SELECT * FROM test_table")

    # not shared during transaction
    c1.execute("BEGIN")
    c1.execute("CREATE TABLE test_table2 (id text, value text)")
    with pytest.raises(OperationalError):
        c2.execute("SELECT * FROM test_table2")
    c1.execute("COMMIT")
    c2.execute("SELECT * FROM test_table2")

    # rollback successful
    c1.execute("BEGIN")
    c1.execute("CREATE TABLE test_table3 (id text, value text)")
    with pytest.raises(OperationalError):
        c2.execute("SELECT * FROM test_table3")
    c1.execute("ROLLBACK")
    with pytest.raises(OperationalError):
        c2.execute("SELECT * FROM test_table3")


def test_sqlite_initialize_db_with_schema(sql_sample_schema):
    """
    Test proper initialization of an existing database with an sql schema
    using `SQLiteAdapter3`.
    """

    db = SQLiteAdapter3(allow_overflow=False)

    # read sql_sample_schema
    assert db.read_file(sql_sample_schema).success
    assert sorted(db.get_table_names().eval()) == sorted(
        ["sample_table", "sample_table2"]
    )


def test_sqlite_caching_table_names():
    """Test caching behavior of `SQLiteAdapter3.get_table_names`."""
    db = SQLiteAdapter3(allow_overflow=False)

    db.custom_cmd("CREATE TABLE test_table (id text, value text)")
    assert db.get_table_names().data == ["test_table"]
    db.custom_cmd("DROP TABLE test_table", clear_schema_cache=False)
    assert db.get_table_names().data == ["test_table"]
    db.custom_cmd("SELECT", clear_schema_cache=True)
    assert db.get_table_names().data == []


def test_sqlite_caching_column_type():
    """Test caching behavior of `SQLiteAdapter3.get_column_types`."""
    db = SQLiteAdapter3(allow_overflow=False)

    db.custom_cmd("CREATE TABLE test_table (id text, value text)")
    assert db.get_column_types("test_table").data["value"] == "text"
    db.custom_cmd("DROP TABLE test_table", clear_schema_cache=False)
    db.custom_cmd(
        "CREATE TABLE test_table (id text, value boolean)",
        clear_schema_cache=False,
    )
    assert db.get_column_types("test_table").data["value"] == "text"
    db.custom_cmd("SELECT", clear_schema_cache=True)
    assert db.get_column_types("test_table").data["value"] == "boolean"


def test_sqlite_caching_column_names():
    """Test caching behavior of `SQLiteAdapter3.get_column_names`."""
    db = SQLiteAdapter3(allow_overflow=False)

    db.custom_cmd("CREATE TABLE test_table (id text, value text)")
    assert sorted(db.get_column_names("test_table").data) == ["id", "value"]
    db.custom_cmd("DROP TABLE test_table", clear_schema_cache=False)
    db.custom_cmd(
        "CREATE TABLE test_table (id text, value2 text)",
        clear_schema_cache=False,
    )
    assert sorted(db.get_column_names("test_table").data) == ["id", "value"]
    db.custom_cmd("SELECT", clear_schema_cache=True)
    assert sorted(db.get_column_names("test_table").data) == ["id", "value2"]


def test_sqlite_caching_primary_key():
    """Test caching behavior of `SQLiteAdapter3.get_primary_key`."""
    db = SQLiteAdapter3(allow_overflow=False)

    db.custom_cmd("CREATE TABLE test_table (id text primary key)")
    assert db.get_primary_key("test_table").data == "id"
    db.custom_cmd("DROP TABLE test_table", clear_schema_cache=False)
    db.custom_cmd(
        "CREATE TABLE test_table (id2 text primary key)",
        clear_schema_cache=False,
    )
    assert db.get_primary_key("test_table").data == "id"
    db.custom_cmd("SELECT", clear_schema_cache=True)
    assert db.get_primary_key("test_table").data == "id2"
