"""
Test module for the class `PostgreSQLAdapter14` of the `db.sql`-
subpackage. See test-module `test_sql_adapters.py` for instructions on
how to setup postgres for these tests.
"""

import pytest

from dcm_common.db import psycopg, PostgreSQLAdapterSQL14


def check_requirements():
    if psycopg is None:
        return False, "Unable to import package 'psycopg'."
    try:
        db = PostgreSQLAdapterSQL14(
            host="localhost",
            port="5432",
            database="postgres",
            user="postgres",
            password="foo",
        )
    except psycopg.OperationalError as exc_info:
        return False, str(exc_info)
    db.pool.close()
    return True, ""


requirements_met, skip_reason = check_requirements()


@pytest.fixture(name="db")
def _db(request):
    db = PostgreSQLAdapterSQL14(
        host="localhost",
        port="5432",
        database="postgres",
        user="postgres",
        password="foo",
    )
    # cleanup
    db.custom_cmd("DROP DATABASE test")  # delete testing-database
    db.custom_cmd("CREATE DATABASE test").eval()  # re-create testing-database
    db.pool.close()

    db = PostgreSQLAdapterSQL14(
        host="localhost",
        port="5432",
        database="test",
        user="postgres",
        password="foo",
    )

    request.addfinalizer(db.pool.close)

    return db


@pytest.mark.skipif(not requirements_met, reason=skip_reason)
def test_postgres_caching_table_names(db: PostgreSQLAdapterSQL14):
    """Test caching behavior of `PostgreSQLAdapterSQL14.get_table_names`."""

    db.custom_cmd("CREATE TABLE test_table (id text, value text)")
    assert "test_table" in db.get_table_names().data
    db.custom_cmd("DROP TABLE test_table", clear_schema_cache=False)
    assert "test_table" in db.get_table_names().data
    db.custom_cmd("SELECT", clear_schema_cache=True)
    assert "test_table" not in db.get_table_names().data


@pytest.mark.skipif(not requirements_met, reason=skip_reason)
def test_postgres_caching_column_type(db: PostgreSQLAdapterSQL14):
    """Test caching behavior of `PostgreSQLAdapterSQL14.get_column_type`."""

    db.custom_cmd("DROP TABLE test_table")
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


@pytest.mark.skipif(not requirements_met, reason=skip_reason)
def test_postgres_caching_column_names(db: PostgreSQLAdapterSQL14):
    """Test caching behavior of `PostgreSQLAdapterSQL14.get_column_names`."""

    db.custom_cmd("DROP TABLE test_table")
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


@pytest.mark.skipif(not requirements_met, reason=skip_reason)
def test_postgres_caching_primary_key(db: PostgreSQLAdapterSQL14):
    """Test caching behavior of `PostgreSQLAdapterSQL14.get_primary_key`."""

    db.custom_cmd("DROP TABLE test_table")
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
