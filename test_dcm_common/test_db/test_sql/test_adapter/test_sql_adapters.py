"""
Test module for the classes `PostgreSQLAdapterSQL14` and `SQLiteAdapter3`
of the `db`-subpackage.

The same tests run for both classes to ensure uniformity of the adapters
and their use.

In order to run the tests for the class `PostgreSQLAdapterSQL14`,
a PostgreSQL-database with the following properties is required:
* host: localhost
* port: 5432
* database: postgres
* user: postgres
* password: foo
The test includes deleting, recreating, and modifying the database
"test". It is recommended to use the default-setup as described in
https://zivgitlab.uni-muenster.de/ULB/lzvnrw/team-se/dcm-database
"""

from typing import Optional
from uuid import uuid4
from pathlib import Path

import pytest

from dcm_common.db import (
    psycopg,
    PostgreSQLAdapterSQL14,
    SQLiteAdapter3,
    TransactionResult,
    Transaction,
)


SQLITE = "sqlite"
POSTGRES = "postgres"


def check_requirements_postgres():
    """Check test-requirements for psql."""
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


def check_requirements_sqlite():
    """Check test-requirements for sqlite."""
    return True, ""


@pytest.fixture(name="sql_sample_schema")
def _sql_sample_schema(fixtures):
    return fixtures / "test_data/sample.sql"


@pytest.fixture(name="sql_sample_schema_w_error")
def _sql_sample_schema_w_error(fixtures):
    return fixtures / "test_data/sample_w_error.sql"


def get_sqlite_adapter(**kwargs):
    """Returns SQLite-adapter."""
    return SQLiteAdapter3(
        **({"pool_size": 1, "allow_overflow": False} | kwargs)
    )


def get_postgres_adapter(**kwargs):
    """Returns PostgreSQL-adapter."""
    return PostgreSQLAdapterSQL14(
        **(
            {
                "host": "localhost",
                "port": "5432",
                "database": "postgres",
                "user": "postgres",
                "password": "foo",
                "pool_size": 1,
                "allow_overflow": False,
            }
            | kwargs
        )
    )


def get_db(db_id, request, init_defaults=True, **kwargs):
    """Returns initialized adapter."""
    if db_id == POSTGRES:
        requirements_met, reason = check_requirements_postgres()
    else:
        requirements_met, reason = check_requirements_sqlite()

    if not requirements_met:
        pytest.skip(reason=reason)

    if db_id == POSTGRES:
        db = get_postgres_adapter(**kwargs)
        # cleanup
        db.custom_cmd("DROP DATABASE test")  # delete testing-database
        db.custom_cmd("CREATE DATABASE test").eval()  # re-create testing-database
        db.pool.close()
        db = get_postgres_adapter(**kwargs | {"database": "test"})
    else:
        db = get_sqlite_adapter(**kwargs)

    request.addfinalizer(db.pool.close)

    if init_defaults:
        db.custom_cmd(
            "CREATE TABLE table1 ("
            + "id uuid PRIMARY KEY, "
            + "name text"
            + ")"
        ).eval()
        db.custom_cmd(
            "CREATE TABLE table2 ("
            + "id uuid PRIMARY KEY, "
            + "name text, "
            + "table1_id uuid REFERENCES table1(id)"
            + ")"
        ).eval()
    return db


parametrize_sql_adapter = pytest.mark.parametrize(
    "db_id",
    [SQLITE, POSTGRES],
    ids=["SQLite", "PostgreSQL"]
)


@parametrize_sql_adapter
def test_get_table_names(db_id, request):
    """Test method `get_table_names` of the SQL adapters."""
    db = get_db(db_id, request)

    assert all(t in db.get_table_names().data for t in ["table1", "table2"])


@parametrize_sql_adapter
def test_get_table_names_many(db_id, request):
    """
    Test method `get_table_names` of the SQL adapters for a large number
    of tables.
    """
    db = get_db(db_id, request)

    for i in range(1, 50):
        db.custom_cmd(f"CREATE TABLE table{i} (id text)")

    assert sorted(db.get_table_names().eval()) == sorted(
        [f"table{i}" for i in range(1, 50)]
    )


@parametrize_sql_adapter
def test_get_column_names(db_id, request):
    """
    Test method `get_column_names` of the SQL adapters.
    """
    db = get_db(db_id, request)

    assert sorted(db.get_column_names("table1").data) == sorted(["id", "name"])
    assert sorted(db.get_column_names("table2").data) == sorted(
        ["id", "name", "table1_id"]
    )
    assert not db.get_column_names("unknown_table").success


@parametrize_sql_adapter
def test_get_column_types(db_id, request):
    """
    Test method `get_column_types` of the SQL adapters.
    """
    db = get_db(db_id, request)

    assert db.get_column_types("table1").data == {"id": "uuid", "name": "text"}
    assert not db.get_column_types("unknown_table").success


@parametrize_sql_adapter
def test_get_primary_key(db_id, request):
    """
    Test method `get_primary_key` of the SQL adapters.
    """
    db = get_db(db_id, request)

    assert db.get_primary_key("table1").data == "id"
    assert db.get_primary_key("table2").data == "id"
    assert not db.get_primary_key("unknown_table").success


@parametrize_sql_adapter
def test_insert_delete(db_id, request):
    """
    Test methods `insert`, `get_rows` and `delete`
    of the SQL adapters.
    """
    db = get_db(db_id, request)

    # table1 should be written first
    key1 = db.insert(
        table="table1", row={"name": "name table1"}
    ).data
    key2 = db.insert(
        table="table2", row={"name": "name table2", "table1_id": key1}
    ).data
    assert len(db.get_rows("table1").data) == 1
    assert len(db.get_rows("table2").data) == 1

    # should delete from table2 first
    assert db.delete(table="table2", value=key2, col="id").success
    assert db.delete(table="table1", value=key1, col="id").success

    assert len(db.get_rows("table1").data) == 0
    assert len(db.get_rows("table2").data) == 0


@parametrize_sql_adapter
def test_get_row(db_id, request):
    """Test method `get_row` of the SQL adapters."""
    db = get_db(db_id, request)

    key = db.insert(table="table1", row={"name": "name table1"}).data

    # The value of the primary key exists
    assert sorted(db.get_row(table="table1", value=key).data) == sorted({
        "id": key,
        "name": "name table1",
    })
    assert db.get_row(table="table1", value=key, cols=["name"]).data == {
        "name": "name table1"
    }

    # The value of the primary key does not exist
    result = db.get_row(
        table="table1", value="a2371f34-ee4c-4526-8cd3-ae3ce1445c07"
    )
    assert result.success
    assert result.data is None

    # The table does not exist
    result = db.get_row(
        table="unknown_table", value="a2371f34-ee4c-4526-8cd3-ae3ce1445c07"
    )
    assert not result.success
    assert result.data is None


@parametrize_sql_adapter
def test_get_column(db_id, request):
    """Test method `get_column` of the SQL adapters. """
    db = get_db(db_id, request)

    key1 = db.insert(
        table="table1", row={"name": "name1 table1"}
    ).data
    key2 = db.insert(
        table="table1", row={"name": "name2 table1"}
    ).data
    assert set(db.get_column(table="table1", column="id").data) == {
        key1,
        key2,
    }


@parametrize_sql_adapter
def test_insert_get_row_nested(db_id, request):
    """
    Test methods `insert` and `get_row` of the SQL adapters.
    """
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 ("
        + "id uuid PRIMARY KEY, "
        + "name text, "
        + "nested jsonb, "
        + "nested_empty jsonb"
        + ")"
    ).success

    assert sorted(db.get_column_names("table3").data) == sorted(
        ["id", "name", "nested", "nested_empty"]
    )
    assert db.get_column_types("table3").eval() == {
        "id": "uuid",
        "name": "text",
        "nested": "jsonb",
        "nested_empty": "jsonb"
    }

    row = {
        "name": "name1",
        "nested": {
            "nested_k": "nested_v",
            "nested_bool": True,
            "nested_none": None,
        },
        "nested_empty": {},
    }
    key = db.insert(table="table3", row=row).data
    assert key is not None
    assert sorted(db.get_row(table="table3", value=key).data) == sorted(
        row | {"id": key}
    )

    # insert a second row
    key2 = db.insert(table="table3", row=row).data

    assert db.get_rows("table3").data == [
        row | {"id": key},
        row | {"id": key2}
    ]


@parametrize_sql_adapter
def test_insert_existing(db_id, request):
    """
    Test methods `insert` and `get_row`
    of the SQL adapters for existing id.
    """
    db = get_db(db_id, request)

    # insert record
    row = {"name": "name initial"}
    key = db.insert(table="table1", row=row).data
    row["id"] = key
    assert sorted(db.get_row(table="table1", value=key).data) == sorted(row)

    # update record
    row["name"] = "name new"
    db.update(table="table1", row=row)
    assert sorted(db.get_row(table="table1", value=key).data) == sorted(row)


@parametrize_sql_adapter
def test_sql_injection_attack_insert_text(db_id, request):
    """Test the SQL adapters for possible SQL injection attacks."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 (id uuid PRIMARY KEY, col text)"
    ).success

    value = "value'); DROP TABLE table1 CASCADE; --"
    result = db.insert("table3", {"col": value})
    assert "table1" in db.get_table_names(True).data
    assert result.success
    assert db.get_row("table3", result.data, ["col"]).data["col"] == value


@parametrize_sql_adapter
def test_sql_injection_attack_insert_uuid(db_id, request):
    """Test the SQL adapters for possible SQL injection attacks."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 (id uuid PRIMARY KEY, col uuid)"
    ).success

    value = "2af0a035-dc28-405f-b057-9866ec76a78f'); DROP TABLE table1 CASCADE; --"
    result = db.insert("table3", {"col": value})
    assert "table1" in db.get_table_names(True).data
    assert not result.success
    print(result.msg)


@parametrize_sql_adapter
def test_sql_injection_attack_insert_integer(db_id, request):
    """Test the SQL adapters for possible SQL injection attacks."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 (id uuid PRIMARY KEY, col integer)"
    ).success

    value = "0'); DROP TABLE table1 CASCADE; --"
    result = db.insert("table3", {"col": value})
    assert "table1" in db.get_table_names(True).data
    assert not result.success
    print(result.msg)


@parametrize_sql_adapter
def test_sql_injection_attack_insert_boolean(db_id, request):
    """Test the SQL adapters for possible SQL injection attacks."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 (id uuid PRIMARY KEY, col boolean)"
    ).success

    value = "FALSE'); DROP TABLE table1 CASCADE; --"
    result = db.insert("table3", {"col": value})
    assert "table1" in db.get_table_names(True).data
    assert not result.success
    print(result.msg)


@parametrize_sql_adapter
def test_sql_injection_attack_insert_jsonb(db_id, request):
    """Test the SQL adapters for possible SQL injection attacks."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 (id uuid PRIMARY KEY, col jsonb)"
    ).success

    # test biggest attack-vector: a string value
    value = "data'); DROP TABLE table1 CASCADE; --"
    result = db.insert("table3", {"col": value})
    assert "table1" in db.get_table_names(True).data
    assert result.success
    assert db.get_row("table3", result.data, ["col"]).data["col"] == value


@parametrize_sql_adapter
def test_sql_injection_attack_update_text(db_id, request):
    """Test the SQL adapters for possible SQL injection attacks."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 (id text PRIMARY KEY, col text)"
    ).success
    assert db.custom_cmd(
        "INSERT INTO table3 (id, col) VALUES ('a', 'value')"
    ).success

    value = "value'); DROP TABLE table1 CASCADE; --"
    result = db.update("table3", {"id": "a", "col": value})
    assert "table1" in db.get_table_names(True).data
    assert result.success
    assert db.get_row("table3", "a", ["col"]).data["col"] == value


@parametrize_sql_adapter
def test_sql_injection_attack_update_uuid(db_id, request):
    """Test the SQL adapters for possible SQL injection attacks."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 (id text PRIMARY KEY, col uuid)"
    ).success
    assert db.custom_cmd(
        "INSERT INTO table3 (id, col) VALUES ('a', '2af0a035-dc28-405f-b057-9866ec76a78f')"
    ).success

    value = "2af0a035-dc28-405f-b057-9866ec76a78f'); DROP TABLE table1 CASCADE; --"
    result = db.update("table3", {"id": "a", "col": value})
    assert "table1" in db.get_table_names(True).data
    assert not result.success
    print(result.msg)


@parametrize_sql_adapter
def test_sql_injection_attack_update_integer(db_id, request):
    """Test the SQL adapters for possible SQL injection attacks."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 (id text PRIMARY KEY, col integer)"
    ).success
    assert db.custom_cmd(
        "INSERT INTO table3 (id, col) VALUES ('a', 2)"
    ).success

    value = "2'); DROP TABLE table1 CASCADE; --"
    result = db.update("table3", {"id": "a", "col": value})
    assert "table1" in db.get_table_names(True).data
    assert not result.success
    print(result.msg)


@parametrize_sql_adapter
def test_sql_injection_attack_update_boolean(db_id, request):
    """Test the SQL adapters for possible SQL injection attacks."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 (id text PRIMARY KEY, col boolean)"
    ).success
    assert db.custom_cmd(
        "INSERT INTO table3 (id, col) VALUES ('a', 'TRUE')"
    ).success

    value = "FALSE'); DROP TABLE table1 CASCADE; --"
    result = db.update("table3", {"id": "a", "col": value})
    assert "table1" in db.get_table_names(True).data
    assert not result.success
    print(result.msg)


@parametrize_sql_adapter
def test_sql_injection_attack_update_jsonb(db_id, request):
    """Test the SQL adapters for possible SQL injection attacks."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 (id text PRIMARY KEY, col jsonb)"
    ).success
    assert db.custom_cmd(
        "INSERT INTO table3 (id, col) VALUES ('a', '\"data\"')"
    ).success

    value = "data'); DROP TABLE table1 CASCADE; --"
    result = db.update("table3", {"id": "a", "col": value})
    assert "table1" in db.get_table_names(True).data
    assert result.success
    assert db.get_row("table3", "a", ["col"]).data["col"] == value


@parametrize_sql_adapter
def test_sql_injection_attack_delete_text(db_id, request):
    """Test the SQL adapters for possible SQL injection attacks."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 (id text PRIMARY KEY, col text)"
    ).success
    assert db.custom_cmd(
        "INSERT INTO table3 (id, col) VALUES ('a', 'value')"
    ).success

    result = db.delete("table3", "a'; DROP TABLE table1 CASCADE; --")
    assert "table1" in db.get_table_names(True).data
    assert result.success
    assert db.get_row("table3", "a").data is not None


@parametrize_sql_adapter
def test_sql_injection_attack_delete_uuid(db_id, request):
    """Test the SQL adapters for possible SQL injection attacks."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 (id text PRIMARY KEY, col uuid)"
    ).success
    assert db.custom_cmd(
        "INSERT INTO table3 (id, col) VALUES ('a', '2af0a035-dc28-405f-b057-9866ec76a78f')"
    ).success

    result = db.delete("table3", "a'; DROP TABLE table1 CASCADE; --")
    assert "table1" in db.get_table_names(True).data
    assert result.success
    assert db.get_row("table3", "a").data is not None


@parametrize_sql_adapter
def test_sql_injection_attack_delete_integer(db_id, request):
    """Test the SQL adapters for possible SQL injection attacks."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 (id text PRIMARY KEY, col integer)"
    ).success
    assert db.custom_cmd(
        "INSERT INTO table3 (id, col) VALUES ('a', 2)"
    ).success

    result = db.delete("table3", "a'; DROP TABLE table1 CASCADE; --")
    assert "table1" in db.get_table_names(True).data
    assert result.success
    assert db.get_row("table3", "a").data is not None


@parametrize_sql_adapter
def test_sql_injection_attack_delete_boolean(db_id, request):
    """Test the SQL adapters for possible SQL injection attacks."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 (id text PRIMARY KEY, col boolean)"
    ).success
    assert db.custom_cmd(
        "INSERT INTO table3 (id, col) VALUES ('a', 'TRUE')"
    ).success

    result = db.delete("table3", "a'; DROP TABLE table1 CASCADE; --")
    assert "table1" in db.get_table_names(True).data
    assert result.success
    assert db.get_row("table3", "a").data is not None


@parametrize_sql_adapter
def test_sql_injection_attack_delete_jsonb(db_id, request):
    """Test the SQL adapters for possible SQL injection attacks."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 (id text PRIMARY KEY, col jsonb)"
    ).success
    assert db.custom_cmd(
        "INSERT INTO table3 (id, col) VALUES ('a', '\"data\"')"
    ).success

    result = db.delete("table3", "a'; DROP TABLE table1 CASCADE; --")
    assert "table1" in db.get_table_names(True).data
    assert result.success
    assert db.get_row("table3", "a").data is not None


@parametrize_sql_adapter
def test_sql_injection_attack_select_text(db_id, request):
    """Test the SQL adapters for possible SQL injection attacks."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 (id text PRIMARY KEY, col text)"
    ).success
    assert db.custom_cmd(
        "INSERT INTO table3 (id, col) VALUES ('a', 'value')"
    ).success

    result = db.get_row(
        "table3",
        "a'; DROP TABLE table1 CASCADE; --",
    )
    assert "table1" in db.get_table_names(True).data
    assert result.success
    assert result.data is None


@parametrize_sql_adapter
def test_sql_injection_attack_select_uuid(db_id, request):
    """Test the SQL adapters for possible SQL injection attacks."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 (id text PRIMARY KEY, col uuid)"
    ).success
    assert db.custom_cmd(
        "INSERT INTO table3 (id, col) VALUES ('a', '2af0a035-dc28-405f-b057-9866ec76a78f')"
    ).success

    result = db.get_row(
        "table3",
        "a'; DROP TABLE table1 CASCADE; --",
    )
    assert "table1" in db.get_table_names(True).data
    assert result.success
    assert result.data is None


@parametrize_sql_adapter
def test_sql_injection_attack_select_integer(db_id, request):
    """Test the SQL adapters for possible SQL injection attacks."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 (id text PRIMARY KEY, col integer)"
    ).success
    assert db.custom_cmd(
        "INSERT INTO table3 (id, col) VALUES ('a', 2)"
    ).success

    result = db.get_row(
        "table3",
        "a'; DROP TABLE table1 CASCADE; --",
    )
    assert "table1" in db.get_table_names(True).data
    assert result.success
    assert result.data is None


@parametrize_sql_adapter
def test_sql_injection_attack_select_boolean(db_id, request):
    """Test the SQL adapters for possible SQL injection attacks."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 (id text PRIMARY KEY, col boolean)"
    ).success
    assert db.custom_cmd(
        "INSERT INTO table3 (id, col) VALUES ('a', 'TRUE')"
    ).success

    result = db.get_row(
        "table3",
        "a'; DROP TABLE table1 CASCADE; --",
    )
    assert "table1" in db.get_table_names(True).data
    assert result.success
    assert result.data is None


@parametrize_sql_adapter
def test_sql_injection_attack_select_jsonb(db_id, request):
    """Test the SQL adapters for possible SQL injection attacks."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 (id text PRIMARY KEY, col jsonb)"
    ).success
    assert db.custom_cmd(
        "INSERT INTO table3 (id, col) VALUES ('a', '\"data\"')"
    ).success

    result = db.get_row(
        "table3",
        "a'; DROP TABLE table1 CASCADE; --",
    )
    assert "table1" in db.get_table_names(True).data
    assert result.success
    assert result.data is None


@parametrize_sql_adapter
def test_sql_injection_attack_table_name(db_id, request):
    """Test the SQL adapters for possible SQL injection attacks."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 (id text PRIMARY KEY, col text)"
    ).success

    with pytest.raises(ValueError) as exc_info:
        db.get_insert_statement(
            "table1; DROP TABLE table1 CASCADE; --", {"id": "a"}
        )
    print(exc_info.value)
    assert "table1" in db.get_table_names(True).data

    with pytest.raises(ValueError) as exc_info:
        db.get_update_statement(
            "table1; DROP TABLE table1 CASCADE; --", {"id": "a"}
        )
    print(exc_info.value)
    assert "table1" in db.get_table_names(True).data

    with pytest.raises(ValueError) as exc_info:
        db.get_delete_statement("table1; DROP TABLE table1 CASCADE; --", "a")
    print(exc_info.value)
    assert "table1" in db.get_table_names(True).data

    with pytest.raises(ValueError) as exc_info:
        db.get_select_statement("table1; DROP TABLE table1 CASCADE; --", "a")
    print(exc_info.value)
    assert "table1" in db.get_table_names(True).data


@parametrize_sql_adapter
def test_sql_injection_attack_col_names(db_id, request):
    """Test the SQL adapters for possible SQL injection attacks."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 (id text PRIMARY KEY, col text)"
    ).success

    with pytest.raises(ValueError) as exc_info:
        db.get_insert_statement(
            "table1", {"id": "a", "col; DROP TABLE table1 CASCADE; --": "b"}
        )
    print(exc_info.value)
    assert "table1" in db.get_table_names(True).data

    with pytest.raises(ValueError) as exc_info:
        db.get_update_statement(
            "table1", {"id": "a", "col; DROP TABLE table1 CASCADE; --": "b"}
        )
    print(exc_info.value)
    assert "table1" in db.get_table_names(True).data

    with pytest.raises(ValueError) as exc_info:
        db.get_delete_statement("table1", "a", "col; DROP TABLE table1 CASCADE; --")
    print(exc_info.value)
    assert "table1" in db.get_table_names(True).data

    with pytest.raises(ValueError) as exc_info:
        db.get_select_statement("table1", "a", "col; DROP TABLE table1 CASCADE; --")
    print(exc_info.value)
    assert "table1" in db.get_table_names(True).data


@parametrize_sql_adapter
def test_sql_injection_attack_query(db_id, request):
    """
    Test the SQL adapters for possible SQL injection attacks
    with the `custom_cmd` method.

    If a custom query is performed based on the field `name`
    that is provided by the user, it's enough for an attacker
    to just know the name of a table to delete it.
    """
    db = get_db(db_id, request)

    query_string = "SELECT * FROM table2 WHERE name = '{}'"

    # insert a clean record
    row1 = {"name": "name table1"}
    db.insert(table="table2", row=row1)

    # the intended use does not delete table2
    db.custom_cmd(query_string.format(row1["name"]))
    assert "table2" in db.get_table_names().data

    # insert a record that attempts an SQL injection attack
    # via the 'name' field
    row2 = {
        "name": "asd'; DROP TABLE table2; --"
    }

    # prepared statement: an attack does not delete table2
    db.insert(table="table2", row=row2).eval()
    assert "table2" in db.get_table_names().data

    # an attack is generally successful for custom command
    # in sqlite, however, the attempt crashes instead of resolving
    with pytest.raises(ValueError) as exc_info:
        db.custom_cmd(query_string.format(row2["name"])).eval()
    # should cover both adapters as well as different python versions
    assert any(
        s in str(exc_info.value)
        for s in [
            "ProgrammingError",
            "You can only execute one statement at a time",
        ]
    )
    if db_id == POSTGRES:
        assert "table2" not in db.get_table_names().data
    else:
        assert "table2" in db.get_table_names().data


@pytest.mark.parametrize(
    "groups",
    [
        [],  # no-group
        [{"group_id": "admin"}],  # admin
        [{"group_id": "curator", "workspace_id": ""}],  # curator
        [
            {"group_id": "admin"},
            {"group_id": "curator", "workspace_id": ""}
        ],  # admin-curator
        [
            {"group_id": "admin"},
            {"group_id": "admin2"},
            {"group_id": "curator", "workspace_id": ""},
        ],  # admin-admin2-curator
    ],
    ids=[
        "no-group",
        "admin",
        "curator",
        "admin-curator",
        "admin-admin2-curator",
    ],
)
@parametrize_sql_adapter
def test_many_to_many_relationships(db_id, request, groups):
    """Test the SQL adapters with a many-to-many relationship."""
    db = get_db(db_id, request)

    # Setup tables
    db.custom_cmd(
        "CREATE TABLE test_users ("
        + "id uuid PRIMARY KEY, "
        + "name text)"
    )
    db.custom_cmd(
        "CREATE TABLE test_workspaces ("
        + "id uuid PRIMARY KEY, "
        + "name text)"
    )
    db.custom_cmd(
        "CREATE TABLE test_user_groups ("
        + "id uuid PRIMARY KEY, "
        + "user_id uuid REFERENCES test_users(id) NOT NULL, "
        + "workspace_id uuid REFERENCES test_workspaces(id) NULL, "
        + "group_id text NOT NULL, "
        + "UNIQUE (user_id, workspace_id, group_id)"
        + ")"
    )

    db.custom_cmd(
        "CREATE UNIQUE INDEX test_user_groups_no_workspace "
        + "ON test_user_groups (user_id, group_id) "
        + "WHERE workspace_id is NULL"
    )

    # insert users
    keys_users = []
    for i in range(0, 2):
        keys_users.append(
            db.insert(
                table="test_users", row={"name": "user" + str(i)}
            ).data
        )
    # insert workspaces
    keys_workspaces = []
    for i in range(0, 2):
        keys_workspaces.append(
            db.insert(
                table="test_workspaces", row={"name": "course" + str(i)}
            ).data
        )

    assert all(k is not None for k in keys_users)
    assert all(k is not None for k in keys_workspaces)

    # Set relationships
    relationships = []
    for group in groups:
        relationship = (
                {"user_id": keys_users[0]}
                | group
                | (
                    {"workspace_id": keys_workspaces[0]}
                    if group["group_id"] == "curator"
                    else {}
                )
            )
        relationships.append(relationship)
        assert db.insert(
            table="test_user_groups",
            row=relationship,
        ).success

    if groups:
        # check unique constraint in user_groups table
        # attempt to rewrite an existing
        # tuple of (user_id, workspace_id, group_id)
        violate_uniqueness = db.insert(
            table="test_user_groups",
            row=relationships[0]
        )
        assert not violate_uniqueness.success

        # delete the user/workspaces should not be possible
        assert not db.delete(
            table="test_users", value=keys_users[0], col="id"
        ).success

        if any(group["group_id"] == "curator" for group in groups):
            assert not db.delete(
                table="test_workspaces", value=keys_workspaces[0], col="id"
            ).success

    # a record without 'group_id' is not allowed
    assert not db.insert(
        table="test_user_groups",
        row={
            "user_id": keys_users[0],
            "workspace_id": keys_workspaces[1],
        },
    ).success

    def get_groups_for_user_id(user_id: str) -> TransactionResult:
        """
        Returns a list of the `GroupMembership` objects associated
        with the user_id.
        """

        from dataclasses import dataclass
        from dcm_common.models import DataModel

        @dataclass
        class GroupMembership(DataModel):
            group_id: str
            workspace_id: Optional[str] = None

        result = db.get_rows(
            "test_user_groups",
            user_id,
            "user_id",
            ["group_id", "workspace_id"]
        )
        if not result.success:
            return result
        return TransactionResult(
            True,
            data=[GroupMembership(**x) for x in result.data]
        )

    response_groups = get_groups_for_user_id(keys_users[0])
    assert len(response_groups.data) == len(groups)
    assert [group.json for group in response_groups.data] == [
        group
        | (
            {"workspace_id": keys_workspaces[0]}
            if group["group_id"] == "curator"
            else {}
        )
        for group in groups
    ]


@parametrize_sql_adapter
def test_insert_with_pk(db_id, request):
    """
    Test method `insert` of the SQL adapters
    for inserting a record with primary key.
    """
    db = get_db(db_id, request)

    key = str(uuid4())

    insert_response = db.insert(
        table="table2", row={"id": key, "name": "name table2"}
    )
    assert insert_response.success
    assert insert_response.data == key

    assert len(db.get_rows("table2").data) == 1

    db.delete(table="table2", value=key, col="id").eval()


@pytest.mark.parametrize(
    ("type_", "input_", "output"),
    pytest_args := [
        ["text NULL", "NULL", "NULL"],
        ["text NULL", None, None],
        ["text NULL", "", ""],
        ["text NULL", "  ", "  "],
        ["text NULL", " a ", " a "],
        ["text NULL", "a\na", "a\na"],
        ["text NULL", "a\r\na", "a\r\na"],
        ["text NULL", "\t", "\t"],
        ["text NULL", "\n", "\n"],
        ["text NULL", """
""", """
"""],
        [
            "jsonb NULL",
            {"a": "|", "b": " | ", "c": "\n"},
            {"a": "|", "b": " | ", "c": "\n"},
        ],
        ["text NULL", "\"b\\'", "\"b\\'"],
        ["integer NULL", 2, 2],
        ["integer NULL", None, None],
        ["boolean NULL", True, True],
        ["boolean NULL", False, False],
        ["boolean NULL", None, None],
        [
            "uuid NULL",
            "ebe34866-53e3-47ed-bf60-b27272797427",
            "ebe34866-53e3-47ed-bf60-b27272797427",
        ],
        ["uuid NULL", None, None],
        [
            "jsonb NULL",
            {"a": 1, "b": '"b\\\'', "c": True, "d": None, "e": {}, "f": []},
            {"a": 1, "b": '"b\\\'', "c": True, "d": None, "e": {}, "f": []},
        ],
        ["jsonb NULL", {}, {}],
        [
            "jsonb NULL",
            {"a": ""},
            {"a": ""},
        ],
        ["jsonb NULL", "", ""],
        ["jsonb NULL", "some text", "some text"],
        ["jsonb NULL", None, None],
    ],
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
@parametrize_sql_adapter
def test_encode_decode(db_id, request, input_, type_, output):
    """Test encoding/decoding for supported types."""
    db = get_db(db_id, request, False)

    assert db.custom_cmd(
        "CREATE TABLE table1 ("
        + "id uuid PRIMARY KEY, "
        + f"field {type_}"
        + ")"
    ).success

    id_ = str(uuid4())
    assert db.insert("table1", {"field": input_, "id": id_}).success
    assert db.get_row("table1", id_).data["field"] == output


@parametrize_sql_adapter
def test_uuid_validation(db_id, request):
    """Test the validation for an invalid uuid in a field with uuid type."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 ("
        + "id uuid PRIMARY KEY, "
        + "field uuid"
        + ")"
    ).success

    input_ = "no uuid"
    result = db.insert("table3", {"field": input_})
    assert not result.success
    assert "Invalid UUID." in result.msg
    print(result.msg)


@pytest.mark.parametrize(
    ("type_", "input_", "output"),
    pytest_args := [
        ["text NULL", "line1\nline2", "line1\nline2"],
        ["jsonb NULL", "line1\nline2", "line1\nline2"],
        ["jsonb NULL", {"a": "line1\nline2"}, {"a": "line1\nline2"}],
    ],
    ids=["newline-text", "newline-jsonb_plain", "newline-jsonb_nested"],
)
@parametrize_sql_adapter
def test_encode_decode_special_characters(db_id, request, input_, type_, output):
    """Test encoding/decoding for special characters."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 ("
        + "id uuid PRIMARY KEY, "
        + f"field {type_}"
        + ")"
    ).success

    id_ = db.insert("table3", {"field": input_}).data
    assert db.get_row("table3", id_).data["field"] == output

    assert db.update("table3", {"field": input_} | {"id": id_}).success
    assert db.get_row("table3", id_).data["field"] == output


@parametrize_sql_adapter
def test_insert_failure(db_id, request):
    """Test the `msg` of response when `insert` fails."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 ("
        + "id uuid PRIMARY KEY, "
        + "name text UNIQUE"
        + ")"
    ).success

    assert db.insert("table3", {"name": "some_name"}).success

    # test rejection of duplicate in UNIQUE column
    with pytest.raises(ValueError) as exc_info:
        db.insert("table3", {"name": "some_name"}).eval()
    if isinstance(db, SQLiteAdapter3):
        expected_msg = "UNIQUE constraint failed: table3.name"
    else:
        expected_msg = (
            'duplicate key value violates unique constraint "table3_name_key"'
        )
    assert expected_msg in str(exc_info)


@parametrize_sql_adapter
def test_insert_update_unknown_column(db_id, request):
    """
    Test `insert` and `update` when the input row contains an unknown column.
    """
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 ("
        + "id uuid PRIMARY KEY, "
        + "name text UNIQUE"
        + ")"
    ).success

    # successful insert
    id_ = db.insert("table3", {"name": "some_name"}).data
    assert id_ is not None
    # insert with unknown column fails
    with pytest.raises(ValueError) as exc_info:
        db.insert("table3", {"name": "some_name", "unknown": "text"}).eval()
    assert "Unknown column 'unknown'" in str(exc_info)
    # multiple unknown yields only single in error message
    with pytest.raises(ValueError) as exc_info:
        db.insert(
            "table3",
            {
                "name": "some_name",
                "unknown-1": "text-1",
                "unknown-2": "text-2",
            },
        ).eval()
    assert ("Unknown column 'unknown-1'" in str(exc_info)) ^ (
        "Unknown column 'unknown-2'" in str(exc_info)
    )

    # successful update
    assert db.update(
        "table3", {"id": id_, "name": "some_new_name"}
    ).success
    # update with unknown column fails
    with pytest.raises(ValueError) as exc_info:
        db.update(
            "table3", {"id": id_, "name": "some_name", "unknown": "text"}
        ).eval()
    assert "Unknown column 'unknown'" in str(exc_info)


@pytest.mark.parametrize(
    ("type_", "is_not_none"),
    pytest_args := [
        ["uuid", True],
        ["text", True],
        ["jsonb", False],
        ["integer", False],
        ["boolean", False],  # this does not make much sense as a pk
    ],
    ids=["uuid", "text", "jsonb", "integer", "boolean"],
)
@parametrize_sql_adapter
def test_generate_pk_types(db_id, request, type_, is_not_none):
    """Test automatic generation of primary key for common data types."""
    db = get_db(db_id, request)

    assert db.custom_cmd(
        "CREATE TABLE table3 ("
        + f"id {type_} PRIMARY KEY)"
    ).success

    id_ = db.insert("table3", {}).data
    assert (id_ is not None) == is_not_none


@parametrize_sql_adapter
def test_read_file_success(db_id, request, sql_sample_schema):
    """
    Test the `read_file` method.
    """
    db = get_db(db_id, request)

    assert db.read_file(sql_sample_schema).success
    assert "sample_table" in db.get_table_names().data
    assert "sample_table2" in db.get_table_names().data


@parametrize_sql_adapter
def test_read_file_not_exist(db_id, request):
    """
    Test the `read_file` method when the file does not exist.
    """
    db = get_db(db_id, request)

    with pytest.raises(ValueError) as exc_info:
        db.read_file(Path("unknown_file")).eval(
            context="reading file:"
        )
    assert "unknown_file" in str(exc_info)


@parametrize_sql_adapter
def test_read_file_syntax_error(db_id, request, sql_sample_schema_w_error):
    """
    Test the `read_file` method when the file contains a syntax error.
    """
    db = get_db(db_id, request)

    with pytest.raises(ValueError):
        db.read_file(sql_sample_schema_w_error).eval()
    assert "sample_table" not in db.get_table_names().data


@parametrize_sql_adapter
def test_delete_two_rows(db_id, request):
    """Test method `delete` of the SQL adapters for multiple matches."""
    db = get_db(db_id, request)

    common_value = "some common value"
    key1 = db.insert(table="table1", row={"name": common_value}).data
    key2 = db.insert(table="table1", row={"name": common_value}).data

    assert all(k is not None for k in [key1, key2])
    assert sorted(
        [d["id"] for d in db.get_rows(table="table1", cols=["id"]).data]
    ) == sorted([key1, key2])

    db.delete(table="table1", value=common_value, col="name")
    assert db.get_rows(table="table1", cols=["id"]).data == []


@parametrize_sql_adapter
def test_transaction_simple(db_id, request):
    """Test `Transaction` without context-manager."""
    db = get_db(db_id, request)

    key1 = str(uuid4())
    key2 = str(uuid4())
    value1 = "value1"
    value2 = "value2"
    transaction = Transaction(db)
    transaction.add(
        f"INSERT INTO table1 (id, name) VALUES ('{key1}', '{value1}')"
    )
    transaction.add(
        f"INSERT INTO table1 (id, name) VALUES ('{key2}', '{value2}')"
    )

    assert transaction.result is None
    transaction.commit()
    assert transaction.result is not None
    assert transaction.result.success
    assert db.get_column(table="table1", column="name").data == [
        value1,
        value2,
    ]


@parametrize_sql_adapter
@pytest.mark.parametrize(
    ("constructor_pp", "commit_pp", "check"),
    [
        (None, None, lambda result: isinstance(result.data[0][0], str)),
        (lambda x: "test", None, lambda result: result.data == "test"),
        (None, lambda x: "test", lambda result: result.data == "test"),
        (
            lambda x: "test",
            lambda x: "test2",
            lambda result: result.data == "test2",
        ),
    ],
    ids=["no-pp", "constructor-pp", "commit-pp", "commit-pp-override"]
)
def test_transaction_post_process(
    db_id, request, constructor_pp, commit_pp, check
):
    """
    Test `Transaction` without context-manager but with `post_process`.
    """
    db = get_db(db_id, request)

    transaction = Transaction(db, post_process=constructor_pp)
    transaction.add_insert("table1", {"id": str(uuid4()), "name": "name"})
    transaction.commit(post_process=commit_pp)

    assert check(transaction.result)


@parametrize_sql_adapter
def test_new_transaction_add(db_id, request):
    """
    Test `Transaction`-context manager via `SQLAdapter.new_transaction`.
    """
    db = get_db(db_id, request)

    key1 = str(uuid4())
    key2 = str(uuid4())
    value1 = "value1"
    value2 = "value2"
    with db.new_transaction() as transaction:
        transaction.add(
            f"INSERT INTO table1 (id, name) VALUES ('{key1}', '{value1}')"
        )
        transaction.add(
            f"INSERT INTO table1 (id, name) VALUES ('{key2}', '{value2}')"
        )

    assert transaction.result.success
    assert db.get_column(table="table1", column="name").data == [
        value1,
        value2,
    ]


@parametrize_sql_adapter
def test_new_transaction_add_rollback_on_fail(db_id, request):
    """
    Test `Transaction`-context manager via `SQLAdapter.new_transaction`.
    Validate rollback for failed command.
    """
    db = get_db(db_id, request)

    key1 = str(uuid4())
    key2 = str(uuid4())
    value1 = "value1"
    value2 = "value2"
    db.insert("table1", {"id": key1, "name": value1})
    with db.new_transaction() as transaction:
        transaction.add(
            f"INSERT INTO table1 (id, name) VALUES ('{key2}', '{value2}')"
        )
        transaction.add("SELECT * FROM table3")

    assert not transaction.result.success
    assert transaction.result.raw.error is not None
    assert transaction.result.msg != ""
    assert db.get_column(table="table1", column="name").data == [value1]


@parametrize_sql_adapter
def test_new_transaction_post_process(db_id, request):
    """
    Test `Transaction`-context manager via `SQLAdapter.new_transaction`.
    """
    db = get_db(db_id, request)

    with db.new_transaction(post_process=lambda x: "test") as transaction:
        transaction.add("SELECT * FROM table1")

    assert transaction.result.data == "test"


@parametrize_sql_adapter
def test_new_transaction_add_insert(db_id, request):
    """
    Test `Transaction`-context manager via `SQLAdapter.new_transaction`
    with method `add_insert`.
    """
    db = get_db(db_id, request)

    key1 = str(uuid4())
    key2 = str(uuid4())
    value1 = "value1"
    value2 = "value2"
    with db.new_transaction() as transaction:
        transaction.add_insert("table1", {"id": key1, "name": value1})
        transaction.add_insert("table1", {"id": key2, "name": value2})

    assert transaction.result.success
    assert sorted(db.get_column(table="table1", column="name").data) == sorted(
        [
            value1,
            value2,
        ]
    )
    assert (key1,) in transaction.result.data
    assert (key2,) in transaction.result.data


@parametrize_sql_adapter
def test_new_transaction_add_update(db_id, request):
    """
    Test `Transaction`-context manager via `SQLAdapter.new_transaction`
    with method `add_update`.
    """
    db = get_db(db_id, request)

    key1 = str(uuid4())
    value1 = "value1"
    value2 = "value2"

    db.insert("table1", {"id": key1, "name": value1})
    with db.new_transaction() as transaction:
        transaction.add_update("table1", {"id": key1, "name": value2})

    assert transaction.result.success
    assert db.get_row(table="table1", value=key1).data["name"] == value2


@parametrize_sql_adapter
def test_new_transaction_add_delete(db_id, request):
    """
    Test `Transaction`-context manager via `SQLAdapter.new_transaction`
    with method `add_delete`.
    """
    db = get_db(db_id, request)

    key1 = str(uuid4())
    value1 = "value1"

    db.insert("table1", {"id": key1, "name": value1})
    with db.new_transaction() as transaction:
        transaction.add_delete("table1", key1)

    assert transaction.result.success
    assert db.get_row(table="table1", value=key1).data is None


@parametrize_sql_adapter
def test_new_transaction_add_select(db_id, request):
    """
    Test `Transaction`-context manager via `SQLAdapter.new_transaction`
    with method `add_select`.
    """
    db = get_db(db_id, request)

    key1 = str(uuid4())
    value1 = "value1"

    db.insert("table1", {"id": key1, "name": value1})
    with db.new_transaction() as transaction:
        transaction.add_select("table1", key1)

    assert transaction.result.success
    assert (key1, value1) in transaction.result.data
