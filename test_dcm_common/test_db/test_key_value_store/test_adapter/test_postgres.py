"""
Test module for the class `PostgreSQLAdapter14` of the `db`-subpackage.

In order to run the tests in this module, a PostgreSQL-database with the
following properties is required:
* host: localhost
* port: 5432
* user: postgres
* password: foo
* database: postgres
It is recommended to use the default-setup as described in
https://zivgitlab.uni-muenster.de/ULB/lzvnrw/team-se/dcm-database.

The test-runner will attempt to create a new table 'tests' upon
starting. (If this table already exists, it is expected to have the
model `(key UUID, value JSONB)`.) This table will be deleted again
on teardown.
"""

from uuid import uuid4
import os
import subprocess

import pytest

from dcm_common.db import KeyValueStoreAdapter, PostgreSQLAdapter14


def check_requirements():
    """Check test-requirements."""
    _PSQL = os.environ.get("POSTGRES_EXECUTABLE", "psql")

    for cmd in (
        [_PSQL, "-w", "-V"],
        [
            _PSQL,
            "-w",
            "-U",
            "postgres",
            "-h",
            "localhost",
            "-p",
            "5432",
            "-c",
            "SHOW server_version;",
        ],
    ):
        try:
            result = subprocess.run(
                cmd,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=os.environ | {"PGPASSWORD": "foo"}
            )
        except FileNotFoundError:
            return False, f"Missing PostgreSQL-client '{_PSQL}'."
        if result.returncode != 0:
            return False, result.stderr
    return True, ""


requirements_met, reason = check_requirements()


@pytest.fixture(name="db", scope="module")
def _db():
    return PostgreSQLAdapter14(
        "key", "value", "tests",
        host="localhost",
        port="5432",
        user="postgres",
        password="foo",
    )


@pytest.fixture(name="init_db", autouse=True, scope="module")
def _init_db(db: PostgreSQLAdapter14, request):
    db.custom_cmd(
        "CREATE TABLE IF NOT EXISTS tests "
        + "(key UUID PRIMARY KEY DEFAULT gen_random_uuid(), value JSONB);"
    )
    request.addfinalizer(
        lambda: db.custom_cmd(
            "DROP TABLE tests;"
        )
    )


@pytest.fixture(name="clear_db", autouse=True, scope="function")
def _clear_db(db: PostgreSQLAdapter14):
    db.clear()


@pytest.fixture(name="uuid")
def _uuid():
    return str(uuid4())


@pytest.mark.skipif(not requirements_met, reason=reason)
def test_interface_compliance():
    """
    Test whether `PostgreSQLAdapter14` implements a
    `KeyValueStoreAdapter`.
    """
    assert issubclass(PostgreSQLAdapter14, KeyValueStoreAdapter)
    PostgreSQLAdapter14("config_id", "config", "configurations")


@pytest.mark.skipif(not requirements_met, reason=reason)
def test_write(db: PostgreSQLAdapter14, uuid):
    """Test method `write` of class `PostgreSQLAdapter14`."""
    db.write(uuid, "value")


@pytest.mark.skipif(not requirements_met, reason=reason)
def test_write_existing(db: PostgreSQLAdapter14, uuid):
    """
    Test method `write` of class `PostgreSQLAdapter14` for existing
    record.
    """
    db.write(uuid, "value1")
    db.write(uuid, "value2")
    assert db.read(uuid) == "value2"


@pytest.mark.skipif(not requirements_met, reason=reason)
def test_read_non_existing(db: PostgreSQLAdapter14, uuid):
    """
    Test method `read` of class `PostgreSQLAdapter14` without existing
    key.
    """
    assert db.read(uuid) is None


@pytest.mark.skipif(not requirements_met, reason=reason)
def test_read(db: PostgreSQLAdapter14, uuid):
    """Test method `read` of class `PostgreSQLAdapter14`."""
    db.write(uuid, "value")
    assert db.read(uuid) == "value"


@pytest.mark.skipif(not requirements_met, reason=reason)
def test_read_pop(db: PostgreSQLAdapter14, uuid):
    """
    Test method `read` of class `PostgreSQLAdapter14` with
    `pop=True`.
    """
    db.write(uuid, "value")
    assert db.read(uuid, pop=True) == "value"
    assert db.read(uuid) is None


@pytest.mark.skipif(not requirements_met, reason=reason)
def test_next(db: PostgreSQLAdapter14, uuid):
    """Test method `next` of class `PostgreSQLAdapter14`."""
    db.write(uuid, "value")
    assert db.next() == (uuid, "value")
    assert db.next(True) == (uuid, "value")
    assert db.next(True) is None


@pytest.mark.skipif(not requirements_met, reason=reason)
def test_push(db: PostgreSQLAdapter14):
    """Test method `push` of class `PostgreSQLAdapter14`."""
    assert len(db.keys()) == 0
    uuid = db.push("value")
    assert uuid
    assert db.read(uuid) == "value"


@pytest.mark.skipif(not requirements_met, reason=reason)
def test_delete_non_existing(db: PostgreSQLAdapter14, uuid):
    """
    Test method `delete` of class `PostgreSQLAdapter14` without existing
    key.
    """
    db.delete(uuid)


@pytest.mark.skipif(not requirements_met, reason=reason)
def test_delete(db: PostgreSQLAdapter14, uuid):
    """Test method `delete` of class `PostgreSQLAdapter14`."""
    db.write(uuid, "value")
    db.delete(uuid)
    assert db.read(uuid) is None


@pytest.mark.skipif(not requirements_met, reason=reason)
def test_keys(db: PostgreSQLAdapter14):
    """Test method `keys` of class `PostgreSQLAdapter14`."""
    assert db.keys() == ()
    db.write(uuid1 := str(uuid4()), "value1")
    assert db.keys() == (uuid1, )
    db.write(uuid2 := str(uuid4()), "value2")
    assert db.keys() == (uuid1, uuid2)


@pytest.mark.skipif(not requirements_met, reason=reason)
@pytest.mark.parametrize(
    "json",
    [
        None,
        {"key": "value"},
        {},
        ["a", 1, True],
        [],
        [{"key1": ["value1", "value2"], "key2": 1}, True, 0.1],
        "'", "''",
        "\"", "\"value\"",
        "\\", "\\\\",
        "line1\nline2.1\tline2.2",
        "*~@#$%^&*()_+=><?/"
    ],
    ids=[
        "null", "object", "object-empty", "array", "array-empty", "nested",
        "single-quote1", "single-quote2",
        "double-quote1", "double-quote2",
        "backslash", "backslash2",
        "newline-tab", "misc"
    ]
)
def test_json(db: PostgreSQLAdapter14, json):
    """
    Test storing various JSON-content with class `PostgreSQLAdapter14`.
    """

    uuid = db.push(json)
    assert db.read(uuid) == json
