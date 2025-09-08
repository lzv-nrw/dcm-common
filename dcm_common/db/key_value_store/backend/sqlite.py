"""Definition of a sqlite-based key-value store-type database."""

from typing import Optional, Any
import sys
from pathlib import Path
import sqlite3
import json
from uuid import uuid4
import threading

from .interface import KeyValueStore


if sys.version_info[0] != 3:
    raise ImportError(f"Module '{__name__}' is only compatible with python 3.")


class Transaction:
    """
    Auxiliary definition for SQLite3-database transactions.

    This is a duplicate of the implementation for the sqlite-based
    orchestra-controller.

    Keyword arguments:
    conn -- database connection (e.g. via `get_connection`)
    check -- if `True`, raise occuring errors, otherwise only track via
             `success` and `exc_val`-properties
             (default False)
    autoclose -- automatically close connection after use
                 (default True)
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        check: bool = True,
        autoclose: bool = True,
    ) -> None:
        self.connection = conn
        self._autoclose = autoclose
        self._check = check
        self.cursor: Optional[sqlite3.Cursor] = None
        self.data: Optional[list[Any]] = None
        self.success: Optional[bool] = None
        self.exc_val: Optional[Exception] = None

    @staticmethod
    def get_connection(path: str | Path, **kwargs) -> sqlite3.Connection:
        """
        Returns `sqlite3.Connection` for multiple threads.
        """
        if sys.version_info[1] >= 12:
            conn = sqlite3.connect(path, autocommit=True, **kwargs)
            # PRAGMA only works in autocommit-mode..
            conn.execute("PRAGMA foreign_keys = 1")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.autocommit = False
        else:
            conn = sqlite3.connect(path, isolation_level=None, **kwargs)
            conn.execute("PRAGMA foreign_keys = 1")
            conn.execute("PRAGMA journal_mode = WAL")
        return conn

    def check(self) -> None:
        """
        Raises exception from `self.exc_value` if not `self.success.
        """
        if self.success:
            return
        raise self.exc_val or ValueError("Unknown error occurred.")

    def __enter__(self):
        self.cursor = self.connection.cursor()
        if sys.version_info[1] < 12:
            self.cursor.execute("BEGIN")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.success = exc_type is None

        if self.success:
            self.data = self.cursor.fetchall()
            if self.connection.in_transaction:
                self.connection.commit()
        else:
            self.exc_val = exc_val
            if self.connection.in_transaction:
                self.connection.rollback()

        self.cursor.close()
        if self._autoclose:
            self.connection.close()

        if self._check and not self.success:
            return False
        return True


class SQLiteStore(KeyValueStore):
    """
    Key value-store for JSON-data that works on a SQLite3-database. This
    class is compatible with `threading` via a threading-lock when
    used as in-memory store. For `multiprocessing`-support (and
    equivalent) the persistent mode is required (using the `path`-
    argument).

    Keyword arguments:
    path -- path to a SQLite-database file
            (default None; uses memory_id)
    memory_id -- identifier for a shared in-memory database (single
                 process only)
                 (default None; uses ':memory:')
    timeout -- timeout duration for creating a database connection in
               seconds (mostly relevant for concurrency; see also
               property `db`)
               (default 5)
    """

    def __init__(
        self,
        path: Optional[Path | str] = None,
        memory_id: Optional[str] = None,
        *,
        timeout: Optional[float] = 5,
    ) -> None:
        self._path = path
        self._memory_id = memory_id

        self.timeout = timeout
        self._threading_db_lock = threading.Lock()

        # always keep one connection when working in memory
        if path is None:
            self._db = self.get_connection()
        else:
            self._db = None

        if self._get_schema_version() == 0:
            self._load_schema()

    def get_connection(self):
        """
        Returns a new database-connection (uses the store's timeout
        setting).
        """
        if self._path is not None:
            return Transaction.get_connection(self._path, timeout=self.timeout)
        if self._memory_id is None:
            self._memory_id = str(uuid4())
        return Transaction.get_connection(
            f"file:{self._memory_id}?mode=memory&cache=shared",
            uri=True,
            timeout=self.timeout,
        )

    def transaction(self, check: bool = True):
        """
        Returns `Transaction`-object connected to the store's
        database.
        """
        return Transaction(self.get_connection(), check=check)

    def close(self):
        """Closes internal database connection."""
        if self._db is not None:
            self._db.close()

    def _get_schema_version(self) -> None:
        """
        Returns value of user_version.
        """
        with self.transaction() as t:
            t.cursor.execute("PRAGMA user_version")

        return t.data[0][0]

    def _load_schema(self) -> None:
        """Loads database schema."""
        with self.transaction() as t:
            t.cursor.execute("PRAGMA user_version = 1")
            t.cursor.execute(
                """CREATE TABLE store (
                  key TEXT NOT NULL PRIMARY KEY,
                  value TEXT NOT NULL
                )"""
            )

    def _encode(self, value):
        return json.dumps(value)

    def _decode(self, value):
        if value is None:
            return None
        return json.loads(value)

    def _write(self, key, value):
        with self._threading_db_lock, self.transaction() as t:
            t.cursor.execute(
                "INSERT OR REPLACE INTO store VALUES (?, ?)", (key, value)
            )

    def _read(self, key):
        with self._threading_db_lock, self.transaction() as t:
            t.cursor.execute("SELECT value FROM store WHERE key = ?", (key,))
        if len(t.data) == 0:
            return None
        return t.data[0][0]

    def _delete(self, key):
        with self._threading_db_lock, self.transaction() as t:
            t.cursor.execute("DELETE FROM store WHERE key = ?", (key,))

    def keys(self):
        with self._threading_db_lock, self.transaction() as t:
            t.cursor.execute("SELECT key FROM store")
        return tuple(row[0] for row in t.data)
