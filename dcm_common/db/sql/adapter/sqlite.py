"""
This module contains a definition for an SQLite database adapter.
The class implements the interface `SQLAdapter`.
"""

from typing import Optional, Any, Callable
import sys
import os
from pathlib import Path
from functools import lru_cache
import sqlite3

from .pooling import Connection, Claim
from .interface import (
    TransactionResult,
    RawTransactionResult,
    PooledConnectionAdapter,
    SQLAdapter,
    _Statement,
)


_DB_ADAPTER_SCHEMA_CACHE_SIZE = int(
    os.environ.get("DB_ADAPTER_SCHEMA_CACHE_SIZE", 64)
)


class SQLiteConnection(Connection):
    """
    Implementation of a SQLite-connection based on the `sqlite3`-
    package.
    """

    def __init__(self, *args, db_file: Optional[str | Path] = None, **kwargs):
        self._db_file = ":memory:" if db_file is None else str(db_file)
        super().__init__(*args, **kwargs)

    def _connect(self) -> None:
        if sys.version_info[1] >= 12:
            self._conn = sqlite3.connect(
                self._db_file, autocommit=True, check_same_thread=False
            )
        else:
            self._conn = sqlite3.connect(
                self._db_file, isolation_level=None, check_same_thread=False
            )

    def _close(self) -> None:
        self._conn.close()

    def _create_claim(self) -> None:
        return Claim(self, self._conn.cursor())

    def _destroy_claim(self, claim: Claim) -> None:
        claim.cursor.close()

    def _execute(self, claim: Claim, cmd: str) -> None:
        claim.cursor.execute(cmd)

    def _fetch(self, claim: Claim) -> Any:
        return claim.cursor.fetchall()

    @property
    def healthy(self) -> tuple[bool, str]:
        try:
            cursor = self._conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
        # pylint: disable=broad-exception-caught
        except Exception as exc_info:
            return False, str(exc_info)
        return True, ""


class SQLiteAdapter3(PooledConnectionAdapter, SQLAdapter):
    """
    Adapter for interacting with a SQLite-database (v3).

    Keyword arguments:
    db_file -- path to database file
               (default None; uses in-memory database, only compatible
               with pool size of one and disallowed overflow)
    pool_size -- maximum connection pool-size
                 (default 1)
    allow_overflow -- whether to create one-time connections if the pool
                      is used at full capacity
                      (default True)
    connect_now -- whether to populate pool immediately
                   (default True)
    connection_timeout -- timeout for making an individual claim for a
                          connection
                          (default 10)
    """

    def _CONNECTION_FACTORY(  # pylint: disable=invalid-name
        self,
    ) -> Callable[[], SQLiteConnection]:
        """Factory for generating database connections."""
        conn = SQLiteConnection(db_file=self._db_file)
        with conn.claim(timeout=self.connection_timeout) as c:
            c.execute("PRAGMA foreign_keys = ON")
        return conn

    def __init__(
        self,
        db_file: Optional[str | Path] = None,
        pool_size: int = 1,
        allow_overflow: bool = True,
        connect_now: bool = True,
        connection_timeout: Optional[float] = 10,
    ) -> None:
        self._db_file = db_file
        self.connection_timeout = connection_timeout
        if not self._db_file and (pool_size != 1 or allow_overflow):
            raise ValueError(
                "SQLite in-memory database requires `pool_size=1` and "
                + "`allow_overflow=False`."
            )
        if pool_size < 1 and not allow_overflow:
            raise ValueError(
                "Adapter initialization needs at least `pool_size=1` or "
                + "`allow_overflow=True`."
            )
        super().__init__(
            pool_size=pool_size,
            allow_overflow=allow_overflow,
            connect_now=connect_now,
        )

    def _execute(
        self,
        *statements: _Statement,
        on_success: Optional[_Statement] = None,
        on_fail: Optional[_Statement] = None,
    ) -> RawTransactionResult:
        raw = RawTransactionResult(statements)

        def extend_if_not_empty(data):
            """If data is not empty, add to raw result."""
            if data:
                raw.data.extend(data)

        with self.pool.get_claim(timeout=self.connection_timeout) as c:
            # run main transaction
            try:
                for statement in statements:
                    extend_if_not_empty(c.execute(statement.value))
            # pylint: disable=broad-exception-caught
            except Exception as exc_info:
                raw.error = exc_info

            # run conditionals
            if raw.error is None:
                if on_success is not None:
                    extend_if_not_empty(c.execute(on_success.value))
            else:
                if on_fail is not None:
                    extend_if_not_empty(c.execute(on_fail.value))

        return raw

    def _read_file(self, path: Path) -> TransactionResult:
        # sqlite-module provides executescript method
        # parse as single statement
        try:
            statement = _Statement(path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc_info:
            return self.build_response(
                RawTransactionResult([], error=exc_info)
            )

        with self.pool.get_claim(timeout=self.connection_timeout) as c:
            # run as script
            try:
                result = c.cursor.executescript(statement.value).fetchall()
            # pylint: disable=broad-exception-caught
            except Exception as exc_info:
                return self.build_response(
                    RawTransactionResult([statement], error=exc_info)
                )

        return self.build_response(
            RawTransactionResult([statement], data=result)
        )

    @lru_cache(maxsize=1)
    def _get_table_names(self) -> TransactionResult:
        raw = self.execute(
            _Statement("SELECT name FROM sqlite_master WHERE type='table'"),
            clear_schema_cache=False,
        )
        return self.build_response(
            raw, post_process=lambda r: [table[0] for table in raw.data]
        )

    @lru_cache(maxsize=_DB_ADAPTER_SCHEMA_CACHE_SIZE)
    def _get_column_types(self, table: str) -> TransactionResult:
        raw = self.execute(
            _Statement(f"SELECT name, type FROM PRAGMA_TABLE_INFO('{table}')"),
            clear_schema_cache=False,
        )
        if len(raw.data) == 0:
            return TransactionResult(
                False,
                msg=f"Table '{table}' does not exist.",
                raw=raw,
            )
        return self.build_response(
            raw,
            post_process=lambda r: {
                colinfo[0]: colinfo[1].strip().lower() for colinfo in r.data
            },
        )

    @lru_cache(maxsize=_DB_ADAPTER_SCHEMA_CACHE_SIZE)
    def _get_column_names(self, table: str) -> TransactionResult:
        raw = self.execute(
            _Statement(f"SELECT name FROM PRAGMA_TABLE_INFO('{table}')"),
            clear_schema_cache=False,
        )
        if len(raw.data) == 0:
            return TransactionResult(
                False,
                msg=f"Table '{table}' does not exist.",
                raw=raw,
            )
        return self.build_response(
            raw,
            post_process=lambda r: [colinfo[0] for colinfo in r.data],
        )

    @lru_cache(maxsize=_DB_ADAPTER_SCHEMA_CACHE_SIZE)
    def _get_primary_key(self, table: str) -> TransactionResult:
        raw = self.execute(
            _Statement(
                f"""
                SELECT l.name
                FROM pragma_table_info('{table}') as l
                WHERE l.pk = 1
                """
            ),
            clear_schema_cache=False,
        )
        if len(raw.data) == 0:
            return TransactionResult(
                False,
                msg=f"Table '{table}' does not exist or has no primary key.",
                raw=raw,
            )
        return self.build_response(raw, post_process=lambda x: x.data[0][0])

    def clear_schema_cache(self):
        # omit clearing cache for _build_base as it does not change
        self._get_table_names.cache_clear()
        self._get_column_types.cache_clear()
        self._get_column_names.cache_clear()
        self._get_primary_key.cache_clear()
