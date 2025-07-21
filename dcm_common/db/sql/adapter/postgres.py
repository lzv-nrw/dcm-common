"""
This module contains a definition for a PostgreSQL database adapter
that operates based on a database that is reachable via psql.
The class implements the interface `SQLAdapter`.
"""

from typing import Optional, Callable, Any
import os
from pathlib import Path
from functools import lru_cache

import psycopg

from .pooling import Connection, Claim
from .interface import (
    SQLAdapter,
    PooledConnectionAdapter,
    _Statement,
    RawTransactionResult,
    TransactionResult,
)


_DB_ADAPTER_SCHEMA_CACHE_SIZE = int(
    os.environ.get("DB_ADAPTER_SCHEMA_CACHE_SIZE", 64)
)


class PostgreSQLConnection(Connection):
    """
    Implementation of a PostgreSQL-connection based on the `psycopg`-
    package.
    """
    # pylint: disable=no-member
    def __init__(
        self,
        *args,
        host: Optional[str] = None,
        port: Optional[str] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        passfile: Optional[str] = None,
        **kwargs
    ):
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        self._passfile = passfile
        super().__init__(*args, **kwargs)

    def _connect(self) -> None:
        self._conn = psycopg.connect(
            host=self._host,
            port=self._port,
            dbname=self._database,
            user=self._user,
            password=self._password,
            passfile=self._passfile,
            autocommit=True
        )

        class UUIDLoader(psycopg.adapt.Loader):
            """Use custom loader-methods for UUID."""
            def load(self, data):
                return bytes(data).decode("utf-8")

        class JSONBoader(psycopg.adapt.Loader):
            """Use custom loader-methods for JSONB."""
            def load(self, data):
                return bytes(data).decode("utf-8")

        self._conn.adapters.register_loader("uuid", UUIDLoader)
        self._conn.adapters.register_loader("jsonb", JSONBoader)

    def _close(self) -> None:
        self._conn.close()

    def _create_claim(self) -> None:
        return Claim(self, self._conn.cursor())

    def _destroy_claim(self, claim: Claim) -> None:
        claim.cursor.close()

    def _execute(self, claim: Claim, cmd: str) -> None:
        claim.cursor.execute(cmd)

    def _fetch(self, claim: Claim) -> Any:
        # no output in cursor yet
        if claim.cursor.rownumber is None:
            return []

        # aggregate all accumulated output
        result = []
        while True:
            result.extend(claim.cursor.fetchall())
            if claim.cursor.nextset() is None:
                break
        return result

    @property
    def healthy(self) -> tuple[bool, str]:
        if self._conn.broken:
            return False, "Connection broken."
        if self._conn.closed:
            return False, "Connection closed."
        try:
            cursor = self._conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
        # pylint: disable=broad-exception-caught
        except Exception as exc_info:
            return False, str(exc_info)
        return True, ""


class PostgreSQLAdapter14(PooledConnectionAdapter, SQLAdapter):
    """
    Adapter for interacting with a PostgreSQL-database (v14).

    Keyword arguments:
    host -- host name for database application
            (default None)
    port -- port for database application
            (default None)
    database -- database name
                (default None)
    user -- database user
            (default None)
    password -- database user password (use is discouraged)
                (default None)
    pgpassfile -- path to pgpass-file (requires proper access rights
                  `chmod 600 ..`)
                  (default None)
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
    ) -> Callable[[], PostgreSQLConnection]:
        """Factory for generating database connections."""
        conn = PostgreSQLConnection(
            host=self._host,
            port=self._port,
            database=self._database,
            user=self._user,
            password=self._password,
            passfile=self._passfile,
        )
        return conn

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[str] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        passfile: Optional[str | Path] = None,
        pool_size: int = 1,
        allow_overflow: bool = True,
        connect_now: bool = True,
        connection_timeout: Optional[float] = 10,
    ) -> None:
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        if passfile is None:
            self._passfile = None
        elif isinstance(passfile, str):
            self._passfile = passfile
        else:
            self._passfile = str(passfile.resolve())
        self.connection_timeout = connection_timeout
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
        try:
            return self.build_response(
                self.execute(
                    *[
                        _Statement(s.strip())
                        for s in path.read_text(encoding="utf-8").split(";")
                    ],
                    clear_schema_cache=False,
                )
            )
        except FileNotFoundError as exc_info:
            return self.build_response(
                RawTransactionResult([], error=exc_info)
            )

    @lru_cache(maxsize=1)
    def _get_table_names(self) -> TransactionResult:
        raw = self.execute(
            _Statement(
                """
                SELECT table_name
                FROM INFORMATION_SCHEMA.TABLES
                WHERE table_schema='public' AND table_type='BASE TABLE'
                """
            ),
            clear_schema_cache=False,
        )
        return self.build_response(
            raw, post_process=lambda r: [table[0] for table in raw.data]
        )

    @lru_cache(maxsize=_DB_ADAPTER_SCHEMA_CACHE_SIZE)
    def _get_column_types(self, table: str) -> TransactionResult:
        raw = self.execute(
            _Statement(
                f"""
                SELECT column_name, data_type
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_name = '{table}'
                """
            ),
            clear_schema_cache=False,
        )
        if len(raw.data) == 0:
            return TransactionResult(
                False, msg=f"Table '{table}' does not exist.", raw=raw,
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
            _Statement(
                f"""
                SELECT column_name
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_name = '{table}'
                """
            ),
            clear_schema_cache=False,
        )
        if len(raw.data) == 0:
            return TransactionResult(
                False, msg=f"Table '{table}' does not exist.", raw=raw,
            )
        return self.build_response(
            raw,
            post_process=lambda r: [
                colinfo[0] for colinfo in r.data
            ],
        )

    @lru_cache(maxsize=_DB_ADAPTER_SCHEMA_CACHE_SIZE)
    def _get_primary_key(self, table: str) -> TransactionResult:
        raw = self.execute(
            _Statement(
                f"""
                SELECT a.attname AS data_type
                FROM pg_index i
                JOIN
                    pg_attribute a ON a.attrelid = i.indrelid
                    AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = '{table}'::regclass AND i.indisprimary
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
        return self.build_response(
            raw, post_process=lambda x: x.data[0][0]
        )

    def clear_schema_cache(self):
        # omit clearing cache for _build_base as it does not change
        self._get_table_names.cache_clear()
        self._get_column_types.cache_clear()
        self._get_column_names.cache_clear()
        self._get_primary_key.cache_clear()
