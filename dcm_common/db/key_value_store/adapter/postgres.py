"""
This module contains a definition for a key-value store-type database
adapter that operates based on a database that is reachable via psql.
"""

from typing import Optional
from pathlib import Path
import os
import subprocess
from json import loads, dumps
from random import choice

from .interface import KeyValueStoreAdapter


class PostgreSQLAdapter14(KeyValueStoreAdapter):
    """
    Implementation of a `KeyValueStoreAdapter` for interacting with a
    PostgreSQL-database (v14) via psql. This adapter only supports
    tables that can be expressed as `(id TEXT, value JSONB)`.

    Keyword arguments:
    key_name -- table primary key name
    value_name -- table value column name
    table -- database table name
    host -- host name for database application
            (default None)
    port -- port for database application
            (default None)
    user -- database user
            (default None)
    password -- database user password (use is discouraged)
                (default None)
    database -- database name
                (default None)
    pgpassfile -- path to pgpass-file (sets environment variable
                  `PGPASSFILE`; requires proper access rights `chmod 600
                  ..`)
                  (default None)
    additional_options -- additional command line options
                          (default None)
    """

    def __init__(
        self,
        key_name: str,
        value_name: str,
        table: str,
        host: Optional[str] = None,
        port: Optional[int | str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        pgpassfile: Optional[str | Path] = None,
        additional_options: Optional[list[str]] = None
    ) -> None:
        self._key_name = key_name
        self._value_name = value_name
        self._table = table
        self._host = host
        self._port = str(port)
        self._user = user
        self._password = password
        self._env = {}
        if self._password is not None:
            self._env["PGPASSWORD"] = str(password)
        self._database = database
        self._pgpassfile = pgpassfile
        if self._pgpassfile is not None:
            self._env["PGPASSFILE"] = str(pgpassfile)
        self._additional_options = additional_options
        self._cmd = self._build_base()

    def _build_base(self) -> list[str]:
        """Returns base of the psql-command as list."""
        cmd = ["psql", "-w", "-q", "-1", "-v", "ON_ERROR_STOP=on"]
        if self._host is not None:
            cmd += ["-h", self._host]
        if self._port is not None:
            cmd += ["-p", self._port]
        if self._user is not None:
            cmd += ["-U", self._user]
        if self._database is not None:
            cmd += ["-d", self._database]
        if self._additional_options is not None:
            cmd += self._additional_options
        return cmd

    def _check(self, result):
        if result.returncode != 0:
            raise ValueError(
                f"Non-zero error code {result.returncode} while processing "
                + f"command '{' '.join(result.args)}': {result.stderr}."
            )

    def _escape_single_quote(self, value: str):
        return value.replace("'", "''")

    def _to_jsonb(self, value):
        return dumps(value).replace("\\", "\\\\").replace("'", "''")

    def _from_jsonb(self, value):
        return loads(value)

    def _run(self, cmd, options=None) -> subprocess.CompletedProcess:
        """
        Runs the given `psql`-cmd and returns the associated
        `subprocess.CompletedProcess`-object.

        Keyword arguments:
        cmd -- `psql`-command
        options -- additional command line options
        """
        return subprocess.run(
            self._cmd + (options if options else []) + ["-c", cmd],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=os.environ | self._env
        )

    def custom_cmd(
        self, cmd: str, options: Optional[list[str]] = None
    ) -> subprocess.CompletedProcess:
        """
        Runs the given `psql`-cmd and returns the associated
        `subprocess.CompletedProcess`-object.

        Keyword arguments:
        cmd -- `psql`-command
        options -- additional command line options
        """
        return self._run(cmd, options)

    def clear(self) -> None:
        """
        Truncates table.
        """
        self._check(
            self._run(f"TRUNCATE {self._table};", ["-t"])
        )

    def read(self, key, pop=False):
        if pop:
            result = self._run(
                f"DELETE FROM {self._table} "
                + f"WHERE ({self._key_name}='{self._escape_single_quote(key)}') "
                + f"RETURNING {self._value_name};",
                ["-t"]
            )
        else:
            result = self._run(
                f"SELECT {self._value_name} FROM {self._table} "
                + f"WHERE ({self._key_name}='{self._escape_single_quote(key)}');",
                ["-t"]
            )
        self._check(result)
        lines = result.stdout.strip().split("\n")
        if len(lines) > 1:
            raise ValueError(
                f"Reading value for key '{key}' in table '{self._table}' "
                + f"gave an unexpected response: {result.stdout}."
            )
        if lines[0] == "":
            return None
        return self._from_jsonb(lines[0].strip())

    def next(self, pop=False):
        keys = self.keys()
        if not keys:
            return None
        key = choice(keys)
        if not pop:
            return key, self.read(key)
        result = self._run(
            f"DELETE FROM {self._table} "
            + f"WHERE ({self._key_name}='{self._escape_single_quote(key)}') "
            + "RETURNING *;",
            ["-t"]
        )
        self._check(result)
        lines = result.stdout.strip().split("\n")
        if len(lines) > 1:
            raise ValueError(
                f"Popping key '{key}' from table '{self._table}' "
                + f"successful, but got unexpected response: {result.stdout}."
            )
        key_, value = lines[0].split(" | ", maxsplit=1)
        if key != key_.strip():
            raise ValueError(
                f"Popping key '{key}' from table '{self._table}' "
                + "successful, but returned key does not match "
                + f"('{key}' vs '{key_}'): {result.stdout}."
            )
        return key, self._from_jsonb(value)

    def write(self, key, value):
        self._check(
            self._run(
                f"INSERT INTO {self._table} "
                + f"VALUES ('{self._escape_single_quote(key)}', E'{self._to_jsonb(value)}') "
                + f"ON CONFLICT ({self._key_name}) "
                + f"DO UPDATE SET {self._value_name} = EXCLUDED.{self._value_name};"
            )
        )

    def push(self, value):
        result = self._run(
            f"INSERT INTO {self._table} "
            + f"VALUES (DEFAULT, E'{self._to_jsonb(value)}') "
            + f"RETURNING {self._key_name};",
            ["-t"]
        )
        self._check(result)
        lines = result.stdout.strip().split("\n")
        if len(lines) > 1:
            raise ValueError(
                f"Inserting value '{value}' into table '{self._table}' "
                + f"successful, but got unexpected response: {result.stdout}."
            )
        return lines[0].strip()

    def delete(self, key):
        self._check(
            self._run(
                f"DELETE FROM {self._table} "
                + f"WHERE {self._key_name} = '{self._escape_single_quote(key)}';"
            )
        )

    def keys(self):
        result = self._run(
            f"SELECT {self._key_name} FROM {self._table};", ["-t"]
        )
        self._check(result)
        return tuple(
            key.strip() for key in result.stdout.strip().split("\n")
            if key.strip() != ""
        )
