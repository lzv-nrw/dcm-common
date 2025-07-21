"""
This module contains an interface for the definition of adapters to
different SQL database implementations.
"""

from typing import Optional, Any, Callable
from dataclasses import dataclass, field
from collections.abc import Mapping
import abc
import uuid
import json
from pathlib import Path

from .pooling import Connection, ConnectionPool


@dataclass
class RawTransactionResult:
    """
    Unprocessed output of a transaction.

    After a transaction, `data` contains a list of all generated outputs
    as returned from `Claim.execute`.
    """

    args: list["_Statement"] = field(default_factory=list)
    data: list[Any] = field(default_factory=list)
    error: Optional[Exception] = None


@dataclass
class TransactionResult:
    """Processed output of a transaction."""

    success: bool
    data: Optional[Any] = None
    msg: str = field(default_factory=lambda: "")
    raw: Optional[RawTransactionResult] = None

    def eval(self, context: Optional[str] = None) -> Any:
        """
        Evaluate the `TransactionResult`.
        Raises a `ValueError` if not successful, or returns `data`
        otherwise.

        Keyword arguments:
        context -- str to add in the error message (in case of failure)
                   (default None leads to "")
        """
        if not self.success:
            raise ValueError(
                (f"{context} " if context is not None else "") + self.msg
            )
        return self.data


class PooledConnectionAdapter(metaclass=abc.ABCMeta):
    """
    Interface for database-adapter with connection pool.

    Required definitions:
    _CONNECTION_FACTORY -- factory for generating database connections

    Keyword arguments:
    pool_size -- maximum number of persistent connections to the
                 database
                 (default 1)
    allow_overflow -- whether to create one-time connections if the pool
                      is used at full capacity
                      (default True)
    connect_now -- whether to populate pool immediately
                   (default True)
    """

    def __init__(
        self,
        pool_size: int = 1,
        allow_overflow: bool = True,
        connect_now: bool = True,
    ):
        self.pool = ConnectionPool(
            self._CONNECTION_FACTORY,
            pool_size,
            allow_overflow,
            connect_now,
        )

    @abc.abstractmethod
    def _CONNECTION_FACTORY(  # pylint: disable=invalid-name
        self,
    ) -> Callable[[], Connection]:
        """Factory for generating database connections."""
        raise NotImplementedError(
            f"Class '{self.__class__.__name__}' does not define property "
            + "'_CONNECTION_FACTORY'."
        )


@dataclass
class _Statement:
    value: str


class SQLAdapter(metaclass=abc.ABCMeta):
    """
    Interface for adapters to SQL databases. Responses to queries are
    encapsulated in a `TransactionResult`-object. It provides methods to
    run custom commands and prepared statements (crud). Only the latter
    are considered secure when processing user input.

    The implementation:
    * supports the data types: boolean, integer, jsonb, text and uuid
      (see `_decode` and `_encode` methods) and
    * allows to generate unique (uuid-based) primary keys automatically
      on insert for uuid and text types (see `insert` method).

    # Implementation guide
    A `SQLAdapter` can inherit most requirements directly from this
    class. Below are the additional requirements imposed by the
    interface.

    DB-schema-related methods like `get_column_names` can oftentimes be
    cached (e.g., when only using prepared statements). In order to
    support caching while also allowing custom commands, the
    `clear_schema_cache` method can be defined. This method is
    automatically called in all potentially schema-mutating methods that
    this interface defines.

    ## Required definitions
    * `_execute` executes the given cmd and returns the associated
      `RawTransactionResult`-object
    * `_get_table_columns` returns a list of table column names
    * `_get_table_names` returns a list of table names
    * `_get_column_types` returns a mapping of column names and their
      (lowercase) type in the table
    * `_get_primary_key` returns the table's primary key
    * `_read_file` reads commands from an input file
    """

    TRANSACTION_BEGIN = _Statement("BEGIN")
    TRANSACTION_COMMIT = _Statement("COMMIT")
    TRANSACTION_ROLLBACK = _Statement("ROLLBACK")

    @abc.abstractmethod
    def _execute(
        self,
        *statements: _Statement,
        on_success: Optional[_Statement] = None,
        on_fail: Optional[_Statement] = None,
    ) -> RawTransactionResult:
        """
        Runs the given `statements` and returns the associated
        `RawTransactionResult`-object.
        """
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define "
            + "method '_execute'."
        )

    def execute(
        self,
        *statements: _Statement,
        on_success: Optional[_Statement] = None,
        on_fail: Optional[_Statement] = None,
        clear_schema_cache=True,
    ) -> RawTransactionResult:
        """
        Runs the given `statements` and returns the associated
        `RawTransactionResult`-object. If `clear_schema_cache`, the
        db-schema-cache is cleared beforehand. The hooks `on_success`
        and `on_fail` can be used to finalize the result.
        """
        if clear_schema_cache:
            self.clear_schema_cache()
        return self._execute(
            *statements,
            on_success=on_success,
            on_fail=on_fail,
        )

    def build_response(
        self,
        raw: RawTransactionResult,
        post_process: Optional[Callable[[RawTransactionResult], Any]] = None,
        handle_error: Callable[
            [RawTransactionResult], Optional[TransactionResult]
        ] = None,
    ) -> TransactionResult:
        """
        Build a `TransactionResult` object based on a `result` and an optional
        `post_process` callable.

        Keyword arguments:
        raw -- `RawTransactionResult`-object
        post_process -- post-processing that generates
                        `TransactionResult.data`; gets passed the
                        `RawTransactionResult`
                        (default None; corresponds to lambda r: r.data)
        handle_error -- optional custom error-handling; gets passed the
                        `RawTransactionResult` and should return either
                        a `TransactionResult` (on error) or `None` (if
                        ok)
                        (default None; `success` is `False` if
                        `raw.error` is not None)
        """
        if handle_error:
            error_response = handle_error(raw)
            if error_response:
                return error_response
        else:
            if raw.error is not None:
                return TransactionResult(
                    False,
                    msg=(
                        "Transaction "
                        + f"'{'; '.join([s.value for s in raw.args])}'"
                        + f" resulted in a {type(raw.error).__name__}: "
                        + str(raw.error)
                    ),
                    raw=raw,
                )

        if post_process is None:

            def post_process(r: RawTransactionResult):
                return r.data

        return TransactionResult(True, data=post_process(raw), raw=raw)

    @abc.abstractmethod
    def _get_table_names(self) -> TransactionResult:
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define "
            + "method '_get_table_names'."
        )

    def get_table_names(self, clear_schema_cache=False) -> TransactionResult:
        """
        Returns a list with the table names. If `clear_schema_cache`,
        the db-schema-cache is cleared beforehand.
        """
        if clear_schema_cache:
            self.clear_schema_cache()
        return self._get_table_names()

    @abc.abstractmethod
    def _get_column_types(self, table: str) -> TransactionResult:
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define "
            + "method '_get_column_types'."
        )

    def get_column_types(
        self, table: str, clear_schema_cache=False
    ) -> TransactionResult:
        """
        Returns a mapping of column name and type in the given `table`.
        If `clear_schema_cache`, the db-schema-cache is cleared
        beforehand.
        """
        if clear_schema_cache:
            self.clear_schema_cache()
        return self._get_column_types(table)

    @abc.abstractmethod
    def _get_column_names(self, table: str) -> TransactionResult:
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define "
            + "method '_get_column_names'."
        )

    def get_column_names(
        self, table: str, clear_schema_cache=False
    ) -> TransactionResult:
        """
        Returns a list of the column names in the given `table`. If
        `clear_schema_cache`, the db-schema-cache is cleared beforehand.
        """
        if clear_schema_cache:
            self.clear_schema_cache()
        return self._get_column_names(table)

    @abc.abstractmethod
    def _get_primary_key(self, table: str) -> TransactionResult:
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define "
            + "method '_get_primary_key'."
        )

    def get_primary_key(
        self, table: str, clear_schema_cache=False
    ) -> TransactionResult:
        """
        Returns the name of the primary key of the given `table`. If
        `clear_schema_cache`, the db-schema-cache is cleared beforehand.
        """
        if clear_schema_cache:
            self.clear_schema_cache()
        return self._get_primary_key(table)

    @abc.abstractmethod
    def _read_file(self, path: Path) -> TransactionResult:
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define "
            + "method '_read_file'."
        )

    def read_file(
        self, path: Path, clear_schema_cache=True
    ) -> TransactionResult:
        """
        Reads commands from an input file. If `clear_schema_cache`,
        cache will be cleared.
        """
        if clear_schema_cache:
            self.clear_schema_cache()
        return self._read_file(path)

    def clear_schema_cache(self) -> None:
        """Clears all db-schema-related caches."""

    @staticmethod
    def _escape_single_quote(value: str) -> None:
        return value.replace("'", "''")

    def custom_cmd(
        self,
        cmd: str,
        clear_schema_cache=True,
    ) -> TransactionResult:
        """
        Runs the given cmd and returns the associated
        `TransactionResult`-object.

        Keyword arguments:
        cmd -- command
        clear_schema_cache -- whether to clear db-schema-cache beforehand
        """
        return self.build_response(
            self.execute(
                _Statement(cmd),
                clear_schema_cache=clear_schema_cache,
            )
        )

    @classmethod
    def decode(cls, value: Any, type_: str) -> str:
        """Decodes a `value` of `type_` for use in custom-statements."""
        return cls._decode(value, type_)

    @classmethod
    def encode(cls, value: Any, type_: str) -> str:
        """Encodes a `value` of `type_` for custom response-parsing."""
        return cls._encode(value, type_)

    @classmethod
    def _check_decode_null(cls, value: Any) -> bool:
        """Returns `True` if `value` decodes to `NULL`."""
        return value is None

    @classmethod
    def _decode_null(cls) -> str:
        """Returns decoded `NULL`."""
        return "NULL"

    @classmethod
    def _decode_text(cls, value: str) -> str:
        """Returns decoded `TEXT`."""
        if not isinstance(value, str):
            raise TypeError(
                f"Cannot decode value '{value}' of type 'TEXT'. "
                + f"Bad input type '{type(value).__name__}'."
            )
        return f"'{cls._escape_single_quote(value)}'"

    @classmethod
    def _decode_uuid(cls, value: str) -> str:
        """Returns decoded `UUID`."""
        try:
            uuid.UUID(value)
        except ValueError as exc_info:
            raise TypeError(
                f"Cannot decode value '{value}' of type 'UUID'. "
                + "Invalid UUID."
            ) from exc_info
        return cls._decode_text(value)

    @classmethod
    def _decode_integer(cls, value: int) -> str:
        """Returns decoded `INTEGER`."""
        if not isinstance(value, int):
            raise TypeError(
                f"Cannot decode value '{value}' of type 'INTEGER'. "
                + f"Bad input type '{type(value).__name__}'."
            )
        return f"'{cls._escape_single_quote(str(value))}'"

    @classmethod
    def _decode_boolean(cls, value: bool) -> str:
        """Returns decoded `BOOLEAN`."""
        if not isinstance(value, bool):
            raise TypeError(
                f"Cannot decode value '{value}' of type 'BOOLEAN'. "
                + f"Bad input type '{type(value).__name__}'."
            )
        return "TRUE" if value else "FALSE"

    @classmethod
    def _decode_jsonb(cls, value: Any) -> str:
        """Returns decoded `JSONB`."""
        try:
            return f"'{cls._escape_single_quote(json.dumps(value))}'"
        except TypeError as exc_info:
            raise TypeError(
                f"Cannot decode value '{value}' of type 'JSONB'. Bad input."
            ) from exc_info

    @classmethod
    def _decode(cls, value: Any, type_: str) -> str:
        """Decodes a value for inserting in the database."""
        if cls._check_decode_null(value):
            return cls._decode_null()
        match type_.lower():
            case "text":
                return cls._decode_text(value)
            case "uuid":
                return cls._decode_uuid(value)
            case "integer":
                return cls._decode_integer(value)
            case "boolean":
                return cls._decode_boolean(value)
            case "jsonb":
                return cls._decode_jsonb(value)
            case _:
                raise TypeError(
                    f"Cannot decode value '{value}' of type '{type_}'."
                )

    @classmethod
    def _check_encode_null(cls, value: Any) -> bool:
        """Returns `True` if `value` encodes to `None`."""
        return value is None

    @classmethod
    def _encode_null(cls) -> None:
        """Returns encoded `NULL`."""
        return None

    @classmethod
    def _encode_text(cls, value: str) -> str:
        """Returns encoded `TEXT`."""
        return value

    @classmethod
    def _encode_uuid(cls, value: str) -> str:
        """Returns encoded `UUID`."""
        try:
            uuid.UUID(value)
        except TypeError as e:
            raise TypeError(
                f"Cannot encode value '{value}' of type 'UUID'. "
                + "Invalid UUID."
            ) from e
        return value

    @classmethod
    def _encode_integer(cls, value: int) -> int:
        """Returns encoded `INTEGER`."""
        if not isinstance(value, int):
            raise TypeError(
                f"Cannot encode value '{value}' of type 'INTEGER'. "
                + f"Bad input type '{type(value).__name__}'."
            )
        return value

    @classmethod
    def _encode_boolean(cls, value: Any) -> bool:
        """Returns encoded `BOOLEAN`."""
        if value:
            return True
        return False

    @classmethod
    def _encode_jsonb(cls, value: str) -> bool:
        """Returns encoded `JSONB`."""
        if not isinstance(value, str):
            raise TypeError(
                f"Cannot encode value '{value}' of type 'JSONB'. "
                + f"Bad input type '{type(value).__name__}'."
            )
        try:
            return json.loads(value)
        except json.JSONDecodeError as exc_info:
            raise TypeError(
                f"Cannot encode value '{value}' of type 'JSONB'. Bad input."
            ) from exc_info
        if value:
            return True
        return False

    @classmethod
    def _encode(cls, value: Any, type_: str) -> Any:
        """Encodes a value for reading from the database."""
        if cls._check_encode_null(value):
            return cls._encode_null()
        match type_:
            case "text":
                return cls._encode_text(value)
            case "uuid":
                return cls._encode_uuid(value)
            case "integer":
                return cls._encode_integer(value)
            case "boolean":
                return cls._encode_boolean(value)
            case "jsonb":
                return cls._encode_jsonb(value)
            case _:
                raise TypeError(
                    f"Cannot encode value '{value}' of type '{type_}'."
                )

    def _validate_table_name(self, table: str) -> None:
        """Raises `ValueError` if `table` is unknown."""
        if table not in self.get_table_names().eval():
            raise ValueError(f"Unknown table '{table}'.")

    def _validate_cols_names(self, table: str, cols: list[str]) -> None:
        """Raises `ValueError` if at least one in `cols` is unknown."""
        unknown_col = next(
            (x for x in cols if x not in self.get_column_names(table).eval()),
            None,
        )
        if unknown_col is not None:
            raise ValueError(
                f"Unknown column '{unknown_col}' for table '{table}'."
            )

    def get_insert_statement(self, table: str, row: Mapping) -> _Statement:
        """
        Returns an INSERT-`_Statement` based on the given `table` and
        `row` with the pattern
        ```
        INSERT INTO <table> (<rows>)
        VALUES (<row-data>)
        RETURNING <primary-key-value>
        ```
        """
        # validate potentially malicious input
        self._validate_table_name(table)
        self._validate_cols_names(table, list(row.keys()))

        types = self._get_column_types(table).eval()
        return _Statement(
            f"INSERT INTO {table} ("
            + ", ".join(self._escape_single_quote(col) for col in row)
            + ") "
            + "VALUES ("
            + ", ".join(
                [
                    self._decode(v, types[col])
                    for col, v in row.items()
                    if col in types
                ]
            )
            + ") "
            + f"RETURNING {self.get_primary_key(table).data}"
        )

    def insert(
        self, table: str, row: Mapping, generate_primary_key: bool = True
    ) -> TransactionResult:
        """
        Inserts row in the table.
        Returns the value of the primary key on success (otherwise None).

        Keyword arguments:
        table -- table name
        row -- dict to insert in a new row
        generate_primary_key -- whether to automatically generate the primary
                                key when the given row does not contain it;
                                only supports `uuid` and `text` types
                                (default True)
        """
        primary_key = self.get_primary_key(table).data
        auto_pk = {}
        pk_type = self._get_column_types(table).eval()[primary_key]
        if (
            primary_key is not None
            and generate_primary_key
            and primary_key not in row
        ):
            if pk_type not in ["uuid", "text"]:
                return TransactionResult(
                    success=False,
                    msg=(
                        "Cannot automatically generate a primary key "
                        + f"for type '{pk_type}'."
                    ),
                )
            auto_pk[primary_key] = str(uuid.uuid4())
        try:
            return self.build_response(
                self.execute(
                    self.get_insert_statement(table, row | auto_pk),
                    clear_schema_cache=False,
                ),
                post_process=lambda r: self._encode(r.data[0][0], pk_type),
            )
        except TypeError as exc_info:
            return self.build_response(RawTransactionResult(error=exc_info))

    def get_update_statement(self, table: str, row: Mapping) -> _Statement:
        """
        Returns an UPDATE-`_Statement` based on the given `table` and
        `row` with the pattern
        ```
        UPDATE <table>
        SET <row-data x = y>
        WHERE <primary-key> = <primary-key-value>
        ```
        """
        # validate potentially malicious input
        self._validate_table_name(table)
        self._validate_cols_names(table, list(row.keys()))

        primary_key = self.get_primary_key(table).eval()
        types = self._get_column_types(table).eval()
        return _Statement(
            f"UPDATE {table} SET "
            + ", ".join(
                [
                    f"{col} = {self._decode(v, types[col])}"
                    for col, v in row.items()
                    if col != primary_key and col in types
                ]
            )
            + f" WHERE {primary_key} = "
            + f"{self._decode(row[primary_key], types[primary_key])}"
        )

    def update(self, table: str, row: Mapping) -> TransactionResult:
        """
        Updates row in the table (requires table to have a primary key).
        The primary key should be contained in `row`.

        Keyword arguments:
        table -- table name
        row -- dict to update an existing row of the table
        """
        pk = self.get_primary_key(table).data
        if pk is None:
            return TransactionResult(
                False, msg=f"The table '{table}' does not have a primary key."
            )
        if pk not in row:
            return TransactionResult(
                False, msg=f"Missing primary key '{pk}' in row '{row}'."
            )
        if len(row) == 1:  # row contains only primary key
            return TransactionResult(
                False, msg=f"Missing data in row '{row}'."
            )
        try:
            return self.build_response(
                self.execute(
                    self.get_update_statement(table, row),
                    clear_schema_cache=False,
                )
            )
        except TypeError as exc_info:
            return self.build_response(RawTransactionResult(error=exc_info))

    def get_delete_statement(
        self, table: str, value: str, col: Optional[str] = None
    ) -> _Statement:
        """
        Returns a DELETE-`_Statement` based on the given `table` where
        rows with `col=value` are deleted.

        Keyword arguments:
        table -- table name
        value -- value to match
        col -- column name to match the value
               (default None leads to the primary key)
        """
        # validate potentially malicious input
        self._validate_table_name(table)
        self._validate_cols_names(table, [col] if col else [])

        if col is None:
            col = self.get_primary_key(table).eval()
        types = self.get_column_types(table).eval()
        return _Statement(
            f"DELETE FROM {table} WHERE {col} = "
            + f"{self._decode(value, types[col])}"
        )

    def delete(
        self, table: str, value: str, col: Optional[str] = None
    ) -> TransactionResult:
        """
        Deletes the rows from the table where col equals value.

        Keyword arguments:
        table -- table name
        value -- value to match
        col -- column name to match the value
               (default None leads to the primary key)
        """
        try:
            return self.build_response(
                self.execute(
                    self.get_delete_statement(table, value, col),
                    clear_schema_cache=False,
                )
            )
        except TypeError as exc_info:
            return self.build_response(RawTransactionResult(error=exc_info))

    def get_select_statement(
        self,
        table: str,
        value: Optional[str] = None,
        col: Optional[str] = None,
        cols: Optional[list[str]] = None,
    ) -> _Statement:
        """
        Returns a SELECT-`_Statement` based on the given parameters.

        Keyword arguments:
        table -- table name
        value -- value of the key to match
                 (default None leads to fetching all rows)
        col -- column name whose value to match
               (default None leads to the primary key)
        cols -- columns to include in the response
                (default None leads to all the columns)
        """
        # validate potentially malicious input
        self._validate_table_name(table)
        self._validate_cols_names(
            table, ([col] if col else []) + (cols if cols else [])
        )

        if col is None:
            col = self.get_primary_key(table).eval()

        types = self.get_column_types(table).eval()
        return _Statement(
            f"SELECT {', '.join(['*'] if cols is None else cols)} "
            + f"FROM {table}"
            + (
                f" WHERE ({col} = {self._decode(value, types[col])})"
                if value is not None
                else ""
            )
            + ""
        )

    def get_rows(
        self,
        table: str,
        value: Optional[str] = None,
        col: Optional[str] = None,
        cols: Optional[list[str]] = None,
    ) -> TransactionResult:
        """
        Gets the rows from the table where col equals value.
        Returns a list of rows as dict (with cols as keys)
        (or an empty list if no match is found).

        Keyword arguments:
        table -- table name
        value -- value of the col to match
                 (default None leads to fetching all rows)
        col -- name of column to match
               (default None leads to the primary key)
        cols -- columns to include in the response
                (default None leads to all the columns)
        """
        if cols is None:
            cols = self.get_column_names(table).eval()

        assert isinstance(cols, list)  # mypy-hint

        try:
            raw = self.execute(
                self.get_select_statement(table, value, col, cols),
                clear_schema_cache=False,
            )
        except TypeError as exc_info:
            return self.build_response(RawTransactionResult(error=exc_info))
        if len(raw.data) == 0:
            return self.build_response(
                raw, post_process=lambda r: []
            )

        types = self._get_column_types(table).eval()
        return self.build_response(
            raw,
            post_process=lambda r: [
                {
                    col: self._encode(v, types[col])
                    for col, v in zip(cols, row)
                } for row in r.data
            ],
        )

    def get_row(
        self, table: str, value: str, cols: Optional[list[str]] = None
    ) -> TransactionResult:
        """
        Gets the row from the table where the primary key equals value.
        Returns a dict (with cols as keys), or None if value does not
        exist.

        Keyword arguments:
        table -- table name
        value -- value of the primary key to match
        cols -- columns to include in the response
                (default None leads to all the columns)
        """
        if cols is None:
            cols = self.get_column_names(table).data

        pk = self.get_primary_key(table).data
        if pk is None:
            return TransactionResult(
                False, msg=f"The table '{table}' does not have a primary key."
            )

        rows = self.get_rows(table, value, pk, cols)

        if not rows.success:
            return rows
        if len(rows.data) == 0:
            return TransactionResult(
                True,
                msg=(
                    f"Empty query for primary key ({pk} = '{value}') in table "
                    + f"'{table}'."
                ),
                raw=rows,
            )
        if len(rows.data) > 1:  # should not be possible
            return TransactionResult(
                False,
                msg=(
                    f"Unexpected result: non-unique primary key ({pk} = "
                    + f"'{value}') in table '{table}' (data = '{rows.data}')."
                ),
                raw=rows,
            )

        rows.data = rows.data[0]
        return rows

    def get_column(self, table: str, column: str) -> TransactionResult:
        """
        Returns a list with the values of the column.

        Keyword arguments:
        table -- table name
        column -- column name
        """
        rows = self.get_rows(table, cols=[column])

        if not rows.success:
            return rows

        rows.data = [row[column] for row in rows.data]
        return rows

    def new_transaction(
        self,
        post_process: Optional[Callable[[RawTransactionResult], Any]] = None,
    ) -> "Transaction":
        """Returns new `Transaction`-context manager.

        Keyword arguments:
        post_process -- post-processing to perform on `data` before
                        returning `TransactionResult` on commit
                        (default None)
        """
        return Transaction(self, post_process=post_process)


class Transaction:
    """
    Multi-statement transactions. If used as a context manager, the
    statements added to this transaction are commited on exit.

    Keyword arguments:
    db -- database adapter
    statements -- initial list of `_Statements`
                  (default None)
    post_process -- post-processing to perform on `data` before
                    returning `TransactionResult` on commit
                    (default None)
    """

    def __init__(
        self,
        db: SQLAdapter,
        statements: Optional[list[_Statement]] = None,
        post_process: Optional[Callable[[RawTransactionResult], Any]] = None,
    ) -> None:
        self.db = db
        self.statements = [] if statements is None else statements
        self.result: Optional[TransactionResult] = None
        self.post_process = post_process
        self._clear_schema_cache = len(self.statements) > 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.commit()

    def add(self, statement: str | _Statement) -> None:
        """Register a custom `statement` for this transaction."""
        self._clear_schema_cache = True
        self.statements.append(
            _Statement(statement) if isinstance(statement, str) else statement
        )

    def add_insert(self, table: str, row: Mapping) -> None:
        """
        Register an INSERT-`_Statement` for this transaction.

        Keyword arguments:
        table -- table name
        row -- row-data to be inserted
        """
        self.statements.append(self.db.get_insert_statement(table, row))

    def add_update(self, table: str, row: Mapping) -> None:
        """
        Register an UPDATE-`_Statement` for this transaction.

        Keyword arguments:
        table -- table name
        row -- row-data to be updated
        """
        self.statements.append(self.db.get_update_statement(table, row))

    def add_delete(
        self, table: str, value: str, col: Optional[str] = None
    ) -> None:
        """
        Register a DELETE-`_Statement` for this transaction.

        Keyword arguments:
        table -- table name
        value -- value to match
        col -- column name to match the value
               (default None leads to the primary key)
        """
        self.statements.append(self.db.get_delete_statement(table, value, col))

    def add_select(
        self,
        table: str,
        value: Optional[str] = None,
        key: Optional[str] = None,
        cols: Optional[list[str]] = None,
    ) -> None:
        """
        Register a SELECT-`_Statement` for this transaction.

        Keyword arguments:
        table -- table name
        value -- value of the key to match
                 (default None leads to fetching all rows)
        key -- key whose value to match
               (default None leads to the primary key)
        cols -- columns to include in the response
                (default None leads to all the columns)
        """
        self.statements.append(
            self.db.get_select_statement(table, value, key, cols)
        )

    def commit(
        self,
        post_process: Optional[Callable[[RawTransactionResult], Any]] = None,
    ) -> TransactionResult:
        """
        Commit the given transaction.

        Keyword arguments:
        post_process -- post-processing to perform on `data` before
                        returning `TransactionResult`
                        (default None)
        """
        self.result = self.db.build_response(
            self.db.execute(
                self.db.TRANSACTION_BEGIN,
                *self.statements,
                clear_schema_cache=self._clear_schema_cache,
                on_success=self.db.TRANSACTION_COMMIT,
                on_fail=self.db.TRANSACTION_ROLLBACK,
            ),
            post_process or self.post_process,
        )
        return self.result
