"""Definition of a sqlite-based `orchestra.Controller`."""

from typing import Optional, Any, Mapping
import sys
from pathlib import Path
import sqlite3
from datetime import datetime, timedelta
import json
from uuid import uuid4
import threading
import socket
from copy import deepcopy

from dcm_common import LoggingContext, Logger
from ..models import (
    Token,
    Progress,
    Status,
    JobMetadata,
    JobInfo,
    Lock,
    Instruction,
    Message,
)
from .interface import Controller
from ..logging import Logging


if sys.version_info[0] != 3:
    raise ImportError(f"Module '{__name__}' is only compatible with python 3.")


class Transaction:
    """
    Auxiliary definition for SQLite3-database transactions.

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


class SQLiteController(Controller):
    """
    Orchestra-Controller that works on a SQLite3-database. This class
    is compatible with `threading` via a threading-lock when used as
    in-memory controller. For `multiprocessing`-support (and equivalent)
    the persistent mode is required (using the `path`-argument).

    This is a duplicate of the implementation for the sqlite-based
    key-value-store.

    Keyword arguments:
    path -- path to a SQLite-database file
            (default None; uses memory_id)
    memory_id -- identifier for a shared in-memory database (single
                 process only)
                 (default None; uses ':memory:')
    name -- optional name tag for this controller (used in logging)
            (default None; generates unique name from hostname)
    requeue -- whether to requeue jobs that have failed
               (default False)
    lock_ttl -- time to live for a lock on a job in the job registry
                (default 10)
    token_ttl -- time to live for a record in the job registry; None
                 corresponds to no expiration
                 (default 3600)
    message_ttl -- time to live for a message; None corresponds to no
                   expiration
                   (default 360)
    timeout -- timeout duration for creating a database connection in
               seconds (mostly relevant for concurrency; see also
               property `db`)
               (default 5)
    """

    SCHEMA_VERSION = 1

    def __init__(
        self,
        path: Optional[Path | str] = None,
        memory_id: Optional[str] = None,
        name: Optional[str] = None,
        requeue: bool = False,
        *,
        lock_ttl: int = 10,
        token_ttl: Optional[int] = 3600,
        message_ttl: Optional[int] = 360,
        timeout: Optional[float] = 5,
    ) -> None:
        self._path = path
        self._memory_id = memory_id

        if name is None:
            self._name = (
                f"Controller-{socket.gethostname()}-{str(uuid4())[:8]}"
            )
        else:
            self._name = name
        self.requeue = requeue

        self.tz = datetime.now().astimezone().tzinfo
        self.lock_ttl = lock_ttl
        self.token_ttl = token_ttl
        self.message_ttl = message_ttl
        self.timeout = timeout
        self._threading_db_lock = threading.Lock()

        # always keep one connection when working in memory
        if path is None:
            self._db = self.db
        else:
            self._db = None

        if self._check_schema_version() == 0:
            self._load_schema()

    @property
    def name(self):
        """Returns controller name."""
        return self._name

    @property
    def db(self):
        """
        Returns a new database-connection (uses the controller's timeout
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
        Returns `Transaction`-object connected to the controller's
        database.
        """
        return Transaction(self.db, check=check)

    def close(self):
        """Closes internal database connection."""
        if self._db is not None:
            self._db.close()

    def _check_schema_version(self) -> None:
        """
        Validates database schema version. Raises `ValueError` if
        version is set and incompatible. Otherwise returns value of
        user_version.
        """
        with self.transaction() as t:
            t.cursor.execute("PRAGMA user_version")

        # perform checks here if schemas changes in future
        # ...

        return t.data[0][0]

    def _load_schema(self) -> None:
        """Loads database schema."""
        with self.transaction() as t:
            t.cursor.execute(f"PRAGMA user_version = {self.SCHEMA_VERSION}")
            t.cursor.execute(
                """CREATE TABLE registry (
                  token TEXT NOT NULL PRIMARY KEY,
                  status TEXT CHECK(
                    status IN (
                      'queued', 'running', 'completed', 'aborted', 'failed'
                    )
                  ) NOT NULL,
                  -- JSON of JobInfo-object
                  info TEXT NOT NULL,
                  -- token expiration; seconds since epoch
                  expires_at INTEGER
                )"""
            )
            t.cursor.execute(
                """CREATE TABLE locks (
                  id TEXT NOT NULL PRIMARY KEY,
                  name TEXT NOT NULL,
                  token TEXT
                    NOT NULL
                    UNIQUE
                    REFERENCES registry (token)
                    ON DELETE CASCADE,
                  -- lock expiration; seconds since epoch
                  expires_at INTEGER NOT NULL
                )"""
            )
            t.cursor.execute(
                """CREATE TABLE messages (
                  token TEXT
                    NOT NULL
                    REFERENCES registry (token)
                    ON DELETE CASCADE,
                  -- instruction like 'abort'
                  instruction TEXT
                    CHECK( instruction IN ( 'abort' ) )
                    NOT NULL,
                  origin TEXT NOT NULL,
                  -- instruction specific
                  content TEXT NOT NULL,
                  -- timestamp of message received; seconds since epoch
                  received_at INTEGER NOT NULL,
                  -- message expiration; seconds since epoch
                  expires_at INTEGER
                )"""
            )

    def queue_push(self, token: str, info: Mapping | JobInfo) -> Token:
        """
        Add job to queue, returns `Token` if successful or already
        existing or `None` otherwise.

        If `info` is not passed as `JobInfo`, adds the `token` and
        `produced`-metadata before submission.
        """
        self.cleanup()

        _token = Token(
            token,
            self.token_ttl is not None,
            (
                (datetime.now() + timedelta(seconds=self.token_ttl)).replace(
                    microsecond=0, tzinfo=self.tz
                )
                if self.token_ttl is not None
                else None
            ),
        )
        if isinstance(info, JobInfo):
            info = deepcopy(info)
            info.token = _token
            info.metadata.produce(self._name)
            if isinstance(info.report, Mapping):
                info.report["token"] = _token.json
            elif info.report is not None:
                info.report.token = _token

        with self._threading_db_lock, self.transaction(False) as t:
            t.cursor.execute(
                "INSERT INTO registry VALUES (?, ?, ?, ?)",
                (
                    token,
                    "queued",
                    json.dumps(
                        info if isinstance(info, Mapping) else info.json
                    ),
                    (
                        None
                        if _token.expires_at is None
                        else int(_token.expires_at.timestamp())
                    ),
                ),
            )
        # new submission
        if t.success:
            Logging.print_to_log(
                f"Controller '{self._name}' accepted job '{token}'.",
                Logging.LEVEL_DEBUG,
            )
            return _token
        # resubmission
        _token = self.get_token(token)
        _info = self.get_info(token)
        if _info.get("config", {}).get("original_body") != (
            info.config.original_body
            if isinstance(info, JobInfo)
            else info.get("config", {}).get("original_body")
        ):
            raise ValueError("Resubmission with different body not allowed.")
        return _token

    def queue_pop(self, name: str) -> Optional[Lock]:
        """Request a lock on a job from the queue."""
        self.cleanup()

        lock_id = str(uuid4())
        with self._threading_db_lock:
            expires_at = (
                datetime.now() + timedelta(seconds=self.lock_ttl)
            ).replace(microsecond=0)
            with self.transaction() as t:
                t.cursor.execute(
                    """WITH available_tokens AS (
                        SELECT token from registry
                        WHERE status = 'queued'
                        AND NOT EXISTS (
                          SELECT 1 FROM locks
                          WHERE locks.token = registry.token)
                        LIMIT 1)
                      INSERT INTO locks
                        SELECT ?, ?, token, ? FROM available_tokens
                    """,
                    (lock_id, name, int(expires_at.timestamp())),
                )
                t.cursor.execute(
                    "SELECT token FROM locks where id = ?",
                    (lock_id,),
                )
            if t.success and len(t.data) > 0:
                return Lock(lock_id, name, t.data[0][0], expires_at)

        # no work
        return None

    def release_lock(self, lock_id: str) -> None:
        """Releases a lock on a job from the queue."""
        with self._threading_db_lock, self.transaction() as t:
            t.cursor.execute("DELETE from locks WHERE id = ?", (lock_id,))

    def refresh_lock(self, lock_id: str) -> Lock:
        """
        Refreshes a lock on a job from the queue. Raises `ValueError` if
        not successful.
        """
        self.cleanup()

        with self._threading_db_lock, self.transaction() as t:
            t.cursor.execute(
                "SELECT name, token, expires_at FROM locks WHERE id = ?",
                (lock_id,),
            )
            data = t.cursor.fetchone()
            now = int(datetime.now().replace(microsecond=0).timestamp())
            if data is None or data[2] < now:
                raise ValueError("Stale lock, refresh rejected.")
            expires_at = now + self.lock_ttl
            t.cursor.execute(
                "UPDATE locks SET expires_at = ? WHERE id = ?",
                (expires_at, lock_id),
            )

        return Lock(
            lock_id, data[0], data[1], datetime.fromtimestamp(expires_at)
        )

    def cleanup(self) -> None:
        """Runs a cleanup for registry and locks regarding expiration."""
        # invalidate broken locks
        now = int(datetime.now().timestamp())
        with self._threading_db_lock, self.transaction() as t:
            t.cursor.execute("DELETE from locks WHERE expires_at < ?", (now,))
            t.cursor.execute(
                "DELETE from registry WHERE expires_at < ?", (now,)
            )
            t.cursor.execute(
                "DELETE from messages WHERE expires_at < ?", (now,)
            )

            # update status and info in registry where needed
            # conditions:
            # * set to running
            # * no lock is present
            # update:
            # * info.metadata
            # * info.report.progress
            # * info.report.log
            t.cursor.execute(
                """SELECT token, info from registry
                  WHERE status = 'running' AND NOT EXISTS (
                    SELECT 1 FROM locks
                    WHERE registry.token = locks.token
                  )
                """
            )
            failed_tokens = t.cursor.fetchall()
            for token, info_str in failed_tokens:
                try:
                    # parse existing info
                    info = json.loads(info_str)
                    if "metadata" not in info:
                        info["metadata"] = JobMetadata()
                    else:
                        info["metadata"] = JobMetadata.from_json(
                            info["metadata"]
                        )
                    if "report" not in info:
                        info["report"] = {}
                    if "log" in info["report"]:
                        info["report"]["log"] = Logger.from_json(
                            info["report"]["log"]
                        )
                    else:
                        info["report"]["log"] = Logger()
                    if self.requeue:
                        # report-progress
                        info["report"]["progress"] = Progress(
                            status=Status.QUEUED,
                            verbose=f"requeued by controller '{self._name}'",
                        ).json
                        # report-log
                        info["report"]["log"].log(
                            LoggingContext.EVENT,
                            origin=self._name,
                            body=(
                                f"Requeued by controller '{self._name}' due "
                                + "to failed state."
                            ),
                        )
                        # metadata
                        info["metadata"].consumed = None
                        info["metadata"].completed = None
                        info["metadata"].aborted = None
                    else:
                        # report-progress
                        info["report"]["progress"] = Progress(
                            status=Status.ABORTED,
                            verbose=f"aborted by controller '{self._name}'",
                        ).json
                        # report-log
                        info["report"]["log"].log(
                            LoggingContext.ERROR,
                            origin=self._name,
                            body=(
                                f"Aborted by controller '{self._name}' due to "
                                + "failed state."
                            ),
                        )
                        # metadata
                        info["metadata"].abort(self._name)

                    # back to serialized
                    info["report"]["log"] = info["report"]["log"].json
                    info["metadata"] = info["metadata"].json
                    t.cursor.execute(
                        """UPDATE registry
                          SET status = ?, info = ?
                          WHERE token = ?
                        """,
                        (
                            "queued" if self.requeue else "failed",
                            json.dumps(info),
                            token,
                        ),
                    )
                    Logging.print_to_log(
                        f"Controller '{self._name}' "
                        + f"{'requeued' if self.requeue else 'finalized'} "
                        + f"a failed job (token: {token}).",
                        Logging.LEVEL_INFO,
                    )
                # pylint: disable=broad-exception-caught
                except Exception as exc_info:
                    t.cursor.execute(
                        """UPDATE registry
                          SET status = ?
                          WHERE token = ?
                        """,
                        (
                            "queued" if self.requeue else "failed",
                            token,
                        ),
                    )
                    Logging.print_to_log(
                        f"Controller '{self._name}' failed to handle "
                        + f"the report of a failed job (token: {token}): "
                        + str(exc_info),
                        Logging.LEVEL_ERROR,
                    )

    def get_token(self, token: str) -> Token:
        """Fetch token-data from registry."""
        self.cleanup()

        with self._threading_db_lock, self.transaction() as t:
            t.cursor.execute(
                "SELECT expires_at FROM registry WHERE token = ?", (token,)
            )

        if len(t.data) == 0:
            raise ValueError(f"Unknown job token '{token}'.")

        return Token(
            token,
            t.data[0][0] is not None,
            (
                None
                if t.data[0][0] is None
                else datetime.fromtimestamp(t.data[0][0], tz=self.tz)
            ),
        )

    def get_info(self, token: str) -> Any:
        """Fetch info from registry as JSON."""
        self.cleanup()

        with self._threading_db_lock, self.transaction() as t:
            t.cursor.execute(
                "SELECT info FROM registry WHERE token = ?", (token,)
            )

        if len(t.data) == 0:
            raise ValueError(f"Unknown job token '{token}'.")

        return json.loads(t.data[0][0])

    def get_status(self, token: str) -> str:
        """Fetch status from registry."""
        self.cleanup()

        with self._threading_db_lock, self.transaction() as t:
            t.cursor.execute(
                "SELECT status FROM registry WHERE token = ?", (token,)
            )

        if len(t.data) == 0:
            raise ValueError(f"Unknown job token '{token}'.")

        return t.data[0][0]

    def registry_push(
        self,
        lock_id: str,
        *,
        status: Optional[str] = None,
        info: Optional[Mapping | JobInfo] = None,
    ) -> None:
        """Push new data to registry."""
        self.cleanup()

        if status is None and info is None:
            return

        # get lock
        with self._threading_db_lock, self.transaction() as t:
            t.cursor.execute(
                "SELECT token, expires_at FROM locks WHERE id = ?", (lock_id,)
            )

        if len(t.data) == 0:
            raise ValueError("Stale lock, update to job registry rejected.")

        token, expires_at = t.data[0]

        # check expiration
        if datetime.now().timestamp() > expires_at:
            raise ValueError("Stale lock, update to job registry rejected.")

        # run update
        args = []
        statement = []
        if status is not None:
            statement.append("status = ?")
            args.append(status)
        if info is not None:
            statement.append("info = ?")
            args.append(
                json.dumps(info if isinstance(info, Mapping) else info.json)
            )
        statement = ",".join(statement)
        args.append(token)

        with self._threading_db_lock, self.transaction() as t:
            t.cursor.execute(
                f"UPDATE registry SET {statement} WHERE token = ?",
                args,
            )

    def message_push(
        self, token: str, instruction: str, origin: str, content: str
    ) -> None:
        """Posts message."""
        with self._threading_db_lock, self.transaction(False) as t:
            t.cursor.execute(
                "INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?)",
                (
                    token,
                    instruction,
                    origin,
                    content,
                    int(datetime.now().timestamp()),
                    (
                        int(
                            (
                                datetime.now()
                                + timedelta(seconds=self.message_ttl)
                            ).timestamp()
                        )
                        if self.message_ttl is not None
                        else None
                    ),
                ),
            )

        # check
        if not t.success:
            if (
                isinstance(t.exc_val, sqlite3.IntegrityError)
                and "foreign key constraint failed" in str(t.exc_val).lower()
            ):
                # token already dropped or never existed > discarded message
                Logging.print_to_log(
                    f"Orchestration controller '{self._name}' received a "
                    + f"message for the token '{token}' which does not "
                    + "exist.",
                    Logging.LEVEL_INFO,
                )
                return None
            # otherwise valid error
            t.check()

    def message_get(self, since: Optional[datetime | int]) -> list[Message]:
        """Returns a list of relevant messages."""
        since_ = 0
        if isinstance(since, int):
            since_ = since
        if isinstance(since, datetime):
            since_ = int(since.timestamp())

        self.cleanup()

        with self._threading_db_lock, self.transaction() as t:
            t.cursor.execute(
                "SELECT * FROM messages WHERE received_at >= ?",
                (since_,),
            )

        return list(
            map(
                lambda message: Message(
                    message[0],
                    Instruction(message[1]),
                    message[2],
                    message[3],
                    datetime.fromtimestamp(message[4]),
                    (
                        None
                        if message[5] is None
                        else datetime.fromtimestamp(message[5])
                    ),
                ),
                t.data,
            )
        )
