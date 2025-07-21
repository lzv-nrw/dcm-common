"""Definitions for Database connection pooling."""

from typing import Optional, Callable, Any
import threading
import abc
from dataclasses import dataclass
from random import randint


@dataclass
class Claim:
    """
    Connection claim.

    Needed to authorize when running commands with a `Connection`. Can
    also be used as a context manager.
    """

    connection: "Connection"
    cursor: Optional[Any] = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.connection.release(self)

    def validate(self, other: "Claim") -> bool:
        """Validate claim."""
        return id(self) == id(other)

    def execute(self, cmd: str) -> Any:
        """
        Executes a command on the associated connection and returns the
        result.

        Keyword arguments:
        cmd -- instruction to run
        """
        return self.connection.execute(self, cmd)

    def release(self) -> None:
        """Release claim."""
        self.connection.release(self)


class Connection(metaclass=abc.ABCMeta):
    """
    Interface for persistent database connections.

    Keyword arguments:
    connect -- if `True` connect immediately

    Required definitions:
    _connect -- establish connection
    _close -- close connection
    _create_claim -- returns `Claim` with reference to this connection
                     and a cursor object (that can be used to run
                     commands)
    _destroy_claim -- invalidate claim and close asssociated cursor
    _execute -- run command using the given claim.cursor
    _fetch -- returns the response data from the last command
    healthy -- returns `True` if the connection is still open
    """

    def __init__(self, connect: bool = True) -> None:
        self._conn: Any = None

        self._lock = threading.Lock()
        self._connected = threading.Event()

        self._claim_lock = threading.Lock()
        self._unclaimed = threading.Event()
        self._unclaimed.set()
        self._claim: Optional[Claim] = None
        if connect:
            self.connect()

    def connect(self) -> None:
        """Open connection."""
        with self._lock:
            if not self._connected.is_set():
                self._connect()
                self._connected.set()

    def close(self) -> None:
        """Close connection."""
        with self._lock:
            if self._connected.is_set():
                self._close()
                self._connected.clear()

    @property
    def connected(self) -> None:
        """Returns `True` if the connection has been opened."""
        return self._connected.is_set()

    @abc.abstractmethod
    def _connect(self) -> None:
        raise NotImplementedError(
            f"Class '{self.__class__.__name__}' does not define property "
            + "'_connect'."
        )

    @abc.abstractmethod
    def _close(self) -> None:
        raise NotImplementedError(
            f"Class '{self.__class__.__name__}' does not define method "
            + "'_close'."
        )

    @abc.abstractmethod
    def _create_claim(self) -> Claim:
        """Generate `Claim`-object."""
        raise NotImplementedError(
            f"Class '{self.__class__.__name__}' does not define method "
            + "'_create_claim'."
        )

    @abc.abstractmethod
    def _destroy_claim(self, claim: Claim) -> None:
        """Destroy `Claim`-object."""
        raise NotImplementedError(
            f"Class '{self.__class__.__name__}' does not define method "
            + "'_destroy_claim'."
        )

    @abc.abstractmethod
    def _execute(self, claim: Claim, cmd: str) -> None:
        """Execute the given command."""
        raise NotImplementedError(
            f"Class '{self.__class__.__name__}' does not define method "
            + "'_execute'."
        )

    @abc.abstractmethod
    def _fetch(self, claim: Claim) -> Any:
        """Fetch data."""
        raise NotImplementedError(
            f"Class '{self.__class__.__name__}' does not define method "
            + "'_fetch'."
        )

    @property
    @abc.abstractmethod
    def healthy(self) -> tuple[bool, str]:
        """Returns `True` if the connection is still open."""
        raise NotImplementedError(
            f"Class '{self.__class__.__name__}' does not define property "
            + "'healthy'."
        )

    @property
    def claimed(self) -> bool:
        """
        Returns `True` if there is an active claim on this connection.
        """
        return not self._unclaimed.is_set()

    def claim(
        self, block: bool = False, timeout: Optional[float] = None
    ) -> Optional[Claim]:
        """
        Attempt to claim this connection. Returns a `Claim` if
        successful otherwise `None`.

        Keyword arguments:
        block -- whether to block until claim can be made
                 (default False)
        timeout -- wait duration until `None` is returned in seconds; if
                   `None`, waits indefinitely (only relevant if `block
                   is True`)
                   (default None)
        """
        healthy, msg = self.healthy
        if not healthy:
            raise ConnectionError(f"Tried to claim bad connection: {msg}")
        with self._claim_lock:
            # check
            if (
                block and not self._unclaimed.wait(timeout)
            ) or not self._unclaimed.is_set():
                return None

            # configure on success
            self._unclaimed.clear()
            self._claim = self._create_claim()
            return self._claim

    def release(self, claim: Claim) -> None:
        """Release claim on connection."""
        if not self._claim:
            raise ConnectionError("Tried to release an unclaimed connection.")
        if not self._claim.validate(claim):
            raise ConnectionError("Tried to release a connection with bad claim.")
        self._destroy_claim(claim)
        self._claim = None
        self._unclaimed.set()

    def execute(self, claim: Claim, cmd: str) -> Any:
        """
        Execute the given command.

        Keyword arguments:
        claim -- proof of claim
        cmd -- instruction to run (passed into stdin)
        """
        if not self._claim:
            raise ConnectionError(
                "Tried to run command on an unclaimed connection."
            )
        if not self._claim.validate(claim):
            raise ConnectionError(
                "Tried to run command on a connection without valid claim."
            )
        healthy, msg = self.healthy
        if not healthy:
            raise ConnectionError(
                f"Connection has not been opened yet or is broken: {msg}"
            )
        self._execute(claim, cmd)
        return self._fetch(claim)


class ConnectionPool:
    """
    Database connection pool implementation.

    The strategy for picking a connection from the pool consists of two
    stages:
    * attempt to claim an unused connection,
    * if that fails for all connections in the pool, either wait for any
      connection to free up (if `allow_overflow is False`) or create a
      temporary connection (if `allow_overflow is True`)

    Keyword arguments:
    connection_factory -- factory for `Connection`-objects
    pool_size -- maximum number of persistent connections to the
                 database
                 (default 1)
    allow_overflow -- whether to create one-time connections if the pool
                      is used at full capacity
                      (default False)
    connect_now -- whether to populate pool immediately
                   (default True)
    """

    def __init__(
        self,
        connection_factory: Callable[[], Connection],
        pool_size: int = 1,
        allow_overflow: bool = False,
        connect_now: bool = True,
    ) -> None:
        self._connection_factory = connection_factory
        self._pool_size = pool_size
        self._allow_overflow = allow_overflow

        self._pool_lock = threading.RLock()
        self._open = connect_now
        if connect_now:
            self._pool: Optional[list[Connection]] = self._init_pool()
        else:
            self._pool = None
        self._overflow_lock = threading.Lock()
        self._overflow: list[Connection] = []

    def init_pool(self) -> None:
        """Initialize pool by connecting to database."""
        if self._open:
            raise RuntimeError("Pool is already open.")
        self._pool = self._init_pool()
        self._open = True

    def _init_pool(self) -> list[Connection]:
        """Initialize pool."""
        return [self._init_connection() for _ in range(self._pool_size)]

    def _init_connection(self) -> Connection:
        """Initialize connection."""
        c = self._connection_factory()
        if not c.connected:
            c.connect()
        healthy, msg = c.healthy
        if not healthy:
            raise ConnectionError(
                f"Connection broken immediately after initialization: {msg}"
            )
        return c

    def _cleanup_overflow(self) -> None:
        """Kills unused overflow-connections."""
        with self._overflow_lock:
            for c in self._overflow.copy():
                if not c.claimed:
                    c.close()
                self._overflow.remove(c)

    @property
    def is_open(self) -> bool:
        """Returns `True` if the pool is open."""
        return self._open

    @property
    def utilization(self) -> int:
        """Returns utilization on connection pool in percent."""
        if not self._open:
            raise RuntimeError("Pool is closed.")
        return (
            sum(1 if c.claimed else 0 for c in self._pool + self._overflow)
            / max(1, self._pool_size)
            * 100
        )

    def _renew_connection(self, c: Connection) -> Connection:
        """Replace connection in pool with new one."""
        with self._pool_lock:
            c.close()
            # if unable to make new connection or is immediately broken,
            # _init_connection raises an error
            new_c = self._init_connection()
            self._pool.remove(c)
            self._pool.append(new_c)
            return new_c

    def get_claim(
        self, block: bool = True, timeout: Optional[float] = None
    ) -> Optional[Claim]:
        """
        Returns `Claim` or `None` if block is `False` or a `claim` times
        out while overflow is disabled.

        Keyword arguments:
        block -- whether to block until claim can be made (only relevant
                 for disabled overflow)
                 (default True)
        timeout -- maximum wait time for a claim in seconds; if `None`,
                   waits indefinitely (only relevant for disabled
                   overflow and if `block is True`)
                   (default None)
        """
        if not self._open:
            raise RuntimeError("Pool is closed.")

        with self._pool_lock:
            for c in self._pool.copy():
                # fix broken connection when encountered
                if not c.healthy[0]:
                    # this connection is guaranteed to be unclaimed
                    return self._renew_connection(c).claim()

                # use first unclaimed connection found in pool
                claim = c.claim()
                if claim:
                    return claim

        if self._allow_overflow:
            self._cleanup_overflow()
            with self._overflow_lock:
                self._overflow.append(self._init_connection())
                return self._overflow[-1].claim()

        if block:
            return self._pool[randint(0, self._pool_size - 1)].claim(
                block, timeout
            )

        return None

    def close(self) -> None:
        """
        Closes down all open connections.
        """
        if not self._open:
            raise RuntimeError("Pool is closed.")

        with self._overflow_lock:
            for c in self._pool + self._overflow:
                c.close()

            self._pool = None
            self._overflow = []
            self._open = False
