"""Test module for pooling-classes."""

from time import sleep, time
from threading import Thread

import pytest

from dcm_common.db import Connection, Claim, ConnectionPool


class ExampleConnection(Connection):
    """Minimal implementation that simply echos commands."""

    def _connect(self) -> None:
        pass

    def _close(self) -> None:
        pass

    def _create_claim(self) -> None:
        return Claim(self, [])

    def _destroy_claim(self, claim) -> None:
        pass

    def _execute(self, claim, cmd: str) -> None:
        claim.cursor.append(cmd)

    def _fetch(self, claim):
        return claim.cursor

    @property
    def healthy(self) -> tuple[bool, str]:
        return self.connected, ""


def test_connection_simple():
    """Test simple use-case of `Connection`-class."""

    # setup connection
    c = ExampleConnection(False)
    assert not c.healthy[0]
    c.connect()
    assert c.healthy[0]
    assert not c.claimed

    # make claim
    claim = c.claim()
    assert claim is not None
    assert c.claimed

    # use claim
    assert c.execute(claim, "test") == ["test"]

    # try to claim again
    assert c.claim() is None

    # release claim
    c.release(claim)
    assert not c.claimed

    # close connection
    c.close()
    assert not c.healthy[0]

    with pytest.raises(ConnectionError):
        c.claim()


def test_connection_simple_context_manager():
    """Test simple use-case of `Connection`-class in context-manager."""

    # setup connection
    c = ExampleConnection(False)
    assert not c.healthy[0]
    c.connect()
    assert c.healthy[0]

    # make claim
    with c.claim() as claim:
        assert c.claimed

        # use claim
        assert claim.execute("test") == ["test"]

        # try to claim again
        assert c.claim() is None

    assert not c.claimed

    # close connection
    c.close()
    assert not c.healthy[0]


def test_connection_errors():
    """Test error-behavior of `Connection`-class."""

    c = ExampleConnection(False)
    # before connecting
    with pytest.raises(ConnectionError):
        c.claim()
    # before claim
    with pytest.raises(ConnectionError):
        c.execute(Claim(c, None), "command")
    c.connect()

    # release unclaimed
    with pytest.raises(ConnectionError):
        c.release(Claim(c, None))

    c.close()


def test_connection_pool_simple():
    """Test simple use-case of `ConnectionPool`-class."""

    p = ConnectionPool(ExampleConnection, pool_size=1, allow_overflow=False)
    assert p.utilization == 0.0

    # make claim
    with p.get_claim() as claim:
        # check load on pool
        assert p.utilization == 100.0
        assert p.get_claim(False) is None

        # use claim
        assert claim.execute("test") == ["test"]

    assert p.utilization == 0.0
    p.close()


def test_connection_pool_allow_overflow():
    """
    Test `ConnectionPool`-class behavior with and without overflow.
    """
    p = ConnectionPool(ExampleConnection, pool_size=0, allow_overflow=False)
    assert p.get_claim(False) is None
    p.close()

    p = ConnectionPool(ExampleConnection, pool_size=1, allow_overflow=True)
    assert p.get_claim(False) is not None
    p.close()


def test_connection_pool_concurrent_use():
    """
    Test concurrency-support for `ConnectionPool`-class.
    """
    p = ConnectionPool(ExampleConnection, pool_size=2)

    with p.get_claim() as claim0, p.get_claim() as claim1:
        assert claim0.execute("test0") == ["test0"]
        assert claim1.execute("test1") == ["test1"]

    p.close()


def test_connection_pool_concurrent_use_threads():
    """
    Test concurrency-support for `ConnectionPool`-class.

    implement this by running two threads that run multiple tasks in
    parallel using different claims.
    """

    class ThisExampleConnection(ExampleConnection):
        def _execute(self, claim, cmd) -> None:
            r = cmd()
            if r:
                claim.cursor.append(r)

    p = ConnectionPool(ThisExampleConnection, pool_size=2)

    # measure iteration
    time0 = time()
    with p.get_claim() as claim:
        claim.execute(lambda: sleep(0.01))
        claim.execute(lambda: "test")
    base_duration = time() - time0

    n = 100

    result1 = []

    def task1():
        for i in range(n):
            with p.get_claim() as claim:
                claim.execute(lambda: sleep(0.01))
                result1.extend(claim.execute(lambda i=i: f"task1.{i}"))

    result2 = []

    def task2():
        for i in range(n):
            with p.get_claim() as claim:
                claim.execute(lambda: sleep(0.01))
                result2.extend(claim.execute(lambda i=i: f"task2.{i}"))

    t1 = Thread(target=task1)
    t2 = Thread(target=task2)

    t1.start()
    t2.start()

    time0 = time()
    while (time() - time0) < 2 * base_duration * n and (
        t1.is_alive() or t2.is_alive()
    ):
        sleep(0.01)

    assert (time() - time0) < 2 * base_duration * n, "timeout, try again"

    # check results
    assert sorted([f"task1.{i}" for i in range(n)]) == sorted(result1)
    assert sorted([f"task2.{i}" for i in range(n)]) == sorted(result2)

    p.close()


def test_connection_pool_reconnect():
    """Test automatic reconnection in ConnectionPool."""

    p = ConnectionPool(ExampleConnection, pool_size=1, allow_overflow=False)
    p.get_claim().connection.close()
    assert p.get_claim() is not None
    p.close()


def test_connection_pool_never_healthy():
    """
    Test error behavior of ConnectionPool when connections are
    immediately broken.
    """

    class ThisExampleConnection(ExampleConnection):
        @property
        def healthy(self):
            return False, "test-message"

    with pytest.raises(ConnectionError) as exc_info:
        ConnectionPool(ThisExampleConnection, pool_size=1)

    assert "test-message" in str(exc_info.value)


def test_connection_pool_unhealthy_after_init_during_claim():
    """
    Test error behavior of ConnectionPool when connections are
    immediately broken during reconnection while claiming.
    """

    class ThisExampleConnection(ExampleConnection):
        connection_status = {"ok": True}

        @property
        def healthy(self):
            return self.connection_status["ok"], ""

    p = ConnectionPool(ThisExampleConnection, pool_size=1)
    p.get_claim().connection.close()
    ThisExampleConnection.connection_status["ok"] = False

    with pytest.raises(ConnectionError):
        p.get_claim()

    p.close()


def test_pool_not_open_yet():
    """Test error-behavior of `ConnectionPool` if `connect_now=False`."""

    p = ConnectionPool(ExampleConnection, connect_now=False)

    assert not p.is_open

    with pytest.raises(RuntimeError):
        _ = p.utilization

    with pytest.raises(RuntimeError):
        p.get_claim()

    with pytest.raises(RuntimeError):
        p.close()

    p.init_pool()
    assert p.is_open
    _ = p.utilization
    p.get_claim().release()

    with pytest.raises(RuntimeError):
        p.init_pool()

    p.close()

    assert not p.is_open
