"""Test module for the `daemon.py`-module."""

from time import time, sleep
from threading import Thread, Event
from unittest.mock import patch

import pytest

from dcm_common import CDaemon, FDaemon


def test_cdaemon_constructor():
    """Test constructor of class `CDaemon`."""
    CDaemon(target=lambda: None)


def test_cdaemon_constructor_error():
    """Test constructor of class `CDaemon`."""
    with pytest.raises(KeyError):
        CDaemon()


def test_cdaemon_run_minimal():
    """
    Test method `run` of class `CDaemon` for minimal setup.
    Includes basic test for
    * constructor
    * run
    * active/status
    * stop
    """
    result = {"data": 0}
    interval = 0.01

    def _service():
        result["data"] += 1
        while True:
            sleep(interval)

    d = CDaemon(target=_service, daemon=True)
    assert not d.active
    assert not d.status

    d.run(interval, False, True)
    assert d.active
    assert d.status
    sleep(2 * interval)
    assert result["data"] == 1
    d.stop(True)

    assert not d.active
    assert not d.status


def test_cdaemon_run_restart():
    """Test method `run` of class `CDaemon` for stopped service."""
    result = {"data": 0}
    interval = 0.01
    stop = Event()
    stopped = Event()

    def _service():
        result["data"] += 1
        stop.clear()
        while not stop.is_set():
            sleep(interval)
        stopped.set()

    d = CDaemon(target=_service, daemon=True)
    d.run(interval, False, True)
    sleep(2 * interval)
    assert result["data"] == 1
    stop.set()
    while not stopped.is_set():
        pass
    sleep(2 * interval)
    assert result["data"] == 2
    d.stop(True)


def test_cdaemon_run_reconfigure():
    """Test methods `run` and `reconfigure` of class `CDaemon`."""
    result = {"data": 0}
    interval = 0.01
    stop = Event()
    stopped = Event()

    def _service(increment):
        result["data"] += increment
        stop.clear()
        while not stop.is_set():
            sleep(interval)
        stopped.set()

    d = CDaemon(target=_service, daemon=True, kwargs={"increment": 1})
    d.run(interval, False, True)
    sleep(2 * interval)
    assert result["data"] == 1
    d.reconfigure(kwargs={"increment": 2})
    stop.set()
    while not stopped.is_set():
        pass
    sleep(2 * interval)
    assert result["data"] == 3
    d.stop(True)


def test_cdaemon_run_unrecoverable_error():
    """
    Test behavior of `run`-loop of class `CDaemon` if unrecoverable
    error occurs.
    """

    def _broken_restart_service():
        raise RuntimeError("Some error occurred.")

    interval = 0.01
    d = CDaemon(target=lambda: sleep(1), daemon=True)
    with patch(
        "dcm_common.daemon.CDaemon._restart_service",
        side_effect=_broken_restart_service,
    ):
        d.run(interval, False, False)
    sleep(interval)
    assert not d.active


def test_cdaemon_stop_quick():
    """
    Test method `stop` of class `CDaemon` for long interval.
    """

    def _service():
        while True:
            sleep(0.01)

    d = CDaemon(target=_service, daemon=True)
    d.run(100, False, True)
    time0 = time()
    d.stop(True)
    assert time() - time0 < 10


def test_fdaemon_minimal():
    """
    Test method `run` of class `FDaemon` for minimal setup.
    Includes basic test for
    * constructor
    * run
    * stop
    """
    result = {"data": 0}
    interval = 0.01

    def _factory():
        def _service():
            result["data"] += 1
            while True:
                sleep(interval)

        return Thread(target=_service, daemon=True)

    d = FDaemon(_factory)
    d.run(interval, block=True)
    sleep(2 * interval)
    assert result["data"] == 1
    d.stop(True)


def test_fdaemon_reconfigure():
    """Test methods `run` and `reconfigure` of class `CDaemon`."""
    result = {"data": 0}
    interval = 0.01
    stop = Event()
    stopped = Event()

    def _factory(increment):
        def _service():
            stop.clear()
            result["data"] += increment
            while not stop.is_set():
                sleep(interval)
            stopped.set()

        return Thread(target=_service, daemon=True)

    d = FDaemon(_factory, kwargs={"increment": 1})
    d.run(interval, block=True)
    sleep(2 * interval)
    assert result["data"] == 1

    d.reconfigure(increment=2)
    stop.set()
    while not stopped.is_set():
        pass
    sleep(2 * interval)
    assert result["data"] == 3
    d.stop(True)


@pytest.mark.parametrize("block", [True, False], ids=["block", "no-block"])
def test_fdaemon_run_block(block):
    """
    Test argument `block` for method `run` of class `FDaemon`.

    Use `FDaemon` instead of `CDaemon` since factory can artificially
    delay execution which makes the test predictable.
    """
    interval = 0.01

    def _factory():
        sleep(2 * interval)

        def _service():
            sleep(10 * interval)

        return Thread(target=_service, daemon=True)

    d = FDaemon(_factory)
    d.run(interval, False, block)
    assert d.status is block
    d.stop(True)
