"""
Test module for the class `Job` in the `orchestration`-package.
"""

from pathlib import Path
from time import sleep, time
import threading

import pytest

from dcm_common import LoggingContext as Context
from dcm_common import orchestration


def test_constructor():
    """Test constructor for `Job`-objects."""

    job = orchestration.Job()

    assert hasattr(job, "name")
    assert hasattr(job, "data")
    assert hasattr(job, "log")
    assert hasattr(job, "completed")
    assert hasattr(job, "running")
    assert hasattr(job, "exit_code")
    assert hasattr(job, "run")
    assert callable(job.run)


def test_name():
    """Test constructor for `Job`-objects with `name`-argument."""

    job = orchestration.Job(
        name="Service1"
    )
    assert job.log.default_origin == "Service1"


def test_run_twice():
    """Test method `run` for `Job`-objects error behavior."""

    job1 = orchestration.Job(
        cmd=lambda data, push: None
    )
    job1.run()

    assert job1.completed

    with pytest.raises(RuntimeError):
        job1.run()


def test_run_twice_with_reset():
    """Test method `reset` for `Job`-objects for multiple runs."""

    def cmd(data, push):
        data["bool"] = not data["bool"]
        push()

    job1 = orchestration.Job(
        cmd=cmd,
        data={"bool": True}
    )

    # baseline
    job1.run()

    assert job1.completed
    assert not job1.data["bool"]

    # reset and re-use data
    job1.reset()
    assert not job1.running
    assert not job1.aborted
    assert not job1.completed
    assert len(job1.log) == 0
    job1.run()

    assert job1.completed
    assert job1.data["bool"]

    # reset and change data
    job1.reset(data={"bool": not job1.data["bool"]})
    job1.run()

    assert job1.completed
    assert job1.data["bool"]


@pytest.mark.parametrize(
    ("cwd", "exists"),
    [
        (None, False),
        (Path("test_dcm_common/test_orchestration"), True),
        (Path("test_dcm_common/test_orchestration").absolute(), True),
    ],
    ids=["no_change", "relative", "absolute"]
)
def test_run_cwd(cwd, exists):
    """Test method `run` for `Job`-objects with `cwd`-argument."""

    def cmd(data, push):
        data["exists"] = data["path"].exists()
        push()

    job1 = orchestration.Job(
        cmd=cmd,
        data={
            "path": Path("test_job.py")
        }
    )

    job1.run(cwd=cwd)
    assert job1.data["exists"] == exists


def test_run_cmd_with_children():
    """Test method `run` for `Job`-cmd with `children`-argument."""

    def cmd(data, push, children):
        children.add(orchestration.ChildJob("url", "token"), "tag")
        push()

    job1 = orchestration.Job(cmd=cmd)

    job1.run()
    assert job1.children["tag"].token == "token"


def test_run_cmd_abort():
    """Test method `run` + `abort` for `Job`-cmd."""

    def cmd(data, push):
        data["key"] = 0
        push()
        sleep(5)

    job = orchestration.Job(cmd=cmd, data={})

    threading.Thread(target=job.run, daemon=True).start()
    time0 = time()
    while "key" not in job.data and time() - time0 < 2:
        sleep(0.01)
    assert job.running

    job.abort("test abort", "pytest-runner")
    assert not job.running
    assert len(job.log[Context.ERROR]) == 1


def test_run_cmd_abort_with_children():
    """Test method `run` + `abort` for `Job`-cmd with `children`-argument."""

    def cmd(data, push, children):
        children.add(orchestration.ChildJob("url", "token"), "tag")
        push()
        sleep(5)

    job1 = orchestration.Job(cmd=cmd)

    threading.Thread(target=job1.run, daemon=True).start()
    time0 = time()
    while len(job1.children) == 0 and time() - time0 < 2:
        sleep(0.01)
    assert job1.running

    job1.abort("test abort", "pytest-runner")
    assert not job1.running
    assert len(job1.log[Context.ERROR]) == 2
    assert len(job1.log[Context.INFO]) == 1
    assert "Error while aborting child" in str(job1.log[Context.ERROR])
    assert "Aborting child" in str(job1.log[Context.INFO])


def test_run_cmd_with_lock():
    """Test method `run` for `Job`-cmd with `lock`-argument."""

    def cmd(data, push, lock):
        # check RLock-property
        data["result"] = 1
        with lock:
            push()

        # check locking of threads
        def subcmd():
            data["result"] += 1
            push()
        t = threading.Thread(target=subcmd)
        with lock:
            t.start()
            sleep(0.1)
            assert data["result"] == 2
            assert t.is_alive()
        sleep(0.1)
        assert not t.is_alive()

    job1 = orchestration.Job(cmd=cmd)

    job1.run()
