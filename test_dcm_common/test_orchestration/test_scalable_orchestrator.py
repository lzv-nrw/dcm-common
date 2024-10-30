"""
Test module for the class `Orchestrator` in the `orchestration`-package.
"""

from time import sleep, time
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from dcm_common import LoggingContext as Context
from dcm_common.util import now
from dcm_common.models import Token, Report
from dcm_common.models.data_model import get_model_serialization_test
from dcm_common.db import MemoryStore, NativeKeyValueStoreAdapter
from dcm_common.orchestration import (
    Job, JobConfig, JobInfo, ScalableOrchestrator
)
from dcm_common.orchestration.scalable_orchestrator import (
    MetadataRecord, JobMetadata
)


@pytest.fixture(name="queue")
def _queue():
    return NativeKeyValueStoreAdapter(MemoryStore())


@pytest.fixture(name="registry")
def _registry():
    return NativeKeyValueStoreAdapter(MemoryStore())


@pytest.fixture(name="debug")
def _debug():
    return True


def simple_orchstrator(id_, queue, registry, nprocesses=1, cmd=None):
    """Returns a simple `ScalableOrchestrator`."""
    return ScalableOrchestrator(
        lambda config: Job(
            name="Test",
            cmd=cmd or (lambda data, push: data.update({"result": id_})),
        ),
        queue, registry, nprocesses, queue_hooks={
            "pre-register": lambda info: setattr(info, "report", {})
        },
        _debug=True
    )


def test_config_constructor_wo_request():
    """
    Test constructor of class `JobConfig` with default values (outside
    of request).
    """
    assert JobConfig().original_body is None


def test_config_constructor_w_request():
    """
    Test constructor of class `JobConfig` with default values (outside
    of request).
    """

    class FakeRequest:
        """`flask.request`-stub"""
        json = {"key": "value"}
    with patch(
        "dcm_common.orchestration.scalable_orchestrator.request",
        FakeRequest
    ):
        assert JobConfig().original_body == FakeRequest.json


test_config_json = get_model_serialization_test(
    JobConfig, (
        ((), {}),
        (("a", "b", "c"), {}),
    )
)


test_info_json = get_model_serialization_test(
    JobInfo, (
        ((JobConfig("a"), Token()), {}),
        (
            (JobConfig("a"), Token()), {
                "metadata": JobMetadata(
                    produced=MetadataRecord("test"), consumed=MetadataRecord()
                )
            }
        ),
        ((JobConfig("a"), Token()), {"report": {}}),
    )
)


def test_minimal(queue, registry):
    """
    Test general functionality of class `ScalableOrchestrator` in
    minimal setup. This includes methods:
    * submit
    * run
    * stop
    * get_report
    * idle/running
    """
    expected_result = 1
    orchestrator = simple_orchstrator(expected_result, queue, registry)
    token = orchestrator.submit(JobConfig({}))
    assert len(registry.keys()) == 1
    assert len(queue.keys()) == 1
    assert queue.keys() == (token.value,)
    assert not orchestrator.idle
    assert not orchestrator.running

    orchestrator.run(interval=0.001, daemon=True)
    sleep(0.001)
    assert not orchestrator.idle
    assert orchestrator.running
    orchestrator.stop_on_idle(True)
    assert orchestrator.idle
    while orchestrator.running:  # allow thread to shut down
        sleep(0.001)
    assert not orchestrator.running
    assert len(registry.keys()) == 1
    assert len(queue.keys()) == 0
    assert token.value in registry.keys()

    result = registry.read(token.value)
    assert result["report"] == {"result": expected_result}
    report = orchestrator.get_report(token)
    assert report == {"result": expected_result}
    assert orchestrator.get_report(token.value) == {"result": expected_result}

    info = JobInfo.from_json(orchestrator.get_info(token))
    assert info.report == report
    assert info.metadata.produced is not None
    assert info.metadata.consumed is not None
    assert info.metadata.completed is not None
    assert info.metadata.aborted is None


def test_multiple(queue, registry):
    """Test running multiple instances of `ScalableOrchestrator`."""
    id0 = "0"
    id1 = "1"
    n = 10
    orchestrator0 = simple_orchstrator(id0, queue, registry)
    orchestrator1 = simple_orchstrator(id1, queue, registry)
    tokens = []
    for _ in range(n):
        tokens.append(orchestrator0.submit(JobConfig({})))
        tokens.append(orchestrator1.submit(JobConfig({})))

    orchestrator0.run(interval=0.001, daemon=True)
    orchestrator1.run(interval=0.001, daemon=True)
    orchestrator0.stop_on_idle()
    orchestrator1.stop_on_idle()
    while orchestrator0.running or orchestrator1.running:
        sleep(0.001)

    assert len(tokens) == 2 * n
    assert all(token.value in registry.keys() for token in tokens)
    reports = [registry.read(token.value)["report"] for token in tokens]
    assert len([report for report in reports if report["result"] == id0]) > 0
    assert len([report for report in reports if report["result"] == id1]) > 0


def test_sparse_jobs(queue, registry):
    """
    Test occasional job submissions while already running
    `ScalableOrchestrator`.
    """
    expected_result = 1
    n = 5
    orchestrator = simple_orchstrator(expected_result, queue, registry)
    orchestrator.run(interval=0.001, daemon=True)
    tokens = []
    for _ in range(n):
        tokens.append(orchestrator.submit(JobConfig({})))
        tokens.append(orchestrator.submit(JobConfig({})))
        orchestrator.block_until_idle()
    orchestrator.stop()
    while orchestrator.running:
        sleep(0.001)
    assert all(token.value in registry.keys() for token in tokens)


def test_loading_error(queue, registry):
    """
    Test behavior of class `ScalableOrchestrator` in case registry does
    not return required data.
    """
    orchestrator = simple_orchstrator(0, queue, registry)
    token = orchestrator.submit(JobConfig({}))
    registry.delete(token.value)

    orchestrator.run(interval=0.001, daemon=True)
    sleep(0.002)
    orchestrator.stop_on_idle(True)

    report = registry.read(token.value)["report"]
    assert "log" in report
    assert Context.ERROR.name in report["log"]
    assert "Unable to load job config from registry." in str(report["log"])


@pytest.mark.parametrize(
    "use_token",
    [True, False],
    ids=["with-token", "without-token"]
)
def test_abort(use_token, queue, registry):
    """Test method `abort` of class `ScalableOrchestrator`."""
    duration = 0.1
    orchestrator = simple_orchstrator(
        0, queue, registry, cmd=lambda data, push: sleep(10)
    )
    token = orchestrator.submit(JobConfig({}))
    orchestrator.run(interval=0.001, daemon=True)
    sleep(0.25*duration)
    assert not orchestrator.ready
    if use_token:
        orchestrator.abort(token=token.value)
    else:
        orchestrator.abort()
    sleep(0.25*duration)
    assert orchestrator.ready
    assert orchestrator.running
    orchestrator.stop(True)
    record = registry.read(token.value)
    assert "aborted" in record["metadata"]


def test_abort_from_queue(queue, registry):
    """
    Test method `abort` of class `ScalableOrchestrator` while job is
    still in queue.
    """
    orchestrator = simple_orchstrator(
        0, queue, registry, nprocesses=0
    )
    token = orchestrator.submit(JobConfig({}))
    assert len(queue.keys()) == 1
    orchestrator.dequeue(token, "origin", "test abort")
    assert len(queue.keys()) == 0
    info = registry.read(token.value)
    assert info["report"]["progress"]["status"] == "aborted"
    assert Context.ERROR.name in info["report"]["log"]
    assert "aborted" in info["metadata"]


def test_abort_requeue(queue, registry):
    """
    Test method `abort` of class `ScalableOrchestrator` with re-queue
    option.
    """
    orchestrator = simple_orchstrator(
        0, queue, registry,
        cmd=lambda data, push: [data.update({"data": 0}), push(), sleep(10)]
    )
    # post job and let it start up
    token = orchestrator.submit(JobConfig({}))
    orchestrator.run(interval=0.001, daemon=True)
    time0 = time()
    while (
        "data" not in registry.read(token.value)["report"]
        and time() - time0 < 2
    ):
        sleep(0.001)
    assert registry.read(token.value)["report"] == {"data": 0}

    # tell orchestrator to stop (i.e. not to fetch from queue) then abort
    orchestrator.stop()
    orchestrator.abort(token, "origin", "test abort", re_queue=True)

    # wait for abort to process
    time0 = time()
    while orchestrator.running and time() - time0 < 2:
        sleep(0.001)
    assert not orchestrator.running

    # eval (reset for report)
    assert len(queue.keys()) == 1
    token_, token__ = queue.next()
    assert token.value == token_ == token__
    info = registry.read(token.value)
    assert list(info["metadata"].keys()) == ["produced"]
    assert info["report"] == {}


def test_hooks_link_job_log_and_abort_with_reason(queue, registry, debug):
    """
    Test method `abort` of class `ScalableOrchestrator` with explicit
    reason along with using hooks to link `job.log` to `data`.
    """
    duration = 0.1
    orchestrator = ScalableOrchestrator(
        lambda config: Job(
            name="Test-Service",
            cmd=lambda data, push: sleep(10),
        ),
        queue, registry, 1,
        exec_hooks={"pre-execution": lambda info, job: setattr(
                info, "report", Report(
                    host="",
                    token=info.token,
                    args=info.config.request_body,
                    log=job.log
                )
            ),
        },
        _debug=debug
    )
    token = orchestrator.submit(JobConfig({}))
    orchestrator.run(interval=0.001, daemon=True)
    sleep(0.25*duration)
    reason = "Service shutdown."
    orchestrator.kill(reason=reason)
    while orchestrator.running:
        sleep(0.1*duration)
    record = registry.read(token.value)
    assert Context.ERROR.name in record["report"]["log"]
    assert reason in str(record["report"]["log"])


def test_kill(queue, registry):
    """Test method `kill` of class `ScalableOrchestrator`."""
    duration = 0.1
    orchestrator = simple_orchstrator(
        0, queue, registry, cmd=lambda data, push: sleep(10)
    )
    token = orchestrator.submit(JobConfig({}))
    orchestrator.run(interval=0.001, daemon=True)
    sleep(0.25*duration)
    assert not orchestrator.ready
    orchestrator.kill()
    while orchestrator.running:
        sleep(0.1*duration)
    assert orchestrator.ready
    assert not orchestrator.running
    record = registry.read(token.value)
    assert "aborted" in record["metadata"]


def test_get_report(queue, registry):
    """Test method `get_report` of class `ScalableOrchestrator`."""
    orchestrator = simple_orchstrator(0, queue, registry)
    assert orchestrator.get_report(Token()) is None


@pytest.mark.parametrize(
    "nprocesses",
    [1, 2],
    ids=["single-job", "two-jobs"]
)
def test_vertical_scaling(nprocesses, queue, registry, debug):
    """
    Test vertical_scaling of class `ScalableOrchestrator`.

    Do this by configuring orchestrator to write times of start and end
    per job. Then submit multiple and start execution. Finally, check
    overlapping times.
    """
    duration = 0.1
    orchestrator = ScalableOrchestrator(
        lambda config: Job(
            name="Test-Service",
            cmd=lambda data, push: sleep(10*duration),
        ),
        queue, registry, nprocesses,
        exec_hooks={
            "pre-execution": lambda info, job:
                setattr(info, "report", {"time0": now(True).isoformat()}),
            "completion": lambda info, job:
                info.report.update({"time1": now(True).isoformat()}),
        },
        _debug=debug
    )
    token0 = orchestrator.submit(JobConfig({}))
    token1 = orchestrator.submit(JobConfig({}))

    orchestrator.run(0.001, daemon=True)
    orchestrator.stop_on_idle(True)

    report0 = orchestrator.get_report(token0)
    report1 = orchestrator.get_report(token1)
    if nprocesses == 2:
        assert (
            datetime.fromisoformat(report0["time0"])
            < datetime.fromisoformat(report1["time1"])
        )
        assert (
            datetime.fromisoformat(report1["time0"])
            < datetime.fromisoformat(report0["time1"])
        )
    else:
        assert (
            datetime.fromisoformat(report0["time1"])
            < datetime.fromisoformat(report1["time0"])
        ) or (
            datetime.fromisoformat(report1["time1"])
            < datetime.fromisoformat(report0["time0"])
        )


@pytest.mark.parametrize(
    "cwd",
    [None, Path("test_dcm_common")],
    ids=["unchanged", "changed-cwd"]
)
def test_run_cwd(cwd, queue, registry):
    """
    Test method `run` of class `ScalableOrchestrator` with different
    values for `cwd`.
    """
    orchestrator = simple_orchstrator(
        0, queue, registry,
        cmd=lambda data, push:
            data.update({"result": Path("conftest.py").is_file()})
    )
    token = orchestrator.submit(JobConfig({}))
    orchestrator.run(interval=0.001, cwd=cwd, daemon=True)
    orchestrator.stop_on_idle(True)
    while orchestrator.running:
        sleep(0.001)
    assert (
        orchestrator.get_report(token)["result"]
        is ((cwd or Path(".")) / "conftest.py").is_file()
    )


def test_error_multiple_instances(queue, registry):
    """
    Test error-behavior when attempting to run method
    `ScalableOrchestrator.run` or `ScalableOrchestrator.as_thread`
    multiple times.
    """
    orchestrator = simple_orchstrator(0, queue, registry)
    _ = orchestrator.as_thread()
    _ = orchestrator.as_thread()
    orchestrator.run(interval=0.001)
    with pytest.raises(RuntimeError):
        _ = orchestrator.as_thread()
    orchestrator.stop()
    while orchestrator.running:
        sleep(0.001)
    thread = orchestrator.as_thread(interval=0.001, daemon=True)
    thread.start()
    with pytest.raises(RuntimeError):
        orchestrator.run()
    orchestrator.stop(True)


def test__mv_factories():
    """
    Test constructor of `ScalableOrchestrator` with `_mv_factories`.
    """
    orchestrator = ScalableOrchestrator()
    assert len(orchestrator.factories) == 0
    orchestrator = ScalableOrchestrator(
        factory=lambda config: Job(cmd=lambda data, push: None)
    )
    assert len(orchestrator.factories) == 1
    orchestrator = ScalableOrchestrator(
        _mv_factories={
            "a": lambda config: Job(cmd=lambda data, push: None),
            "b": lambda config: Job(cmd=lambda data, push: None),
        }
    )
    assert len(orchestrator.factories) == 2
    with pytest.raises(ValueError):
        orchestrator = ScalableOrchestrator(
            factory=lambda config: Job(cmd=lambda data, push: None),
            _mv_factories={
                "a": lambda config: Job(cmd=lambda data, push: None),
                "b": lambda config: Job(cmd=lambda data, push: None),
            }
        )


def test_multi_factory(queue, registry, debug):
    """
    Test configuration of `ScalableOrchestrator` for use with multiple
    factories.
    """
    orchestrator = ScalableOrchestrator(
        queue=queue, registry=registry,
        _mv_factories={
            "a": lambda config: Job(
                cmd=lambda data, push: data.update({"result": "a"})
            ),
            "b": lambda config: Job(
                cmd=lambda data, push: data.update({"result": "b"})
            ),
        },
        _mv_queue_hooks={
            "a": {"pre-register": lambda info: setattr(info, "report", {})},
            "b": {"pre-register": lambda info: setattr(info, "report", {})},
        },
        _debug=debug
    )

    token_a = orchestrator.submit(JobConfig(context="a"))
    token_b = orchestrator.submit(JobConfig(context="b"))

    orchestrator.run(interval=0.001, daemon=True)
    orchestrator.stop_on_idle()
    while orchestrator.running:
        sleep(0.001)

    assert orchestrator.get_report(token_a)["result"] == "a"
    assert orchestrator.get_report(token_b)["result"] == "b"


def test_multi_factory_unknown_context(queue, registry, debug):
    """
    Test configuration of `ScalableOrchestrator` for use with multiple
    factories but unknown context.
    """
    orchestrator = ScalableOrchestrator(
        queue=queue, registry=registry,
        factory=lambda config: Job(cmd=lambda data, push: None),
        _debug=debug
    )

    token = orchestrator.submit(JobConfig(context="a"))

    orchestrator.run(interval=0.001, daemon=True)
    orchestrator.stop_on_idle()
    while orchestrator.running:
        sleep(0.001)

    assert "Unknown job-context" in str(orchestrator.get_report(token))


def test__mv_queue_hooks():
    """
    Test constructor of `ScalableOrchestrator` with `_mv_queue_hooks`.
    """
    orchestrator = ScalableOrchestrator()
    assert len(orchestrator.queue_hooks) == 0
    orchestrator = ScalableOrchestrator(
        queue_hooks={"pre-register": lambda info: None}
    )
    assert len(orchestrator.queue_hooks) == 1
    orchestrator = ScalableOrchestrator(
        _mv_queue_hooks={
            "a": {"pre-register": lambda info: None},
            "b": {"pre-register": lambda info: None},
        }
    )
    assert len(orchestrator.queue_hooks) == 2
    with pytest.raises(ValueError):
        orchestrator = ScalableOrchestrator(
            queue_hooks={"pre-register": lambda info: None},
            _mv_queue_hooks={
                "a": {"pre-register": lambda info: None},
                "b": {"pre-register": lambda info: None},
            }
        )


def test__mv_exec_hooks():
    """
    Test constructor of `ScalableOrchestrator` with `_mv_exec_hooks`.
    """
    orchestrator = ScalableOrchestrator()
    assert len(orchestrator.exec_hooks) == 0
    orchestrator = ScalableOrchestrator(
        exec_hooks={"completion": lambda info: None}
    )
    assert len(orchestrator.exec_hooks) == 1
    orchestrator = ScalableOrchestrator(
        _mv_exec_hooks={
            "a": {"completion": lambda info: None},
            "b": {"completion": lambda info: None},
        }
    )
    assert len(orchestrator.exec_hooks) == 2
    with pytest.raises(ValueError):
        orchestrator = ScalableOrchestrator(
            exec_hooks={"completion": lambda info: None},
            _mv_exec_hooks={
                "a": {"completion": lambda info: None},
                "b": {"completion": lambda info: None},
            }
        )


def test_multi_hooks(queue, registry, debug):
    """
    Test configuration of `ScalableOrchestrator` for use with multiple
    factories and hooks.
    """
    orchestrator = ScalableOrchestrator(
        queue=queue, registry=registry,
        _mv_factories={
            "a": lambda config: Job(cmd=lambda data, push: None),
            "b": lambda config: Job(cmd=lambda data, push: None),
        },
        _mv_queue_hooks={
            "a": {"pre-register": lambda info: setattr(info, "report", {"pre-reg": "a"})},
            "b": {"pre-register": lambda info: setattr(info, "report", {"pre-reg": "b"})},
        },
        _mv_exec_hooks={
            "a": {"completion": lambda info, job: info.report.update({"completion": "a"})},
            "b": {"completion": lambda info, job: info.report.update({"completion": "b"})},
        },
        _debug=debug
    )

    token_a = orchestrator.submit(JobConfig(context="a"))
    token_b = orchestrator.submit(JobConfig(context="b"))

    orchestrator.run(interval=0.001, daemon=True)
    orchestrator.stop_on_idle()
    while orchestrator.running:
        sleep(0.001)

    report_a = orchestrator.get_report(token_a)
    report_b = orchestrator.get_report(token_b)
    assert report_a["pre-reg"] == "a"
    assert report_b["pre-reg"] == "b"
    assert report_a["completion"] == "a"
    assert report_b["completion"] == "b"
