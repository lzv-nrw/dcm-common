"""
Test module for the orchestration-controls API (flask-blueprint).
"""

from time import time, sleep

import pytest
from flask import Flask

from dcm_common import FDaemon
from dcm_common.orchestration import (
    Job, JobConfig, ScalableOrchestrator, orchestrator_controls_bp
)


@pytest.fixture(name="interval")
def _interval():
    return 0.01


@pytest.fixture(name="orchestrator")
def _orchestrator(interval):
    return ScalableOrchestrator(
        lambda config: Job(
            cmd=lambda data, push:
                sleep(config.original_body.get("sleep_duration", interval))
        ),
        _debug=True,
        #exec_hooks={  # useful for debugging
        #    "completion": lambda info, job: print(job.log)
        #}
    )


@pytest.fixture(name="client_wo_daemon")
def _client_wo_daemon(orchestrator):
    app = Flask(__name__)
    app.register_blueprint(orchestrator_controls_bp(orchestrator))
    return app.test_client(), orchestrator


@pytest.fixture(name="client_w_daemon")
def _client_w_daemon(interval, orchestrator):
    app = Flask(__name__)
    daemon = FDaemon(
        orchestrator.as_thread, kwargs={"interval": interval}
    )
    app.register_blueprint(
        orchestrator_controls_bp(orchestrator, daemon)
    )
    return app.test_client(), orchestrator, daemon


def test_stop_running_orchestrator(interval, client_wo_daemon):
    """
    Test `Orchestrator`-controls by stopping an already running
    orchestrator.
    """
    client, orchestrator = client_wo_daemon
    orchestrator.run(interval)

    assert orchestrator.running
    client.delete("http://localhost:8080/orchestration", json={})
    sleep(2*interval)
    assert not orchestrator.running


def test_stop_running_daemon(interval, client_w_daemon):
    """
    Test `Orchestrator`-controls by stopping an already running daemon.
    """
    client, _, daemon = client_w_daemon
    daemon.run(interval, block=True)

    assert daemon.status
    client.delete("http://localhost:8080/orchestration", json={})
    assert not daemon.status


def test_start_stop(interval, client_wo_daemon):
    """
    Test `Orchestrator`-controls by starting then stopping an
    orchestrator.
    """
    client, orchestrator = client_wo_daemon

    client.put(
        "http://localhost:8080/orchestration",
        json={
            "orchestrator": {"interval": interval}
        }
    )
    assert orchestrator.running
    client.delete("http://localhost:8080/orchestration", json={})
    sleep(2*interval)
    assert not orchestrator.running


def test_start_stop_via_daemon(interval, client_w_daemon):
    """
    Test `Orchestrator`-controls by stopping an already running daemon.
    """
    client, _, daemon = client_w_daemon

    client.put(
        "http://localhost:8080/orchestration",
        json={"daemon": {"interval": interval}}
    )
    assert daemon.status
    client.delete("http://localhost:8080/orchestration", json={})
    assert not daemon.status


def test_submit(client_wo_daemon):
    """
    Test `Orchestrator`-controls by submitting a job.
    """
    client, orchestrator = client_wo_daemon

    response = client.post(
        "http://localhost:8080/orchestration", json=JobConfig().json
    )
    assert response.json["value"] in orchestrator.registry.keys()


def test_start_until_idle(interval, client_wo_daemon):
    """
    Test `Orchestrator`-controls by executing current queue only.
    """
    client, orchestrator = client_wo_daemon
    assert orchestrator.idle
    _ = orchestrator.submit(JobConfig({"sleep_duration": 2}))
    assert not orchestrator.idle
    _ = client.put(
        "http://localhost:8080/orchestration?until-idle=", json={}
    )
    assert orchestrator.running
    while not orchestrator.idle:
        sleep(interval)
    time0 = time()
    while time() < time0 + 1:  # wait for shutdown
        sleep(interval)
        if not orchestrator.running:
            return
    assert False, f"Orchestration did not shut down after {time()-time0}s."


def test_abort(interval, client_wo_daemon):
    """
    Test `Orchestrator`-controls by aborting a job.
    """
    client, orchestrator = client_wo_daemon
    token = orchestrator.submit(JobConfig({"sleep_duration": 10}))
    orchestrator.run(interval, daemon=True)
    sleep(2*interval)
    assert not orchestrator.idle
    assert len(orchestrator.jobs) == 1

    origin = "some-origin"
    client.delete(
        "http://localhost:8080/orchestration",
        json={
            "mode": "abort",
            "options": {
                "block": True,
                "token": token.value,
                "reason": "some-reason",
                "origin": origin
            }
        }
    )
    orchestrator.stop_on_idle(True)
    info = orchestrator.registry.read(token.value)
    assert "aborted" in info["metadata"]
    assert info["metadata"]["aborted"]["by"] == origin


@pytest.mark.parametrize(
    "re_queue", [True, False], ids=["re-queue", "no-re-queue"]
)
def test_kill(interval, client_wo_daemon, re_queue):
    """
    Test `Orchestrator`-controls by killing the orchestrator.
    """
    client, orchestrator = client_wo_daemon
    token = orchestrator.submit(JobConfig({"sleep_duration": 10}))
    orchestrator.run(interval, daemon=True)
    sleep(2*interval)
    assert not orchestrator.idle
    assert len(orchestrator.jobs) == 1

    origin = "some-origin"
    client.delete(
        "http://localhost:8080/orchestration",
        json={
            "mode": "kill",
            "options": {
                "block": True,
                "reason": "some-reason",
                "origin": origin,
                "re_queue": re_queue,
            }
        }
    )
    sleep(2*interval)
    if re_queue:
        assert len(orchestrator.queue.keys()) == 1
        assert token.value in orchestrator.queue.keys()
        info = orchestrator.registry.read(token.value)
        assert "aborted" not in info["metadata"]
    else:
        info = orchestrator.registry.read(token.value)
        assert "aborted" in info["metadata"]
        assert info["metadata"]["aborted"]["by"] == origin
