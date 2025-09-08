"""Tests for the `HTTPController`-class."""

from datetime import datetime, timedelta
import threading
from time import sleep
import json

import pytest
import requests
from flask import Flask

from dcm_common.services.tests import run_service
from dcm_common.orchestra import (
    HTTPController,
    get_http_controller_bp,
    DilledProcess,
)
from dcm_common.orchestra.models import JobConfig, JobInfo
from dcm_common.services.config import OrchestratedAppConfig


def get_http_controller_app():
    """Returns flask app with http-controller-API."""
    config = OrchestratedAppConfig()
    app = Flask("test-http-controller")
    app.register_blueprint(get_http_controller_bp(config.controller))

    return app


def Info():  # pylint: disable=invalid-name
    """Minimal `JobInfo`."""
    return JobInfo(JobConfig("test", {}, {}))


def test_queue(run_service):
    """Test queue-related methods of `HTTPController`."""

    p = run_service(from_factory=get_http_controller_app, port=8080)

    c = HTTPController("http://localhost:8080")

    # basic submission
    token = c.queue_push("0", Info())
    assert token.value == "0"
    assert token.expires

    # resubmit same token
    token = c.queue_push("0", Info())
    assert token.value == "0"
    assert token.expires

    # get lock
    lock = c.queue_pop("test")
    assert lock.token == token.value

    # submit and lock a different token
    token2 = c.queue_push("1", Info())
    assert token2.value == "1"
    assert token2.expires
    lock2 = c.queue_pop("test")
    assert lock2.token == token2.value

    # pop with empty queue
    assert c.queue_pop("test") is None

    # stop API
    p.kill()
    assert c.queue_pop("test") is None
    with pytest.raises(requests.exceptions.RequestException):
        c.queue_push("2", Info())


def test_refresh_lock(run_service):
    """Test method `HTTPController.refresh_lock`."""

    run_service(from_factory=get_http_controller_app, port=8080)
    c = HTTPController("http://localhost:8080")

    c.queue_push("0", Info())

    # get lock
    lock0 = c.queue_pop("some-name")

    # wait
    sleep(1)

    # refresh and check
    lock1 = c.refresh_lock(lock0.id)
    assert lock1.expires_at > lock0.expires_at


def test_release_lock(run_service):
    """Test method `HTTPController.release_lock`."""

    run_service(from_factory=get_http_controller_app, port=8080)
    c = HTTPController("http://localhost:8080")

    c.queue_push("0", Info())

    # get lock
    lock = c.queue_pop("some-name")
    c.release_lock(lock.id)

    with pytest.raises(ValueError):
        c.refresh_lock(lock.id)


def test_registry_push_get_x(run_service):
    """
    Test methods `HTTPController.registry_push` and
    `HTTPController.get_...`.
    """

    run_service(from_factory=get_http_controller_app, port=8080)
    c = HTTPController("http://localhost:8080")

    token = c.queue_push("0", Info())
    token_ = c.get_token(token.value)
    assert token_.value == token.value
    assert token_.expires is token.expires
    assert token_.expires_at == token.expires_at

    lock = c.queue_pop("some-name")

    # status only
    assert c.get_status(token.value) == "queued"
    c.registry_push(lock.id, status="running")
    assert c.get_status(token.value) == "running"

    # info only
    # * adds token data on queue_push
    assert c.get_info(token.value)["token"]["value"] == token.value
    # * can be overwriten via registry_push
    c.registry_push(lock.id, info=Info().json)
    assert c.get_info(token.value) == Info().json
    # * did not change status
    assert c.get_status(token.value) == "running"


def test_threading_concurrency(run_service):
    """Test behavior of `HTTPController` with concurrent access."""

    run_service(from_factory=get_http_controller_app, port=8080)
    c = HTTPController("http://localhost:8080")

    interval = 0.0001
    n_jobs = 200
    n_workers = 10
    all_jobs_posted = False
    worker_logs = {
        # mapping of worker id and jobs ids
        i: []
        for i in range(n_workers)
    }

    def post():
        nonlocal all_jobs_posted
        for i in range(n_jobs):
            c.queue_push(str(i), Info())
            sleep(interval)
            if i % 10 == 0:
                print(".", end="", flush=True)
        print("")
        all_jobs_posted = True

    def work(worker_id: int):
        nonlocal all_jobs_posted
        sleep(worker_id * interval / n_workers)
        while True:
            lock = c.queue_pop(str(worker_id))
            if lock is None:
                if all_jobs_posted:
                    break
                continue

            worker_logs[worker_id].append(lock.token)
            sleep(interval / 10)
            c.registry_push(
                lock.id, status="completed", info={"worker": worker_id}
            )
            c.release_lock(lock.id)

    api = threading.Thread(target=post, daemon=True)

    workers = []
    for i in range(n_workers):
        workers.append(threading.Thread(target=work, args=(i,), daemon=True))
    for worker in workers:
        worker.start()
    api.start()

    api.join()
    for worker in workers:
        worker.join(60)

    data = []
    for i in range(n_jobs):
        data.append(
            (str(i), c.get_status(str(i)), json.dumps(c.get_info(str(i))))
        )

    print(
        "worker: ",
        " | ".join(
            map(
                lambda id: f"#{str(id)}   ",
                worker_logs.keys(),
            )
        ),
    )
    print(
        " stats: ",
        " | ".join(
            map(
                lambda log: f"{(str(len(log)*100/n_jobs) + ' ')[:4]}%",
                worker_logs.values(),
            )
        ),
    )

    assert len(list(filter(lambda item: item[1] == "queued", data))) == 0
    assert sum(map(len, worker_logs.values())) == n_jobs

    for worker_id, log in worker_logs.items():
        for token in log:
            assert (
                token,
                "completed",
                json.dumps({"worker": worker_id}),
            ) in data


def test_multiprocessing_concurrency(run_service):
    """Test behavior of `HTTPController` with concurrent access."""

    run_service(from_factory=get_http_controller_app, port=8080)
    c = HTTPController("http://localhost:8080")

    interval = 0.0001
    n_jobs = 200
    n_workers = 10

    def post():
        for i in range(n_jobs):
            c.queue_push(str(i), Info())
            sleep(interval)
            if i % 10 == 0:
                print(".", end="", flush=True)
        print("")

    def work(worker_id: int):
        sleep(worker_id * interval / n_workers)
        c = HTTPController("http://localhost:8080", timeout=10)
        while True:
            lock = c.queue_pop(str(worker_id))
            if lock is None:
                break

            sleep(interval / 10)
            c.registry_push(
                lock.id, status="completed", info={"worker": worker_id}
            )
            c.release_lock(lock.id)

    post()

    workers = []
    for i in range(n_workers):
        workers.append(DilledProcess(target=work, args=(i,), daemon=True))
    for worker in workers:
        worker.start()

    for worker in workers:
        worker.join(60)

    data = []
    for i in range(n_jobs):
        data.append((str(i), c.get_status(str(i)), c.get_info(str(i))))

    worker_logs = {}
    for i in range(n_workers):
        worker_logs[i] = list(
            map(
                lambda d: d[0],
                filter(lambda d, i=i: d[2].get("worker") == i, data),
            )
        )

    print(
        "worker: ",
        " | ".join(
            map(
                lambda id: f"#{str(id)}   ",
                worker_logs.keys(),
            )
        ),
    )
    print(
        " stats: ",
        " | ".join(
            map(
                lambda log: f"{(str(len(log)*100/n_jobs) + ' ')[:4]}%",
                worker_logs.values(),
            )
        ),
    )

    assert len(data) == n_jobs
    assert len(list(filter(lambda item: item[1] == "queued", data))) == 0
    assert sum(map(len, worker_logs.values())) == n_jobs

    for worker_id, log in worker_logs.items():
        for token in log:
            assert (
                token,
                "completed",
                {"worker": worker_id},
            ) in data


def test_message_push(run_service):
    """Test method `HTTPController.message_push`."""

    run_service(from_factory=get_http_controller_app, port=8080)
    c = HTTPController("http://localhost:8080")

    # basic submission
    token = c.queue_push("0", Info())
    c.message_push(token.value, "abort", "test", "reason for abort")
    assert len(c.message_get(0)) == 1

    # second message
    c.message_push(token.value, "abort", "test-2", "reason for abort 2")
    assert len(c.message_get(0)) == 2

    # submit with invalid token: ignored
    c.message_push("1", "abort", "test", "reason for abort")
    assert len(c.message_get(0)) == 2

    # other errors: not ignored
    with pytest.raises(ValueError):
        c.message_push(token.value, "some-instruction", "test", "reason")


def test_message_get(run_service):
    """Test method `HTTPController.message_get`."""

    run_service(from_factory=get_http_controller_app, port=8080)
    c = HTTPController("http://localhost:8080")

    # basic submission
    token = c.queue_push("0", Info())
    c.message_push(token.value, "abort", "test", "reason for abort")

    # test argument types
    assert len(c.message_get(None)) == 1
    assert len(c.message_get(datetime.now() - timedelta(seconds=1))) == 1
    assert len(c.message_get(int(datetime.now().timestamp()) - 1)) == 1

    # test ranges
    message = c.message_get(None)[0]
    assert len(c.message_get(message.received_at - timedelta(seconds=1))) == 1
    assert len(c.message_get(message.received_at)) == 1
    assert len(c.message_get(message.received_at + timedelta(seconds=1))) == 0
