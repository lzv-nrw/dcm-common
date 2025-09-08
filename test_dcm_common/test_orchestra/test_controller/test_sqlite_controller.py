"""Tests for the `SQLiteController`-class."""

from datetime import datetime, timedelta
import sqlite3
import threading
from time import sleep
import json
from uuid import uuid4

import pytest

from dcm_common import LoggingContext
from dcm_common.orchestra.controller.sqlite import (
    SQLiteController,
    Transaction,
)
from dcm_common.orchestra import JobConfig, JobInfo, DilledProcess


def test_transaction():
    """Test basic functionality of `Transaction`-context manager."""
    c = Transaction.get_connection(
        "file:test?mode=memory&cache=shared", uri=True
    )
    with Transaction(c) as t:
        t.cursor.execute(
            """CREATE TABLE registry (
                token TEXT NOT NULL PRIMARY KEY
            )"""
        )
        t.cursor.execute("SELECT * FROM registry")
        t.cursor.execute("INSERT INTO registry VALUES ('0')")
        t.cursor.execute("SELECT * FROM registry")

    assert t.success
    assert t.exc_val is None
    assert len(t.data) == 1
    assert t.data[0] == ("0",)


def test_transaction_error():
    """Test `Transaction`-context manager error behavior."""
    # check off
    c = Transaction.get_connection(
        "file:test?mode=memory&cache=shared", uri=True
    )

    with Transaction(c, False, autoclose=False) as t:
        t.cursor.execute("CREATE TABLE a (id TEXT)")
        t.cursor.execute("INSERT INTO a VALUES ('a', 'b')")

    assert not t.success
    assert isinstance(t.exc_val, sqlite3.OperationalError)
    with pytest.raises(sqlite3.OperationalError):
        t.check()

    # no table exists
    with Transaction(c, False, autoclose=False) as t:
        t.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    assert t.data == []

    # the following tests for issues regarding transaction-control
    # actually create table
    with Transaction(c, False, autoclose=False) as t:
        t.cursor.execute("CREATE TABLE a (id TEXT)")
    with Transaction(c, False, autoclose=False) as t:
        t.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    assert t.data == [("a",)]
    with Transaction(c, False, autoclose=False) as t:
        t.cursor.execute("DROP TABLE a")
    with Transaction(c, False, autoclose=False) as t:
        t.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    assert t.data == []

    # repeat with check on
    with pytest.raises(sqlite3.OperationalError), Transaction(
        c, True, autoclose=False
    ) as t:
        t.cursor.execute("CREATE TABLE a (id TEXT)")
        t.cursor.execute("INSERT INTO a VALUES ('a', 'b')")

    with pytest.raises(sqlite3.OperationalError):
        t.check()

    # ok
    with Transaction(c, autoclose=False) as t:
        t.cursor.execute("CREATE TABLE a (id TEXT)")
        t.cursor.execute("INSERT INTO a VALUES ('id')")

    assert t.success
    assert t.check() is None

    with Transaction(c, False, autoclose=False) as t:
        t.cursor.execute("SELECT * FROM a")
    assert t.data == [("id",)]


def Info():  # pylint: disable=invalid-name
    """Minimal `JobInfo`."""
    return JobInfo(JobConfig("test", {}, {}))


def test_constructor(temporary_directory):
    """Test `SQLiteController` constructor."""

    # default in-memory
    c = SQLiteController()
    with Transaction(c.db) as t:
        t.cursor.execute("PRAGMA user_version")

    assert t.data[0][0] == c.SCHEMA_VERSION

    with Transaction(c.db) as t:
        t.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    assert len(t.data) == 3
    assert ("registry",) in t.data
    assert ("locks",) in t.data
    assert ("messages",) in t.data

    # another in-memory
    c = SQLiteController()
    with Transaction(c.db) as t:
        t.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    assert len(t.data) == 3

    # named in-memory
    c0 = SQLiteController(memory_id="a")
    with Transaction(c0.db) as t:
        t.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    assert len(t.data) == 3
    with Transaction(c0.db) as t:
        t.cursor.execute("CREATE TABLE a (id TEXT)")
    c1 = SQLiteController(memory_id="a")
    with Transaction(c0.db) as t:
        t.cursor.execute("CREATE TABLE b (id TEXT)")
    c0.close()
    with Transaction(c1.db) as t:
        t.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    assert len(t.data) == 5
    c1 = SQLiteController(memory_id="b")
    with Transaction(c1.db) as t:
        t.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    assert len(t.data) == 3

    # disk
    path = temporary_directory / str(uuid4())
    c = SQLiteController(path)
    assert path.is_file()
    with Transaction(c.db) as t:
        t.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    assert len(t.data) == 3


def test_queue_push():
    """Test method `SQLiteController.queue_push`."""

    c = SQLiteController()

    # basic submission
    token = c.queue_push("0", Info())
    assert token.value == "0"
    assert token.expires
    with Transaction(c.db) as t:
        t.cursor.execute("SELECT * FROM registry")
    assert len(t.data) == 1

    # resubmit same token
    assert c.queue_push("0", Info()).json == token.json
    with Transaction(c.db) as t:
        t.cursor.execute("SELECT * FROM registry")
    assert len(t.data) == 1

    # submit another job
    c.queue_push("1", Info())
    with Transaction(c.db) as t:
        t.cursor.execute("SELECT * FROM registry")
    assert len(t.data) == 2


def test_queue_push_info_token():
    """
    Test behavior of method `SQLiteController.queue_push` when actual
    JobInfo is provided (update to metadata and token).
    """

    c = SQLiteController()

    original_info = JobInfo(JobConfig("test", {}, {}))
    token = c.queue_push("0", original_info)

    info = JobInfo.from_json(c.get_info(token.value))
    assert info.token.json == token.json
    assert info.metadata.produced is not None

    # does not affect original
    assert original_info.token is None
    assert original_info.metadata.produced is None


def test_queue_push_expiration():
    """Test method `SQLiteController.queue_push` with expiration."""

    # no expiration
    c0 = SQLiteController(token_ttl=None)

    token = c0.queue_push("0", Info())
    assert not token.expires

    # expiration
    c1 = SQLiteController(token_ttl=10)

    token = c1.queue_push("0", Info())
    assert token.expires
    assert (
        pytest.approx(datetime.now().timestamp() + 10, 1)
        == token.expires_at.timestamp()
    )


def test_queue_pop():
    """Test method `SQLiteController.queue_push`."""

    c = SQLiteController()
    token = c.queue_push("0", Info())

    # get lock - ok
    lock0 = c.queue_pop("some-name")
    assert lock0 is not None
    assert lock0.token == token.value
    assert (
        pytest.approx(lock0.expires_at.timestamp())
        == datetime.now().timestamp() + c.lock_ttl
    )

    # attempt to get another - no success
    lock1 = c.queue_pop("some-name")
    assert lock1 is None

    # release
    c.release_lock(lock0.id)

    # attempt to get another - ok
    lock1 = c.queue_pop("some-name")
    assert lock1 is not None

    # release
    c.release_lock(lock1.id)

    # add more jobs
    c.queue_push("1", Info())

    # claims only one job
    lock2 = c.queue_pop("some-name")
    with Transaction(c.db) as t:
        t.cursor.execute("SELECT * from locks")
    assert len(t.data) == 1
    lock3 = c.queue_pop("some-name")
    with Transaction(c.db) as t:
        t.cursor.execute("SELECT * from locks")
    assert len(t.data) == 2
    assert lock2.token != lock3.token
    assert lock2.token in ["0", "1"]
    assert lock3.token in ["0", "1"]


def test_refresh_lock():
    """Test method `SQLiteController.refresh_lock`."""

    c = SQLiteController()
    c.queue_push("0", Info())

    # get lock and check current
    lock0 = c.queue_pop("some-name")
    with Transaction(c.db) as t:
        t.cursor.execute(
            "SELECT expires_at FROM locks WHERE id = ?", (lock0.id,)
        )
    assert t.data[0][0] == lock0.expires_at.timestamp()

    # double ttl then refresh and check
    c.lock_ttl *= 2
    lock1 = c.refresh_lock(lock0.id)
    assert lock1.expires_at > lock0.expires_at


def test_lock_expiration():
    """Test method `SQLiteController.queue_push` with lock expiration."""

    c = SQLiteController(lock_ttl=-1)
    c.queue_push("0", Info())

    # locks immediately expired
    assert c.queue_pop("some-name") is not None
    assert c.queue_pop("some-name") is not None

    # get lock, attempt to write
    lock = c.queue_pop("some-name")
    with pytest.raises(ValueError):
        c.registry_push(lock.id, status="running", info=Info())


def test_lock_expiration_due_to_token_expiration():
    """
    Test method `SQLiteController.queue_push` with lock expiration caused by
    an expired token.
    """

    c = SQLiteController()
    token = c.queue_push("0", Info())

    # get lock
    lock = c.queue_pop("some-name")
    assert lock is not None

    # manipulate token expiry date
    with Transaction(c.db) as t:
        t.cursor.execute(
            "UPDATE registry SET expires_at = 0 WHERE token = ?",
            (token.value,),
        )

    with pytest.raises(ValueError):
        c.refresh_lock(lock.id)


def test_registry_push_get_x():
    """
    Test methods `SQLiteController.registry_push` and
    `SQLiteController.get_...`.
    """

    c = SQLiteController()
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


def test_cleanup():
    """Test method `SQLiteController.cleanup`."""

    c = SQLiteController(lock_ttl=-1)
    c.queue_push("0", Info())

    # cleanup of expired lock
    c.queue_pop("some-name")
    # * check current state
    with Transaction(c.db) as t:
        t.cursor.execute("SELECT * FROM locks")
    assert len(t.data) == 1
    # * cleanup
    c.cleanup()
    # * check again
    with Transaction(c.db) as t:
        t.cursor.execute("SELECT * FROM locks")
    assert len(t.data) == 0

    # cleanup of non-expired lock
    c.lock_ttl = 10
    lock = c.queue_pop("some-name")
    # * check current state
    with Transaction(c.db) as t:
        t.cursor.execute("SELECT * FROM locks")
    assert len(t.data) == 1
    # * cleanup
    c.cleanup()
    # * check again
    with Transaction(c.db) as t:
        t.cursor.execute("SELECT * FROM locks")
    assert len(t.data) == 1
    c.release_lock(lock.id)

    # cleanup of registry
    # * unaffected by previous cleanups
    with Transaction(c.db) as t:
        t.cursor.execute("SELECT status FROM registry")
    assert len(t.data) == 1
    assert t.data[0][0] == "queued"
    # * add expired record
    token = c.queue_push("1", Info())
    # * update "1" to running without lock
    with Transaction(c.db) as t:
        t.cursor.execute(
            "UPDATE registry SET status = 'running' WHERE token = ?",
            (token.value,),
        )
    # * cleanup
    c.cleanup()
    # * check
    with Transaction(c.db) as t:
        t.cursor.execute(
            "SELECT status, info FROM registry WHERE token = ?", (token.value,)
        )
    assert len(t.data) == 1
    assert t.data[0][0] == "failed"
    info = JobInfo.from_json(json.loads(t.data[0][1]))
    assert info.metadata.aborted is not None
    assert (
        info.report.get("log", {}).get(LoggingContext.ERROR.name) is not None
    )

    # check cleanup with requeue
    c.requeue = True
    # * post another entry
    token = c.queue_push("2", Info())
    # * update "2" to running without lock
    with Transaction(c.db) as t:
        t.cursor.execute(
            "UPDATE registry SET status = 'running' WHERE token = ?",
            (token.value,),
        )
    # * cleanup
    c.cleanup()
    # * check
    with Transaction(c.db) as t:
        t.cursor.execute(
            "SELECT status, info FROM registry WHERE token = ?", (token.value,)
        )
    assert len(t.data) == 1
    assert t.data[0][0] == "queued"
    info = JobInfo.from_json(json.loads(t.data[0][1]))
    assert info.metadata.consumed is None
    assert info.metadata.completed is None
    assert info.metadata.aborted is None
    assert (
        info.report.get("log", {}).get(LoggingContext.EVENT.name) is not None
    )


@pytest.mark.parametrize(
    "path",
    [None, str(uuid4())],
    ids=["memory", "disk"],
)
def test_threading_concurrency(path, temporary_directory):
    """
    Test behavior of `SQLiteController` with concurrent access via
    threading (in-memory).
    """

    c = SQLiteController(
        path=path if path is None else temporary_directory / path
    )

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
            c.queue_push(str(i), {})
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

    with Transaction(c.db) as t:
        t.cursor.execute("SELECT token, status, info FROM registry")

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

    assert len(t.data) == n_jobs
    assert len(list(filter(lambda item: item[1] == "queued", t.data))) == 0
    assert sum(map(len, worker_logs.values())) == n_jobs

    for worker_id, log in worker_logs.items():
        for token in log:
            assert (
                token,
                "completed",
                json.dumps({"worker": worker_id}),
            ) in t.data


def test_multiprocessing_concurrency(temporary_directory):
    """
    Test behavior of `SQLiteController` with concurrent access via
    multiprocessing (filesystem).
    """

    db_file = temporary_directory / str(uuid4())
    c = SQLiteController(path=db_file)

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
        c = SQLiteController(path=db_file, timeout=10)
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


def test_message_push():
    """Test method `SQLiteController.message_push`."""

    c = SQLiteController()

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
    with pytest.raises(sqlite3.IntegrityError):
        c.message_push(token.value, "some-instruction", "test", "reason")


def test_message_get():
    """Test method `SQLiteController.message_get`."""

    c = SQLiteController()

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

    # test expiration
    c.message_ttl = -1
    c.message_push(token.value, "abort", "test", "reason for abort")
    assert len(c.message_get(None)) == 1
