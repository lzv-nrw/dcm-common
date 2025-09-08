"""Tests for the `Worker`-class."""

from typing import Optional
from dataclasses import dataclass
from time import sleep, time
from uuid import uuid4
from random import randrange

import pytest

from dcm_common import LoggingContext
from dcm_common.models import JSONObject
from dcm_common.orchestra.controller import SQLiteController
from dcm_common.orchestra.worker import Worker, WorkerState
from dcm_common.orchestra.models import (
    ChildJob,
    JobContext,
    JobInfo,
    JobConfig,
    Status,
    Report,
)
from dcm_common.orchestra.dilled import dillignore


@dataclass(kw_only=True)
class ReportWithData(Report):
    data: Optional[JSONObject] = None


def test_constructor():
    """Test behavior of `Worker`-contructor."""

    # at least one entry in map
    with pytest.raises(ValueError):
        Worker(SQLiteController(), {}, {})

    # maps need to have the same keys
    with pytest.raises(ValueError):
        Worker(
            SQLiteController(),
            {"type-a": lambda: None},
            {"type-b": None},
        )

    # ok
    Worker(
        SQLiteController(),
        {"type-a": lambda: None},
        {"type-a": None},
    )


def test_simple():
    """Test plain job execution using a `Worker`."""

    def job(context: JobContext, info: JobInfo):
        assert info.report.data is None
        info.report.data = {
            "original": info.config.original_body,
            "request": info.config.request_body,
        }
        info.report.log.set_default_origin("Test-Service")
        info.report.log.log(LoggingContext.INFO, body="test")
        context.push()

    worker = Worker(
        SQLiteController(),
        {"test": job},
        {"test": ReportWithData},
    )

    worker.start(0.01, True)

    submission_info = JobInfo(JobConfig("test", {"a": 0}, {"b": 1}))
    token = worker.controller.queue_push("0", submission_info)

    worker.stop_on_idle(True, timeout=1)

    assert worker.controller.get_status(token.value)

    info = JobInfo.from_json(worker.controller.get_info(token.value))
    info.report = ReportWithData.from_json(info.report)
    assert info.report.data == {
        "original": submission_info.config.original_body,
        "request": submission_info.config.request_body,
    }
    assert info.metadata.produced is not None
    assert info.metadata.consumed is not None
    assert info.metadata.completed is not None
    assert info.metadata.aborted is None
    assert info.report.progress.status is Status.COMPLETED
    assert LoggingContext.EVENT in info.report.log
    assert LoggingContext.INFO in info.report.log
    assert LoggingContext.ERROR not in info.report.log
    print(info.report.log.fancy())


def test_state(temporary_directory):
    """Test `Worker.state`."""

    path0 = temporary_directory / str(uuid4())
    path1 = temporary_directory / str(uuid4())

    def job(_: JobContext, __: JobInfo):
        path0.touch()
        # block until released via host
        while not path1.is_file():
            sleep(0.01)

    worker = Worker(
        SQLiteController(),
        {"test": job},
        {"test": ReportWithData},
    )
    assert worker.state is WorkerState.STOPPED

    worker.start(0.01, True)
    assert worker.state is WorkerState.IDLE

    worker.controller.queue_push("0", JobInfo(JobConfig("test", {}, {})))

    # block until released via child
    # this ensures that the job is actually running
    while not path0.is_file():
        sleep(0.01)

    assert worker.state is WorkerState.BUSY
    path1.touch()

    time0 = time()
    while worker.state is not WorkerState.IDLE and time() - time0 < 1:
        sleep(0.01)

    assert worker.state is WorkerState.IDLE

    worker.stop_on_idle(True, timeout=1)

    assert worker.state is WorkerState.STOPPED


def test_unknown_type():
    """
    Test behavior of `Worker` when encountering an unknown type.
    """

    worker = Worker(
        SQLiteController(),
        {"test": lambda context, info: None},
        {"test": ReportWithData},
    )

    token = worker.controller.queue_push(
        "0", JobInfo(JobConfig("test-2", {}, {}))
    )

    worker.start(0.01, True)

    time0 = time()
    while worker.state is not WorkerState.STOPPED and time() - time0 < 1:
        sleep(0.01)

    assert worker.state is WorkerState.STOPPED

    # worker stopped (unknown type) and released lock but queue is not empty
    assert worker.controller.queue_pop("test").token == token.value
    assert worker.controller.get_status(token.value) == "queued"

    # safeguard for test - probably not necessary
    worker.stop_on_idle(True, timeout=1)


def test_prefilled_report():
    """Test conservation of pre-existing data in report."""

    def job(_: JobContext, __: JobInfo):
        pass

    worker = Worker(
        SQLiteController(),
        {"test": job},
        {"test": ReportWithData},
    )

    worker.start(0.01, True)

    data = {"key": "value"}
    token = worker.controller.queue_push(
        "0",
        JobInfo(
            JobConfig("test", {"a": 0}, {"b": 1}),
            report=ReportWithData(data={"key": "value"}),
        ),
    )

    worker.stop_on_idle(True, timeout=1)

    info = worker.controller.get_info(token.value)
    assert info["report"]["data"] == data
    assert info["report"]["progress"]["status"] == Status.COMPLETED.value


def test_abort_message():
    """Test abort via message."""

    def job(context: JobContext, info: JobInfo):
        # add child and log
        context.add_child(
            ChildJob(
                "c0",
                "child 0",
                lambda origin, reason: info.report.log.log(
                    LoggingContext.WARNING, body="child 'c0' aborted"
                ),
            )
        )
        info.report.log.log(LoggingContext.WARNING, body="child created")
        # add and remove another child
        context.add_child(
            ChildJob(
                "c1",
                "child 1",
                lambda origin, reason: info.report.log.log(
                    LoggingContext.WARNING, body="child 'c1' aborted"
                ),
            )
        )
        context.remove_child("c1")
        context.push()
        sleep(10)
        info.report.log.log(LoggingContext.INFO, body="test")
        context.push()

    worker = Worker(
        SQLiteController(),
        {"test": job},
        {"test": ReportWithData},
    )

    token = worker.controller.queue_push(
        "0",
        JobInfo(JobConfig("test", {}, {})),
    )

    worker.start(0.01, True)

    # wait for child being registered
    time0 = time()
    while time() - time0 < 1:
        report = ReportWithData.from_json(
            worker.controller.get_info(token.value).get("report", {})
        )
        if LoggingContext.WARNING in report.log:
            break
        sleep(0.01)

    worker.controller.message_push("0", "abort", "test", "test reason")

    worker.stop_on_idle(True, timeout=1)

    info = JobInfo.from_json(worker.controller.get_info(token.value))
    info.report = ReportWithData.from_json(info.report)
    # job did not complete
    assert LoggingContext.INFO not in info.report.log
    # abort got logged
    assert LoggingContext.ERROR in info.report.log
    assert len(info.report.log[LoggingContext.ERROR]) == 1
    assert "test reason" in info.report.log[LoggingContext.ERROR][0].body
    # status is aborted
    assert info.report.progress.status is Status.ABORTED
    # child abort-callback is executed
    assert LoggingContext.WARNING in info.report.log
    assert len(info.report.log[LoggingContext.WARNING]) == 2
    assert (
        "child 'c0' aborted" in info.report.log[LoggingContext.WARNING][1].body
    )


def test_kill():
    """Test abort via kill."""

    def job(context: JobContext, info: JobInfo):
        # add child and log
        context.add_child(
            ChildJob(
                "c0",
                "child 0",
                lambda origin, reason: info.report.log.log(
                    LoggingContext.WARNING, body="child 'c0' aborted"
                ),
            )
        )
        info.report.log.log(LoggingContext.WARNING, body="child created")
        # add and remove another child
        context.add_child(
            ChildJob(
                "c1",
                "child 1",
                lambda origin, reason: info.report.log.log(
                    LoggingContext.WARNING, body="child 'c1' aborted"
                ),
            )
        )
        context.remove_child("c1")
        context.push()
        sleep(10)
        info.report.log.log(LoggingContext.INFO, body="test")
        context.push()

    worker = Worker(
        SQLiteController(),
        {"test": job},
        {"test": ReportWithData},
    )

    token = worker.controller.queue_push(
        "0",
        JobInfo(JobConfig("test", {}, {})),
    )

    worker.start(0.01, True)

    time0 = time()
    while time() - time0 < 1:
        report = ReportWithData.from_json(
            worker.controller.get_info(token.value).get("report", {})
        )
        if LoggingContext.WARNING in report.log:
            break
        sleep(0.01)

    worker.kill("test", "test reason", True)

    assert worker.state is WorkerState.STOPPED

    info = JobInfo.from_json(worker.controller.get_info(token.value))
    info.report = ReportWithData.from_json(info.report)
    # job did not complete
    assert LoggingContext.INFO not in info.report.log
    # abort got logged
    assert LoggingContext.ERROR in info.report.log
    assert len(info.report.log[LoggingContext.ERROR]) == 1
    assert "test reason" in info.report.log[LoggingContext.ERROR][0].body
    # status is aborted
    assert info.report.progress.status is Status.ABORTED
    # child abort-callback is executed
    assert LoggingContext.WARNING in info.report.log
    assert len(info.report.log[LoggingContext.WARNING]) == 2
    assert (
        "child 'c0' aborted" in info.report.log[LoggingContext.WARNING][1].body
    )


def test_job_unexpected_exit():
    """Test behavior if job is killed from somewhere else."""

    def job(context: JobContext, info: JobInfo):
        sleep(1)
        info.report.log.log(LoggingContext.INFO, body="test")
        context.push()

    worker = Worker(
        SQLiteController(),
        {"test": job},
        {"test": ReportWithData},
    )

    token = worker.controller.queue_push(
        "0",
        JobInfo(JobConfig("test", {}, {})),
    )

    worker.start(0.01, True)

    time0 = time()
    while (
        worker.controller.get_status(token.value) == "queued"
        and time() - time0 < 1
    ):
        sleep(0.01)

    # pylint: disable=protected-access
    worker._process.kill()

    time0 = time()
    while worker.state is WorkerState.BUSY and time() - time0 < 1:
        sleep(0.01)

    assert worker.state is WorkerState.IDLE

    worker.stop(True, timeout=1)

    assert worker.state is WorkerState.STOPPED

    info = JobInfo.from_json(worker.controller.get_info(token.value))
    info.report = ReportWithData.from_json(info.report)
    # job did not complete
    assert LoggingContext.INFO not in info.report.log
    # abort got logged
    assert LoggingContext.ERROR in info.report.log
    assert len(info.report.log[LoggingContext.ERROR]) == 1
    # status is aborted
    assert info.report.progress.status is Status.ABORTED


def test_job_with_exception():
    """
    Test behavior of `Worker` when an uncaught exception is raised in
    the job command.
    """

    def job(context: JobContext, info: JobInfo):
        raise ValueError("Test")

    worker = Worker(
        SQLiteController(),
        {"test": job},
        {"test": ReportWithData},
    )

    token = worker.controller.queue_push(
        "0", JobInfo(JobConfig("test", {}, {}))
    )

    worker.start(0.01, True)
    worker.stop_on_idle(True, timeout=1)

    info = JobInfo.from_json(worker.controller.get_info(token.value))
    info.report = ReportWithData.from_json(info.report)
    assert LoggingContext.ERROR in info.report.log
    assert len(info.report.log[LoggingContext.ERROR]) == 1
    assert (
        'raise ValueError("Test")'
        in info.report.log[LoggingContext.ERROR][0].body
    )
    assert info.report.progress.status is Status.COMPLETED
    assert info.metadata.completed is not None


def test_lost_lock():
    """Test behavior if the lock has been lost."""

    def job(context: JobContext, info: JobInfo):
        sleep(1)
        info.report.log.log(LoggingContext.INFO, body="test")
        context.push()

    worker = Worker(
        SQLiteController(),
        {"test": job},
        {"test": ReportWithData},
        lock_refresh_interval=0.1,
    )

    token = worker.controller.queue_push(
        "0",
        JobInfo(JobConfig("test", {}, {})),
    )

    worker.start(0.01, True)

    time0 = time()
    while (
        worker.controller.get_status(token.value) == "queued"
        and time() - time0 < 1
    ):
        sleep(0.01)

    # cause locking-issue
    worker.controller.lock_ttl = 0.01

    worker.stop(True, timeout=1)

    info = JobInfo.from_json(worker.controller.get_info(token.value))
    info.report = ReportWithData.from_json(info.report)
    # job did not complete
    assert LoggingContext.INFO not in info.report.log
    # abort got logged
    assert LoggingContext.ERROR in info.report.log
    assert len(info.report.log[LoggingContext.ERROR]) == 1
    assert (
        "Aborted by controller"
        in info.report.log[LoggingContext.ERROR][0].body
    )
    # status is aborted
    assert info.report.progress.status is Status.ABORTED


def test_job_timeout():
    """
    Test behavior of `Worker` when a job exceeds its maximum duration.
    """

    def job(context: JobContext, info: JobInfo):
        sleep(1)
        info.report.log.log(LoggingContext.INFO, body="test")
        context.push()

    worker = Worker(
        SQLiteController(),
        {"test": job},
        {"test": ReportWithData},
        process_timeout=0.01,
    )

    token = worker.controller.queue_push(
        "0",
        JobInfo(JobConfig("test", {}, {})),
    )

    worker.start(0.01, True)

    time0 = time()
    while (
        worker.controller.get_status(token.value) == "queued"
        and time() - time0 < 1
    ):
        sleep(0.01)

    worker.stop(True, timeout=1)

    info = JobInfo.from_json(worker.controller.get_info(token.value))
    info.report = ReportWithData.from_json(info.report)
    # job did not complete
    assert LoggingContext.INFO not in info.report.log
    # abort got logged
    assert LoggingContext.ERROR in info.report.log
    assert len(info.report.log[LoggingContext.ERROR]) == 1
    assert "process timeout" in info.report.log[LoggingContext.ERROR][0].body
    # status is aborted
    assert info.report.progress.status is Status.ABORTED


def test_long_queue():
    """Test processing of a long queue."""

    def job(context: JobContext, info: JobInfo):
        info.report.log.log(LoggingContext.INFO, body="test")
        context.push()

    worker = Worker(
        SQLiteController(),
        {"test": job},
        {"test": ReportWithData},
    )

    tokens = []
    for i in range(10):
        tokens.append(
            worker.controller.queue_push(
                str(i),
                JobInfo(JobConfig("test", {}, {})),
            )
        )

    worker.start(0.01, True)
    worker.stop_on_idle(True, timeout=1)

    for token in tokens:
        info = JobInfo.from_json(worker.controller.get_info(token.value))
        info.report = ReportWithData.from_json(info.report)
        # job is completed
        assert LoggingContext.INFO in info.report.log
        assert info.report.progress.status is Status.COMPLETED


def test_stop_on_idle():
    """Test stopping behavior."""

    def job(context: JobContext, info: JobInfo):
        sleep(0.01)
        info.report.log.log(LoggingContext.INFO, body="test")
        context.push()

    worker = Worker(
        SQLiteController(),
        {"test": job},
        {"test": ReportWithData},
    )

    for i in range(10):
        worker.controller.queue_push(
            str(i),
            JobInfo(JobConfig("test", {}, {})),
        )

    worker.start(0.01, True)
    worker.stop_on_idle(True, timeout=1)

    assert worker.controller.queue_pop("") is None


def test_stop():
    """Test stopping behavior."""

    def job(context: JobContext, info: JobInfo):
        sleep(0.01)
        info.report.log.log(LoggingContext.INFO, body="test")
        context.push()

    worker = Worker(
        SQLiteController(),
        {"test": job},
        {"test": ReportWithData},
    )

    for i in range(10):
        worker.controller.queue_push(
            str(i),
            JobInfo(JobConfig("test", {}, {})),
        )

    worker.start(0.01, True)
    worker.stop(True, timeout=1)

    tokens = []
    while True:
        token = worker.controller.queue_pop("test")
        if token is None:
            break
        tokens.append(token)
    assert 0 < len(tokens) < 10


def test_concurrency():
    """Test concurrent workers"""

    def job(context: JobContext, info: JobInfo):
        sleep(info.config.request_body["sleep"])
        info.report.log.log(LoggingContext.INFO, body="test")
        context.push()

    # setup workers
    controller = SQLiteController()
    n_workers = 5
    workers = []
    for i in range(n_workers):
        workers.append(
            Worker(
                controller,
                {"test": job},
                {"test": ReportWithData},
            )
        )

    # setup jobs
    n_jobs = 100
    tokens = []
    for i in range(n_jobs):
        tokens.append(
            controller.queue_push(
                str(i),
                JobInfo(
                    JobConfig("test", {}, {"sleep": 0.01 * randrange(25, 100)})
                ),
            )
        )

    # run
    for worker in workers:
        worker.start(0.01, True)

    for worker in workers:
        worker.stop_on_idle(True, timeout=1)

    # check queue
    assert controller.queue_pop("") is None

    stats = {}
    # eval results
    for token in tokens:
        info = JobInfo.from_json(worker.controller.get_info(token.value))
        info.report = ReportWithData.from_json(info.report)
        # job is completed
        assert LoggingContext.INFO in info.report.log
        assert info.report.progress.status is Status.COMPLETED
        # collect worker
        if info.metadata.consumed.by not in stats:
            stats[info.metadata.consumed.by] = 0
        stats[info.metadata.consumed.by] += 1

    assert len(stats) == n_workers

    print("# stats")
    print("worker id | workload")
    print("--------------------")
    for worker, jobs in stats.items():
        print(
            f"{worker.rsplit('-', maxsplit=1)[-1]}  "
            + f"| {str(jobs/n_jobs * 100)[0:4]}%"
        )


def test_dcm_app():
    """
    Run a test that mimics the intended usage with in a dcm-
    microservice.
    """

    @dillignore("orchestration_controller")
    class AppConfig:
        """App configuration"""

        SOME_SETTING = 0

        def __init__(self):
            self.orchestration_controller = SQLiteController()

    class View:
        """Flask-view."""

        def __init__(self, config: AppConfig):
            self.config = config

        def process(self, context: JobContext, info: JobInfo):
            info.report.data = {"result": config.SOME_SETTING}
            context.push()

    config = AppConfig()
    # change some data after instantiation to test pickling works as
    # intended
    config.SOME_SETTING = 1

    view = View(config)

    worker = Worker(
        config.orchestration_controller,
        {"test": view.process},
        {"test": ReportWithData},
    )
    config.orchestration_controller.queue_push(
        "0", JobInfo(JobConfig("test", {}, {}))
    )
    worker.start(1, True)
    worker.stop_on_idle(True, timeout=1)

    assert (
        config.orchestration_controller.get_info("0")["report"]["data"][
            "result"
        ]
        == config.SOME_SETTING
    )
