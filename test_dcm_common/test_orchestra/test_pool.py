"""Tests for the `WorkerPool`-class."""

from typing import Optional
from dataclasses import dataclass
from time import sleep
from random import randrange

import pytest

from dcm_common import LoggingContext
from dcm_common.models import JSONObject
from dcm_common.orchestra.controller import SQLiteController
from dcm_common.orchestra.worker import WorkerState
from dcm_common.orchestra import WorkerPool
from dcm_common.orchestra.models import (
    JobContext,
    JobInfo,
    JobConfig,
    Status,
    Report,
)


@dataclass(kw_only=True)
class ReportWithData(Report):
    data: Optional[JSONObject] = None


def test_constructor():
    """Test constructor of class `WorkerPool`."""

    with pytest.raises(ValueError):
        WorkerPool(None, 0, {"name": ""})

    WorkerPool(None, 0)


def test_register_job_type():
    """Test method `WorkerPool.register_job_type`."""

    pool = WorkerPool(None, 0)

    pool.register_job_type("test", lambda _, __: None, ReportWithData)
    assert len(pool.kwargs["job_factory_map"].keys()) == 1
    assert len(pool.kwargs["report_type_map"].keys()) == 1

    pool.register_job_type("test-2", lambda _, __: None, ReportWithData)
    assert len(pool.kwargs["job_factory_map"].keys()) == 2
    assert len(pool.kwargs["report_type_map"].keys()) == 2


def test_register_duplicate_init():
    """Test method `WorkerPool.init`."""

    pool = WorkerPool(
        None,
        2,
        {
            "job_factory_map": {"test": lambda _, __: None},
            "report_type_map": {"test": ReportWithData},
        },
    )
    assert pool.workers is None
    pool.init()
    assert pool.workers is not None
    with pytest.raises(RuntimeError):
        pool.init()


def test_concurrency():
    """Test concurrent workers in pool."""

    def job(context: JobContext, info: JobInfo):
        sleep(info.config.request_body["sleep"])
        info.report.log.log(LoggingContext.INFO, body="test")
        context.push()

    # setup workers
    pool = WorkerPool(
        SQLiteController(),
        5,
        {
            "job_factory_map": {"test": job},
            "report_type_map": {"test": ReportWithData},
        },
    )

    # setup jobs
    n_jobs = 100
    tokens = []
    for i in range(n_jobs):
        tokens.append(
            pool.controller.queue_push(
                str(i),
                JobInfo(
                    JobConfig("test", {}, {"sleep": 0.01 * randrange(25, 100)})
                ),
            )
        )

    # run (w auto-init)
    pool.start(interval=0.01, daemon=True)
    # duplicate start ok
    pool.start(interval=0.01, daemon=True)
    # can be stopped
    pool.stop(block=True, timeout=1)
    for worker in pool.workers.values():
        assert worker.state is WorkerState.STOPPED
    # and restarted
    pool.start(interval=0.01, daemon=True)

    pool.stop_on_idle(block=True, timeout=1)

    # check queue
    assert pool.controller.queue_pop("") is None

    stats = {}
    # eval results
    for token in tokens:
        info = JobInfo.from_json(pool.controller.get_info(token.value))
        info.report = ReportWithData.from_json(info.report)
        # job is completed
        assert LoggingContext.INFO in info.report.log
        assert info.report.progress.status is Status.COMPLETED
        # collect worker
        if info.metadata.consumed.by not in stats:
            stats[info.metadata.consumed.by] = 0
        stats[info.metadata.consumed.by] += 1

    assert len(stats) == pool.size

    print("# stats")
    print("worker id | workload")
    print("--------------------")
    for worker, jobs in stats.items():
        print(
            f"{worker.rsplit('-', maxsplit=1)[-1]}  "
            + f"| {str(jobs/n_jobs * 100)[0:4]}%"
        )
