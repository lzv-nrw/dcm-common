"""Test module for the orchestra-extension."""

import threading
from unittest.mock import patch

from dcm_common.orchestra import SQLiteController, WorkerPool
from dcm_common.orchestra.worker import WorkerState
from dcm_common.services.extensions.common import ExtensionEventRequirement
from dcm_common.services.extensions.orchestra import orchestra_loader


class AppConfig:
    ORCHESTRA_AT_STARTUP = True
    ORCHESTRA_DAEMON_INTERVAL = 1.0
    ORCHESTRA_WORKER_INTERVAL = 0.01


def test_basic():
    """Test basic functionality of orchestra-extension."""

    pool = WorkerPool(
        SQLiteController(),
        2,
        {
            "job_factory_map": {"test": lambda context, info: None},
            "report_type_map": {"test": None},
        },
    )

    orchestrad = orchestra_loader(None, AppConfig(), pool, "orchestra", False)

    orchestrad.ready.wait(1)

    assert orchestrad.ready.is_set()
    assert pool.workers is not None
    assert len(pool.workers) == 2
    for worker in pool.workers.values():
        assert worker.state is WorkerState.IDLE

    orchestrad.stop(block=True)

    for worker in pool.workers.values():
        assert worker.state is WorkerState.STOPPED


def test_basic_requirement():
    """Test basic functionality of orchestra-extension."""

    pool = WorkerPool(
        SQLiteController(),
        2,
        {
            "job_factory_map": {"test": lambda context, info: None},
            "report_type_map": {"test": None},
        },
    )

    lines = []
    with patch(
        "dcm_common.services.extensions.common.print_status",
        side_effect=lines.append,
    ), patch(
        "dcm_common.services.extensions.orchestra.print_status",
        side_effect=lines.append,
    ):
        requirement = ExtensionEventRequirement(
            threading.Event(), "test-requirement"
        )

        orchestrad = orchestra_loader(
            None, AppConfig(), pool, "orchestra", False, [requirement]
        )

        orchestrad.ready.wait(1)

        assert not orchestrad.ready.is_set()
        assert pool.workers is None

        # setting requirement to ready
        requirement.ready.set()

        orchestrad.ready.wait(2 * AppConfig.ORCHESTRA_DAEMON_INTERVAL)

        assert orchestrad.ready.is_set()
        for worker in pool.workers.values():
            assert worker.state is not WorkerState.STOPPED

        # setting requirement to not ready
        requirement.ready.clear()
        orchestrad.ready.clear()
        orchestrad.ready.wait(2 * AppConfig.ORCHESTRA_DAEMON_INTERVAL)

        assert not orchestrad.ready.is_set()
        for worker in pool.workers.values():
            assert worker.state is WorkerState.STOPPED

    orchestrad.stop(block=True)
    for line in lines:
        print(line)
