"""Tests for the `Report`- and related data models."""

from dcm_common import Logger
from dcm_common.models.data_model import get_model_serialization_test
from dcm_common.orchestra.models import Token, Status, Progress, Report


test_progress_json = get_model_serialization_test(
    Progress,
    (
        ((), {}),
        ((Status.COMPLETED, "complete", 100), {}),
    ),
)


def test_progress_helpers():
    """Test helper-methods of `Progress`."""
    progress = Progress()

    assert progress.status is Status.QUEUED

    progress.run()
    assert progress.status is Status.RUNNING

    progress.complete()
    assert progress.status is Status.COMPLETED

    progress.abort()
    assert progress.status is Status.ABORTED


test_report_json = get_model_serialization_test(
    Report,
    (
        ((), {}),
        (
            (),
            {
                "host": "host",
                "token": Token("a"),
                "args": {},
                "progress": Progress(),
                "log": Logger(),
            },
        ),
    ),
)
