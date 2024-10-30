"""Report-data model test-module."""

from dcm_common import Logger
from dcm_common.models.data_model import get_model_serialization_test
from dcm_common.models.report import Status, Progress
from dcm_common.models import Token, Report


def test_Progress_constructor():
    """Test constructor default values of model `Progress`."""

    p = Progress()
    assert p.status == Status.QUEUED
    assert p.numeric == 0


test_progress_serialization = get_model_serialization_test(
    Progress, (((), {}),)
)


def test_Progress_status_methods():
    """Test methods altering `status`-property of model `Progress`."""

    p = Progress()
    p.run()
    assert p.status == Status.RUNNING
    p.abort()
    assert p.status == Status.ABORTED
    p.complete()
    assert p.status == Status.COMPLETED
    p.queue()
    assert p.status == Status.QUEUED


def test_Report_constructor():
    """Test constructor default values of model `Report`."""

    r = Report(host="")
    assert isinstance(r.progress, Progress)
    assert isinstance(r.log, Logger)


test_report_json = get_model_serialization_test(
    Report, (
        ((), {"host": "some_host"}),
        ((), {"host": "some_host", "token": Token()}),
        ((), {"host": "some_host", "args": {"arg": "value"}}),
        ((), {"host": "some_host", "progress": Progress()}),
        ((), {
            "host": "some_host",
            "log": Logger(json={
                "INFO": [
                    {
                        "datetime": "2024-08-30T09:12:14",
                        "origin": "some-origin",
                        "body": "some-body"
                    }
                ]
            })
        }),
    )
)
