"""
Test module for the Demo Service API.
"""

from time import time, sleep
import re

import pytest

from dcm_common import LoggingContext as Context
from dcm_common.services.tests import run_service, external_service
from dcm_common.services.notification import (
    app_factory as notify_app_factory, Topic, HTTPMethod
)
from dcm_common.db import MemoryStore, NativeKeyValueStoreAdapter
from dcm_common.services.demo import app_factory
from dcm_common.services.demo.config import AppConfig
try:
    import dcm_demo_sdk
    sdk_available = True
except ImportError:
    sdk_available = False


@pytest.fixture(name="testing_config")
def _testing_config(temporary_directory):
    class _AppConfig(AppConfig):
        FS_MOUNT_POINT = temporary_directory
        TESTING = True
        ORCHESTRATION_DAEMON_INTERVAL = 0.001
        ORCHESTRATION_ORCHESTRATOR_INTERVAL = 0.001
        ORCHESTRATION_ABORT_NOTIFICATIONS_STARTUP_INTERVAL = 0.01
    return _AppConfig


@pytest.fixture(name="demo_app")
def _demo_app(testing_config):
    return app_factory(testing_config(), as_process=True)


@pytest.fixture(name="sdk_clients")
def _sdk_clients():
    def _get_api_clients(host):
        client = dcm_demo_sdk.ApiClient(
            dcm_demo_sdk.Configuration(host=host)
        )
        return dcm_demo_sdk.DefaultApi(client), dcm_demo_sdk.DemoApi(client)
    return _get_api_clients


@pytest.fixture(name="default_client")
def _default_client(sdk_clients):
    def _get_api_client(host):
        return sdk_clients(host)[0]
    return _get_api_client


@pytest.fixture(name="demo_client")
def _demo_client(sdk_clients):
    def _get_api_client(host):
        return sdk_clients(host)[1]
    return _get_api_client


def wait_for_report(demo_api, token):
    """Idles until given job is completed."""
    while True:
        try:
            return demo_api.get_report(token.value)
        except dcm_demo_sdk.exceptions.ApiException as e:
            if e.status != 503:
                print(e)
                return None
        sleep(0.01)


@pytest.mark.skipif(not sdk_available, reason="missing dcm-demo-sdk")
def test_sdk_ping(demo_app, default_client, run_service):
    """Run minimal test for demo-app."""
    run_service(app=demo_app, port=8080)
    default_api: dcm_demo_sdk.DefaultApi = default_client("http://localhost:8080")
    assert default_api.ping() == "pong"


@pytest.mark.skipif(not sdk_available, reason="missing dcm-demo-sdk")
def test_sdk_identify(demo_app, default_client, run_service, testing_config):
    """Test identify-implementation for demo-app."""
    run_service(app=demo_app, port=8080)
    default_api: dcm_demo_sdk.DefaultApi = default_client("http://localhost:8080")
    assert (
        default_api.identify().model_dump(exclude_none=True)
        == testing_config().CONTAINER_SELF_DESCRIPTION
    )


def test_demo_minimal_flask_client(testing_config):
    """Run test for demo-app with minimal job."""
    testing_config.ORCHESTRATION_AT_STARTUP = False
    client = app_factory(testing_config()).test_client()
    token = client.post("/demo", json={"demo": {"duration": 0}}).json["value"]
    client.put("/orchestration?until-idle", json={})
    time0 = time()
    while time() - time0 < 2:
        r = client.get(f"/report?token={token}")
        if r.status_code == 200:
            break
        sleep(0.01)
    assert r.json["progress"]["status"] == "completed"
    assert r.json["data"]["success"]


@pytest.mark.skipif(not sdk_available, reason="missing dcm-demo-sdk")
def test_sdk_demo_minimal(demo_app, demo_client, run_service):
    """Run test for demo-app with minimal job."""
    run_service(app=demo_app, port=8080)
    demo_api: dcm_demo_sdk.DemoApi = demo_client("http://localhost:8080")
    token = demo_api.demo(
        {
            "demo": {"duration": 0}
        }
    )
    report = wait_for_report(demo_api, token).model_dump()

    assert report["progress"]["status"] == "completed"
    assert report["data"]["success"]


@pytest.mark.skipif(not sdk_available, reason="missing dcm-demo-sdk")
def test_sdk_demo_complex(demo_app, demo_client, run_service):
    """Run test for demo-app with complex job."""
    run_service(app=demo_app, port=8080)
    run_service(app=demo_app, port=8081)
    run_service(app=demo_app, port=8082)
    demo_api: dcm_demo_sdk.DemoApi = demo_client("http://localhost:8080")
    token = demo_api.demo(
        {
            "demo": {
                "duration": 0, "children": [
                    {
                        "host": "http://localhost:8081",
                        "body": {
                            "demo": {
                                "duration": 0,
                                "children": [
                                    {
                                        "host": "http://localhost:8082",
                                        "body": {"demo": {"duration": 0}}
                                    }
                                ]
                            }
                        }
                    },
                    {
                        "host": "http://localhost:8081",
                        "body": {
                            "demo": {
                                "duration": 0,
                                "children": [
                                    {
                                        "host": "http://localhost:8082",
                                        "body": {"demo": {"duration": 0}}
                                    }
                                ]
                            }
                        }
                    }
                ]
            }
        }
    )
    report = wait_for_report(demo_api, token).model_dump()

    assert report["data"]["success"]
    assert len(report["children"]) == 2
    for child in report["children"].values():
        assert len(child["children"]) == 1


@pytest.mark.skipif(not sdk_available, reason="missing dcm-demo-sdk")
def test_sdk_demo_duration(demo_app, demo_client, run_service):
    """Run test for demo-app and job duration-setting."""
    run_service(app=demo_app, port=8080)
    demo_api: dcm_demo_sdk.DemoApi = demo_client("http://localhost:8080")
    # skipped since the timing of the reference-call is very inconsistent
    #time0 = time()  # call for reference
    #_ = wait_for_report(
    #    demo_api, demo_api.demo({"demo": {"duration": 0}})
    #).model_dump()
    time1 = time()  # actual run
    _ = wait_for_report(
        demo_api, demo_api.demo({"demo": {"duration": 0.25}})
    ).model_dump()
    time2 = time()
    assert time2 - time1 >= 0.25
    # assert abs((time2 - time1) - (time1 - time0)) >= 0.09


@pytest.mark.skipif(not sdk_available, reason="missing dcm-demo-sdk")
def test_sdk_demo_no_success(demo_app, demo_client, run_service):
    """
    Run test for demo-app nested jobs and different value for success.
    """
    run_service(app=demo_app, port=8080)
    run_service(app=demo_app, port=8081)
    demo_api: dcm_demo_sdk.DemoApi = demo_client("http://localhost:8080")
    token = demo_api.demo(
        {
            "demo": {
                "success": True, "duration": 0, "children": [
                    {
                        "host": "http://localhost:8081",
                        "body": {
                            "demo": {
                                "success": False,
                                "duration": 0,
                            }
                        }
                    },
                ]
            }
        }
    )
    report = wait_for_report(demo_api, token).model_dump()

    assert not report["data"]["success"]


@pytest.mark.skipif(not sdk_available, reason="missing dcm-demo-sdk")
def test_sdk_demo_abort(demo_app, demo_client, run_service):
    """Run test for abortion of running job."""
    run_service(app=demo_app, port=8080)
    demo_api: dcm_demo_sdk.DemoApi = demo_client("http://localhost:8080")
    token = demo_api.demo(
        {
            "demo": {"duration": 5}
        }
    )
    sleep(0.1)
    demo_api.abort(
        token.value,
        abort_request={"origin": "pytest-runner", "reason": "test abort"}
    )

    report = wait_for_report(demo_api, token).model_dump()

    assert report["progress"]["status"] == "aborted"
    assert report["data"]["success"] is None
    assert Context.ERROR.name in report["log"]


def test_sdk_demo_abort_with_child(demo_app, demo_client, run_service):
    """Run test for abortion of running job with child."""
    run_service(app=demo_app, port=8080)
    demo_api: dcm_demo_sdk.DemoApi = demo_client("http://localhost:8080")
    run_service(app=demo_app, port=8081)
    demo_api2: dcm_demo_sdk.DemoApi = demo_client("http://localhost:8081")
    token = demo_api.demo(
        {
            "demo": {
                "success": True, "duration": 0, "children": [
                    {
                        "host": "http://localhost:8081",
                        "body": {
                            "demo": {
                                "success": False,
                                "duration": 5,
                            }
                        }
                    },
                ]
            }
        }
    )
    sleep(0.5)
    demo_api.abort(
        token.value,
        abort_request={"origin": "pytest-runner", "reason": "test abort"}
    )

    report = wait_for_report(demo_api, token).model_dump()

    assert report["progress"]["status"] == "aborted"
    assert report["data"]["success"] is None
    assert Context.ERROR.name in report["log"]

    token2 = re.match(
        r".*Got token '(.+)' from external service.*", str(report)
    ).groups()[0]

    report2 = demo_api2.get_report(token2).model_dump()
    assert report2["progress"]["status"] == "aborted"
    assert report2["data"]["success"] is None
    assert Context.ERROR.name in report2["log"]
    assert "Aborting child" not in str(report2)


@pytest.mark.skipif(not sdk_available, reason="missing dcm-demo-sdk")
@pytest.mark.parametrize(
    "success",
    [True, False],
    ids=["success", "no-success"]
)
def test_sdk_demo_abort_via_notification(
    success, demo_client, run_service, testing_config
):
    """
    Run test for abortion of running job by using other worker/
    notification. This is done by
    * running a notification service
    * run the first instance of the demo-app
    * submit a job and wait for it to start up
    * run another instance of the demo-app
    * abort via this second app

    In case of `not success`, the timeout is set very low causing the
    limit to be exceeded while calling the notification api.
    """
    # notifications
    run_service(
        app=notify_app_factory(
            NativeKeyValueStoreAdapter(MemoryStore()),
            topics={
                "abort": Topic("/demo", HTTPMethod.DELETE, 200)
            },
            debug=True
        ), port=5000
    )
    # first demo-app
    testing_config.ORCHESTRATION_ABORT_NOTIFICATIONS = True
    testing_config.ORCHESTRATION_ABORT_NOTIFICATIONS_URL = "http://localhost:5000"
    testing_config.ORCHESTRATION_ABORT_NOTIFICATIONS_CALLBACK = "http://localhost:8080"
    run_service(app=app_factory(testing_config(), as_process=True), port=8080)

    demo_api: dcm_demo_sdk.DemoApi = demo_client("http://localhost:8080")
    token = demo_api.demo(
        {
            "demo": {"duration": 5}
        }
    )
    sleep(0.5)

    # second demo-app
    testing_config.ORCHESTRATION_ABORT_NOTIFICATIONS_CALLBACK = "http://localhost:8081"
    if not success:
        testing_config.ORCHESTRATION_ABORT_TIMEOUT = 0.0001
    run_service(app=app_factory(testing_config(), as_process=True), port=8081)
    demo_api2: dcm_demo_sdk.DemoApi = demo_client("http://localhost:8081")
    if success:
        demo_api2.abort(
            token.value,
            abort_request={"origin": "pytest-runner", "reason": "test abort"}
        )

        report = wait_for_report(demo_api, token).model_dump()

        assert report["progress"]["status"] == "aborted"
        assert report["data"]["success"] is None
        assert Context.ERROR.name in report["log"]
    else:
        with pytest.raises(dcm_demo_sdk.exceptions.ApiException) as exc_info:
            demo_api2.abort(
                token.value,
                abort_request={"origin": "pytest-runner", "reason": "test abort"}
            )
        assert "error while making abort-request" in exc_info.value.body
        assert "timed out" in exc_info.value.body
        assert exc_info.value.status == 502
        demo_api.abort(  # actually abort job
            token.value,
            abort_request={"origin": "pytest-runner", "reason": "test abort"}
        )


def test_sdk_demo_abort_with_child_get_report_hook(
    demo_app, demo_client, run_service
):
    """
    Run test for abortion of running job with child and configured post-
    abort abort hook for collecting the report.
    """
    run_service(app=demo_app, port=8080)
    demo_api: dcm_demo_sdk.DemoApi = demo_client("http://localhost:8080")
    run_service(app=demo_app, port=8081)
    token = demo_api.demo(
        {
            "demo": {
                "success": True, "duration": 0, "children": [
                    {
                        "host": "http://localhost:8081",
                        "body": {
                            "demo": {
                                "success": False,
                                "duration": 5,
                            }
                        }
                    },
                ]
            }
        }
    )
    sleep(0.5)
    demo_api.abort(
        token.value,
        abort_request={"origin": "pytest-runner", "reason": "test abort"}
    )

    report = wait_for_report(demo_api, token).model_dump()
    assert "SIGKILL" in str(report["log"])
    assert "child-0@demo" in report["children"]
    assert report["children"]["child-0@demo"]["progress"]["status"] == "aborted"
    assert "SIGKILL" in str(report["children"]["child-0@demo"]["log"])


@pytest.mark.skipif(not sdk_available, reason="missing dcm-demo-sdk")
def test_sdk_demo_plugin(demo_app, demo_client, run_service):
    """Run test for demo-app with plugin-job."""
    run_service(app=demo_app, port=8080)
    demo_api: dcm_demo_sdk.DemoApi = demo_client("http://localhost:8080")
    token = demo_api.demo(
        {
            "demo": {
                "duration": 0,
                "successPlugin": {
                    "plugin": "demo-plugin",
                    "args": {"success": False},
                },
            }
        }
    )
    report = wait_for_report(demo_api, token).model_dump()

    assert report["progress"]["status"] == "completed"
    assert not report["data"]["success"]
    assert "demo-plugin" in str(report["log"])
