"""
Test module for the Demo Service API.
"""

from time import time, sleep
import re
from uuid import uuid4
from json import loads

import pytest

from dcm_common import LoggingContext as Context
from dcm_common.services.tests import run_service, external_service
from dcm_common.plugins import PluginInterface, Argument, Signature, JSONType
from dcm_common.services.demo import app_factory
from dcm_common.services.demo.config import AppConfig
from dcm_common.orchestra import dillignore

try:
    import dcm_demo_sdk

    sdk_available = True
except ImportError:
    sdk_available = False


@pytest.fixture(name="testing_config")
def _testing_config(temporary_directory):
    @dillignore("db", "controller", "worker_pool")
    class _AppConfig(AppConfig):
        FS_MOUNT_POINT = temporary_directory
        TESTING = True
        ORCHESTRA_DAEMON_INTERVAL = 0.01
        ORCHESTRA_WORKER_INTERVAL = 0.01
        ORCHESTRA_WORKER_ARGS = {"messages_interval": 0.01}
        DB_ADAPTER_STARTUP_INTERVAL = 0.01
        DB_ADAPTER_STARTUP_IMMEDIATELY = True

    return _AppConfig


@pytest.fixture(name="demo_app")
def _demo_app(testing_config):
    return app_factory(testing_config(), as_process=True)


@pytest.fixture(name="sdk_clients")
def _sdk_clients():
    def _get_api_clients(host):
        client = dcm_demo_sdk.ApiClient(dcm_demo_sdk.Configuration(host=host))
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
def test_sdk_ping(testing_config, default_client, run_service):
    """Run minimal test for demo-app."""
    run_service(from_factory=lambda: app_factory(testing_config()), port=8080)
    default_api: dcm_demo_sdk.DefaultApi = default_client(
        "http://localhost:8080"
    )
    assert default_api.ping() == "pong"


@pytest.mark.skipif(not sdk_available, reason="missing dcm-demo-sdk")
def test_sdk_identify(testing_config, default_client, run_service):
    """Test identify-implementation for demo-app."""
    run_service(from_factory=lambda: app_factory(testing_config()), port=8080)
    default_api: dcm_demo_sdk.DefaultApi = default_client(
        "http://localhost:8080"
    )

    # apparently, the OpenAPI-Generator sdk model_dump renames fields in json
    # so the data has to be converted to a json beforehand (which does not
    # support "exclude_none")
    # also optional fields with None-value are not removed..
    def clear_null_values(json):
        return {
            k: (clear_null_values(v) if isinstance(v, dict) else v)
            for k, v in json.items()
            if v is not None
        }

    assert clear_null_values(
        default_api.identify().to_dict()
    ) == clear_null_values(testing_config().CONTAINER_SELF_DESCRIPTION)


@pytest.mark.skipif(not sdk_available, reason="missing dcm-demo-sdk")
def test_sdk_identify_w_complex_array_plugin_arg_signature(
    default_client, run_service, testing_config
):
    class PluginWithComplexArgSignature(PluginInterface):
        _NAME = "demo-plugin"
        _DISPLAY_NAME = "Demo Plugin"
        _DESCRIPTION = "Some plugin description"
        _CONTEXT = "testing"
        _SIGNATURE = Signature(
            array0=Argument(
                JSONType.ARRAY,
                False,
                item_type=JSONType.STRING,
            ),
            array1=Argument(
                JSONType.ARRAY,
                False,
                item_type=Argument(JSONType.STRING, False),
            ),
            array2=Argument(
                JSONType.ARRAY,
                False,
                item_type=Argument(
                    JSONType.ARRAY,
                    False,
                    item_type=Argument(
                        JSONType.OBJECT,
                        False,
                        properties={"p1": Argument(JSONType.STRING, False)},
                    ),
                ),
            ),
        )

        def _get(self, context, /, **kwargs):
            return context.result

    class ThisAppConfig(testing_config):
        AVAILABLE_PLUGINS = {"demo-plugin": PluginWithComplexArgSignature()}

    run_service(from_factory=lambda: app_factory(ThisAppConfig()), port=8080)
    default_api: dcm_demo_sdk.DefaultApi = default_client(
        "http://localhost:8080"
    )

    # apparently, the OpenAPI-Generator sdk model_dump does not work with
    # anyOf (occurs in itemType of PluginArgSignature); so the data has to
    # be converted to a json beforehand (which does not support "exclude_none")
    # also optional fields with None-value are not removed..
    def clear_null_values(json):
        return {
            k: (clear_null_values(v) if isinstance(v, dict) else v)
            for k, v in json.items()
            if v is not None
        }

    # pylint: disable=comparison-with-callable
    assert clear_null_values(
        default_api.identify().to_dict()
    ) == clear_null_values(ThisAppConfig().CONTAINER_SELF_DESCRIPTION)


def test_demo_minimal_flask_client(testing_config):
    """Run test for demo-app with minimal job."""
    app = app_factory(testing_config())
    client = app.test_client()
    token = client.post("/demo", json={"demo": {"duration": 0}}).json["value"]
    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={token}").json
    assert report["progress"]["status"] == "completed"
    assert report["data"]["success"]


@pytest.mark.skipif(not sdk_available, reason="missing dcm-demo-sdk")
def test_sdk_demo_minimal(testing_config, demo_client, run_service):
    """Run test for demo-app with minimal job."""
    run_service(from_factory=lambda: app_factory(testing_config()), port=8080)
    demo_api: dcm_demo_sdk.DemoApi = demo_client("http://localhost:8080")
    token = demo_api.demo({"demo": {"duration": 0}})
    report = wait_for_report(demo_api, token).model_dump()

    assert report["progress"]["status"] == "completed"
    assert report["data"]["success"]


@pytest.mark.skipif(not sdk_available, reason="missing dcm-demo-sdk")
def test_sdk_demo_complex(testing_config, demo_client, run_service):
    """Run test for demo-app with complex job."""
    run_service(from_factory=lambda: app_factory(testing_config()), port=8080)
    run_service(from_factory=lambda: app_factory(testing_config()), port=8081)
    run_service(from_factory=lambda: app_factory(testing_config()), port=8082)
    demo_api: dcm_demo_sdk.DemoApi = demo_client("http://localhost:8080")
    token = demo_api.demo(
        {
            "demo": {
                "duration": 0,
                "children": [
                    {
                        "host": "http://localhost:8081",
                        "body": {
                            "demo": {
                                "duration": 0,
                                "children": [
                                    {
                                        "host": "http://localhost:8082",
                                        "body": {"demo": {"duration": 0}},
                                    }
                                ],
                            }
                        },
                    },
                    {
                        "host": "http://localhost:8081",
                        "body": {
                            "demo": {
                                "duration": 0,
                                "children": [
                                    {
                                        "host": "http://localhost:8082",
                                        "body": {"demo": {"duration": 0}},
                                    }
                                ],
                            }
                        },
                    },
                ],
            }
        }
    )
    report = wait_for_report(demo_api, token).model_dump()

    assert report["data"]["success"]
    assert len(report["children"]) == 2
    for child in report["children"].values():
        assert len(child["children"]) == 1


@pytest.mark.skipif(not sdk_available, reason="missing dcm-demo-sdk")
def test_sdk_demo_duration(testing_config, demo_client, run_service):
    """Run test for demo-app and job duration-setting."""
    run_service(from_factory=lambda: app_factory(testing_config()), port=8080)
    demo_api: dcm_demo_sdk.DemoApi = demo_client("http://localhost:8080")
    # skipped since the timing of the reference-call is very inconsistent
    # time0 = time()  # call for reference
    # _ = wait_for_report(
    #    demo_api, demo_api.demo({"demo": {"duration": 0}})
    # ).model_dump()
    time1 = time()  # actual run
    _ = wait_for_report(
        demo_api, demo_api.demo({"demo": {"duration": 0.25}})
    ).model_dump()
    time2 = time()
    assert time2 - time1 >= 0.25
    # assert abs((time2 - time1) - (time1 - time0)) >= 0.09


@pytest.mark.skipif(not sdk_available, reason="missing dcm-demo-sdk")
def test_sdk_demo_no_success(testing_config, demo_client, run_service):
    """
    Run test for demo-app nested jobs and different value for success.
    """
    run_service(from_factory=lambda: app_factory(testing_config()), port=8080)
    run_service(from_factory=lambda: app_factory(testing_config()), port=8081)
    demo_api: dcm_demo_sdk.DemoApi = demo_client("http://localhost:8080")
    token = demo_api.demo(
        {
            "demo": {
                "success": True,
                "duration": 0,
                "children": [
                    {
                        "host": "http://localhost:8081",
                        "body": {
                            "demo": {
                                "success": False,
                                "duration": 0,
                            }
                        },
                    },
                ],
            }
        }
    )
    report = wait_for_report(demo_api, token).model_dump()

    assert not report["data"]["success"]


@pytest.mark.skipif(not sdk_available, reason="missing dcm-demo-sdk")
def test_sdk_demo_abort(testing_config, demo_client, run_service):
    """Run test for abortion of running job."""
    run_service(from_factory=lambda: app_factory(testing_config()), port=8080)
    demo_api: dcm_demo_sdk.DemoApi = demo_client("http://localhost:8080")
    token = demo_api.demo({"demo": {"duration": 5}})
    sleep(0.1)
    demo_api.abort(
        token.value,
        abort_request={"origin": "pytest-runner", "reason": "test abort"},
    )

    report = wait_for_report(demo_api, token).model_dump()

    assert report["progress"]["status"] == "aborted"
    assert report["data"]["success"] is None
    assert Context.ERROR.name in report["log"]


def test_sdk_demo_abort_with_child(testing_config, demo_client, run_service):
    """Run test for abortion of running job with child."""
    run_service(from_factory=lambda: app_factory(testing_config()), port=8080)
    demo_api: dcm_demo_sdk.DemoApi = demo_client("http://localhost:8080")
    run_service(from_factory=lambda: app_factory(testing_config()), port=8081)
    demo_api2: dcm_demo_sdk.DemoApi = demo_client("http://localhost:8081")
    token = demo_api.demo(
        {
            "demo": {
                "success": True,
                "duration": 0,
                "children": [
                    {
                        "host": "http://localhost:8081",
                        "body": {
                            "demo": {
                                "success": False,
                                "duration": 10,
                            }
                        },
                    },
                ],
            }
        }
    )

    # wait until child is started by polling for report and checking
    # children
    time0 = time()
    while time() - time0 < 5:
        try:
            report = demo_api.get_report(token.value).model_dump()
        except dcm_demo_sdk.exceptions.ApiException as exc_info:
            report = loads(exc_info.data)
        if report.get("children", {}).get("child-0@demo") is not None:
            break
        sleep(0.05)
    assert (
        report.get("children", {}).get("child-0@demo") is not None
    ), "missing child, consider increasing maximum wait duration for this test"
    assert "child-0@demo" in report["children"]

    demo_api.abort(
        token.value,
        abort_request={"origin": "pytest-runner", "reason": "test abort"},
    )

    report = wait_for_report(demo_api, token).model_dump()

    assert report["progress"]["status"] == "aborted"
    assert report["data"]["success"] is None
    assert Context.ERROR.name in report["log"]

    assert "Job aborted by" in str(report["log"])
    assert (
        report["children"]["child-0@demo"]["progress"]["status"] == "aborted"
    )
    assert "Job aborted by" in str(report["children"]["child-0@demo"]["log"])

    token2 = re.match(
        r".*Got token '(.+)' from external service.*", str(report)
    ).groups()[0]

    report2 = demo_api2.get_report(token2).model_dump()
    assert report2["progress"]["status"] == "aborted"
    assert report2["data"]["success"] is None
    assert Context.ERROR.name in report2["log"]
    assert "Aborting child" not in str(report2)


@pytest.mark.skipif(not sdk_available, reason="missing dcm-demo-sdk")
def test_sdk_demo_plugin(testing_config, demo_client, run_service):
    """Run test for demo-app with plugin-job."""
    run_service(from_factory=lambda: app_factory(testing_config()), port=8080)
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


@pytest.mark.skipif(not sdk_available, reason="missing dcm-demo-sdk")
def test_sdk_demo_submit_with_token(testing_config, demo_client, run_service):
    """Run test for demo-app with minimal job and providing token."""
    run_service(from_factory=lambda: app_factory(testing_config()), port=8080)
    demo_api: dcm_demo_sdk.DemoApi = demo_client("http://localhost:8080")
    _token = str(uuid4())
    token = demo_api.demo({"demo": {"duration": 1}, "token": _token})
    assert token.value == _token
    with pytest.raises(dcm_demo_sdk.ApiException) as exc_info:
        demo_api.get_report(token.value).progress.status

    # repeat submission (same token)
    token = demo_api.demo({"demo": {"duration": 1}, "token": _token})
    assert token.value == _token
    with pytest.raises(dcm_demo_sdk.ApiException) as exc_info:
        demo_api.get_report(token.value).progress.status

    wait_for_report(demo_api, token).model_dump()

    # repeat again post-job (same token)
    token = demo_api.demo({"demo": {"duration": 1}, "token": _token})
    assert token.value == _token

    # already completed
    report = demo_api.get_report(token.value)
    assert report.progress.status == "completed"

    # submission fails if configuration changed (same token)
    with pytest.raises(dcm_demo_sdk.ApiException) as exc_info:
        demo_api.demo({"demo": {"duration": 0}, "token": _token})
    print(exc_info.value)
