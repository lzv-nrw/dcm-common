"""Test suite for the adapter-subpackage."""

from time import sleep

import pytest
import dcm_import_module_sdk

from dcm_common.services import APIResult, ServiceAdapter
from dcm_common.models.data_model import get_model_serialization_test
from dcm_common.services.tests import external_service, run_service


test_stage_info_json = get_model_serialization_test(
    APIResult, (
        ((False,), {}),
        ((True, True), {"report": {"host": ""}}),
    )
)


class _TestAdapter(ServiceAdapter):
    """`ServiceAdapter` for automated tests."""
    _SERVICE_NAME = "Test Module"
    _SDK = dcm_import_module_sdk  # used as placeholder for OpenAPITools-sdks

    def _get_api_clients(self):
        client = self._SDK.ApiClient(self._SDK.Configuration(host=self._url))
        return self._SDK.DefaultApi(client), self._SDK.ImportApi(client)

    def _get_api_endpoint(self):
        return self._api_client.import_external

    def _build_request_body(self, base_request_body, target):
        return base_request_body

    def success(self, info):
        return info.report.get("data", {}).get("success", False)


@pytest.fixture(name="port")
def _port():
    return 8080


@pytest.fixture(name="url")
def _url(port):
    return f"http://localhost:{port}"


@pytest.fixture(name="adapter")
def _adapter(url):
    return _TestAdapter(url)


@pytest.fixture(name="request_body")
def _request_body():
    return {
        "import": {"plugin": "demo", "args": {"number": 1}}
    }


@pytest.fixture(name="token")
def _token():
    return {
        "value": "eb7948a58594df3400696b6ce12013b0e26348ef27e",
        "expires": True,
        "expires_at": "2024-08-09T13:15:10+00:00"
    }


@pytest.fixture(name="report")
def _report(url, token, request_body):
    return {
        "host": url,
        "token": token,
        "args": request_body,
        "progress": {
            "status": "completed",
            "verbose": "Job terminated normally.",
            "numeric": 100
        },
        "log": {
            "EVENT": [
                {
                    "datetime": "2024-08-09T12:15:10+00:00",
                    "origin": "Test Module",
                    "body": "Some event"
                },
            ]
        },
        "data": {
            "success": True,
            "IEs": {
                "ie0": {
                    "path": "ie/4a814fe6-b44e-4546-95ec-5aee27cc1d8c",
                    "sourceIdentifier": "test:oai_dc:f50036dd-b4ef",
                    "fetchedPayload": True,
                    "logId": "0@demo-plugin",
                    "IPIdentifier": None
                }
            },
            "IPs": {}
        }
    }


@pytest.fixture(name="test_api")
def _test_api(port, token, report, run_service):
    run_service(
        routes=[
            ("/import/external", lambda: (token, 201), ["POST"]),
            ("/report", lambda: (report, 200), ["GET"]),
        ],
        port=port
    )


@pytest.fixture(name="run_result")
def _run_result(
    adapter: _TestAdapter, request_body, test_api
):
    """Returns result of `_TestAdapter.run`."""
    adapter.run(request_body, None, info := APIResult())
    return info


def test_run_minimal(
    adapter: _TestAdapter, request_body, report, test_api
):
    """Test method `run` of `_TestAdapter`."""
    adapter.run(request_body, None, info := APIResult())

    assert info.completed
    assert info.success
    assert info.report == report


def test_run_hooks(adapter: _TestAdapter, request_body, test_api):
    """
    Test execution of post-submission hooks in method `run` of
    `_TestAdapter`.
    """

    hook_info = {}
    adapter.run(
        request_body, None, info := APIResult(),
        post_submission_hooks=(
            lambda token: hook_info.update({"token": token}),
        )
    )

    assert "token" in hook_info
    assert hook_info["token"] == info.report["token"]["value"]


def test_run_no_connection(adapter: _TestAdapter, request_body):
    """
    Test behavior of method `run` of `_TestAdapter` if no connection
    can be made.
    """

    adapter.run(request_body, None, info := APIResult())

    assert info.completed
    assert not info.success
    assert "no connection" in str(info.report)


def test_run_bad_request_body(adapter: _TestAdapter, request_body):
    """
    Test behavior of method `run` of `_TestAdapter` if request body
    is bad.
    """

    del request_body["import"]

    adapter.run(request_body, None, info := APIResult())

    assert info.completed
    assert not info.success
    assert "invalid request body" in str(info.report)


def test_run_rejected(
    adapter: _TestAdapter, request_body, port, run_service
):
    """
    Test behavior of method `run` of `_TestAdapter` if request is
    rejected.
    """

    msg = "rejected"
    run_service(
        routes=[("/import/external", lambda: (msg, 422), ["POST"])], port=port
    )

    adapter.run(request_body, None, info := APIResult())

    assert info.completed
    assert not info.success
    assert msg in str(info.report)


def test_run_request_timeout_initial(
    adapter: _TestAdapter, request_body, port, run_service
):
    """
    Test behavior of method `run` of `_TestAdapter` if initial
    request times out.
    """

    msg = "some message"
    adapter.REQUEST_TIMEOUT = 0.01

    def _import():
        sleep(2*adapter.REQUEST_TIMEOUT)
        return msg, 400
    run_service(routes=[("/import/external", _import, ["POST"])], port=port)

    adapter.run(request_body, None, info := APIResult())

    assert info.completed
    assert not info.success
    assert msg not in str(info.report)
    assert "timed out" in str(info.report)


def test_run_request_timeout_polling(
    adapter: _TestAdapter, request_body, port, token, run_service
):
    """
    Test behavior of method `run` of `_TestAdapter` if a request
    times out while polling for a report.
    """

    msg = "some message"
    adapter.REQUEST_TIMEOUT = 0.01

    def _get_report():
        sleep(2*adapter.REQUEST_TIMEOUT)
        return msg, 400
    run_service(
        routes=[
            ("/import/external", lambda: (token, 201), ["POST"]),
            ("/report", _get_report, ["GET"]),
        ],
        port=port
    )

    adapter.run(request_body, None, info := APIResult())

    assert info.completed
    assert not info.success
    assert msg not in str(info.report)
    assert "timed out" in str(info.report)


def test_run_job_timeout(
    adapter: _TestAdapter, request_body, port, token, report, run_service
):
    """
    Test behavior of method `run` of `_TestAdapter` if the polling
    for a report times out.
    """

    adapter.timeout = 0.01
    adapter.interval = 0.01
    run_service(
        routes=[
            ("/import/external", lambda: (token, 201), ["POST"]),
            ("/report", lambda: (report, 503), ["GET"]),
        ],
        port=port
    )

    adapter.run(request_body, None, info := APIResult())

    assert info.completed
    assert not info.success
    assert "timed out" in str(info.report)


def test_success(adapter: _TestAdapter, run_result: APIResult):
    """Test method `success` of `_TestAdapter`."""
    assert adapter.success(run_result)
    run_result.report["data"]["success"] = False
    assert not adapter.success(run_result)


def test_get_info(adapter: _TestAdapter, request_body, test_api):
    """Test method `get_info` of `_TestAdapter`."""
    adapter.run(request_body, None, info := APIResult())
    _info = adapter.get_info(info.report["token"]["value"])

    assert info.report == _info.report
    assert info.completed == _info.completed
    assert info.success == _info.success


def test_get_info_override(adapter: _TestAdapter, request_body, test_api):
    """Test method `get_info` of `_TestAdapter` with override-arg."""
    adapter.run(request_body, None, info := APIResult())

    class FakeResponse:
        def to_dict(self):
            return info.report["progress"]

    _info = adapter.get_info(
        info.report["token"]["value"],
        endpoint=lambda token, _request_timeout: FakeResponse()
    )

    assert info.report["progress"] == _info.report
    assert not _info.completed
    assert _info.success is None


def test_get_report(adapter: _TestAdapter, request_body, test_api):
    """Test method `get_info` of `_TestAdapter`."""
    adapter.run(request_body, None, info := APIResult())
    _report = adapter.get_report(info.report["token"]["value"])

    assert info.report == _report
