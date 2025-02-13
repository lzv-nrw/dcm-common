"""Test suite for the handlers-module."""

from pathlib import Path
from importlib.metadata import version

import pytest
if version("data_plumber_http").startswith("1."):
    from data_plumber_http.settings import Responses
else:  # TODO remove legacy-support
    from data_plumber_http import Responses as _R

    def Responses():  # pylint: disable=invalid-name
        "Mimic access to Responses as in v1."
        return _R

from dcm_common.services import handlers


@pytest.mark.parametrize(
    ("json", "status"),
    [
        ({"key": "value"}, 400),
        ({}, Responses().GOOD.status),
    ],
    ids=["no-args", "args"]
)
def test_no_args_handler_known(json, status):
    "Test `no_args_handler`."

    output = handlers.no_args_handler.run(json=json)

    assert output.last_status == status


@pytest.mark.parametrize(
    ("json", "status"),
    (pytest_args := [
        ({"notoken": ""}, 400),
        ({"token": ""}, Responses().GOOD.status),
        ({"token": "value"}, Responses().GOOD.status),
    ]),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))]
)
def test_report_handler(json, status):
    "Test `report_handler`."

    output = handlers.report_handler.run(json=json)

    assert output.last_status == status


def test_target_path():
    "Test `_relative_to` of `TargetPath`."

    expected_result = "fixtures"
    result = handlers.TargetPath(
        _relative_to=Path("test_dcm_common")
    ).make(json=f"test_dcm_common/{expected_result}", loc="")

    assert result[0] == Path(expected_result)


@pytest.mark.parametrize(
    ("json", "status"),
    (pytest_args := [
        ({"no-token": None}, 400),
        ({"token": None}, 422),
        ({"token": "value"}, Responses().GOOD.status),
        ({"token": "value", "unknown": None}, 400),
        ({"token": "value", "broadcast": None}, 422),
        ({"token": "value", "broadcast": "true"}, Responses().GOOD.status),
        ({"token": "value", "re-queue": None}, 422),
        ({"token": "value", "re-queue": "true"}, Responses().GOOD.status),
        ({"token": "value", "broadcast": "false", "re-queue": "false"}, Responses().GOOD.status),
    ]),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))]
)
def test_abort_query_handler(json, status):
    "Test `abort_query_handler`."

    output = handlers.abort_query_handler.run(json=json)

    assert output.last_status == status


@pytest.mark.parametrize(
    ("json", "status"),
    (pytest_args := [
        ({}, Responses().GOOD.status),
        ({"unknown": None}, 400),
        ({"reason": None}, 422),
        ({"reason": ""}, Responses().GOOD.status),
        ({"origin": None}, 422),
        ({"origin": ""}, Responses().GOOD.status),
        ({"origin": "", "reason": ""}, Responses().GOOD.status),
    ]),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))]
)
def test_abort_body_handler(json, status):
    "Test `abort_body_handler`."

    output = handlers.abort_body_handler.run(json=json)

    assert output.last_status == status
