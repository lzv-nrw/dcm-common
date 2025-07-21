"""Test suite for the views-module."""

import pytest
from flask import Flask

from dcm_common.db import NativeKeyValueStoreAdapter, MemoryStore
from dcm_common.models import Token
from dcm_common.orchestration import (
    JobConfig, JobInfo, ScalableOrchestrator, Job
)
from dcm_common.services import DefaultView, ReportView, BaseConfig


@pytest.fixture(name="default_config")
def _default_config():
    return BaseConfig()


@pytest.fixture(name="default_app")
def _default_app(default_config):
    app = Flask(__name__)
    app.config.from_object(default_config)
    app.register_blueprint(
        DefaultView(
            default_config, ready=lambda: True
        ).get_blueprint(),
        url_prefix="/"
    )
    return app


@pytest.fixture(name="default_client")
def _default_client(default_app):
    return default_app.test_client()


def test_ping(default_client):
    """Test ping route."""
    response = default_client.get("/ping")
    assert response.status_code == 200
    assert response.mimetype == "text/plain"
    assert response.data == b"pong"


def test_status(default_client):
    """Test status route."""
    response = default_client.get("/status")
    assert response.status_code == 200
    assert response.mimetype == "application/json"

    assert "ready" in response.json
    assert isinstance(response.json["ready"], bool)


def test_ready(default_client, default_config):
    """Test status route."""
    response = default_client.get("/ready")
    assert response.status_code == 200
    assert response.mimetype == "text/plain"

    app2 = Flask(__name__)
    app2.register_blueprint(
        DefaultView(default_config, ready=lambda: False).get_blueprint(),
        url_prefix="/"
    )
    default_client2 = app2.test_client()
    response2 = default_client2.get("/ready")
    assert response2.status_code == 503
    assert response2.mimetype == "text/plain"


def test_identify(default_client, default_config):
    """Test identify route."""
    response = default_client.get("/identify")
    assert response.status_code == 200
    assert response.mimetype == "application/json"

    assert response.json == default_config.CONTAINER_SELF_DESCRIPTION


@pytest.mark.parametrize(
    "endpoint",
    ["ping", "status", "identify"]
)
def test_default_unknown_query_input(default_client, endpoint):
    """
    Test default-blueprint endpoints for acceptance of unknown query.
    """

    response = default_client.get(f"/{endpoint}?query")
    assert response.status_code == 400
    assert response.mimetype == "text/plain"


@pytest.fixture(name="report_tokens")
def _report_token():
    return (Token(False), Token(False), Token(False), Token(False))


@pytest.fixture(name="sample_report")
def _sample_report(report_tokens):
    return {
        "token": report_tokens[1].json,
        "progress": {"status": "completed", "verbose": "done", "numeric": 100}
    }


@pytest.fixture(name="report_app")
def _report_app(report_tokens, sample_report, default_config):
    app = Flask(__name__)
    app.config.from_object(default_config)
    registry = NativeKeyValueStoreAdapter(MemoryStore())
    registry.write(
        report_tokens[0].value,
        JobInfo(JobConfig({}), report_tokens[0]).json
    )
    registry.write(
        report_tokens[1].value,
        JobInfo(JobConfig({}), report_tokens[1], report=sample_report).json | {
            "metadata": {"completed": {"by": "", "datetime": ""}}
        }
    )
    registry.write(
        report_tokens[2].value,
        JobInfo(JobConfig({}), report_tokens[2], report=sample_report).json | {
            "metadata": {"aborted": {"by": "", "datetime": ""}}
        }
    )
    app.register_blueprint(
        ReportView(
            default_config,
            ScalableOrchestrator(lambda config: Job(), registry=registry)
        ).get_blueprint(),
        url_prefix="/"
    )
    return app


@pytest.fixture(name="report_client")
def _report_client(report_app):
    return report_app.test_client()


@pytest.mark.parametrize(
    ("token_id", "status"),
    [
        (0, 503),
        (1, 200),
        (2, 200),
        (3, 404)
    ]
)
def test_report(token_id, status, report_client, report_tokens, sample_report):
    """Test report route."""
    response = report_client.get(
        f"/report?token={report_tokens[token_id].value}"
    )
    assert response.status_code == status
    if status == 200:
        assert response.mimetype == "application/json"
        assert response.json == sample_report
    elif status == 503:
        assert response.mimetype == "application/json"
    else:
        assert response.mimetype == "text/plain"
        print(response.data.decode(encoding="utf-8"))


def test_report_unknown_query_input(report_client):
    """
    Test report-blueprint endpoint for acceptance of unknown query.
    """

    response = report_client.get("/report?query=bad")
    assert response.status_code == 400
    assert response.mimetype == "text/plain"
    print(response.data.decode(encoding="utf-8"))


@pytest.mark.parametrize(
    ("token_id", "status"),
    [
        (0, 503),
        (1, 200),
        (2, 200),
        (3, 404)
    ]
)
def test_progress(
    token_id, status, report_client, report_tokens, sample_report
):
    """Test progress route."""
    response = report_client.get(
        f"/progress?token={report_tokens[token_id].value}"
    )
    assert response.status_code == status
    if status == 200:
        assert response.mimetype == "application/json"
        assert response.json == sample_report["progress"]
    elif status == 503:
        assert response.mimetype == "application/json"
    else:
        assert response.mimetype == "text/plain"
        print(response.data.decode(encoding="utf-8"))
