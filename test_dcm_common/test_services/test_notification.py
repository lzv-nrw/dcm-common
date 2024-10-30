"""
Test module for the Notification API (flask-blueprint).
"""

import os
from time import sleep, time
from uuid import uuid4
import json

from flask import Response, request
import requests
import pytest

from dcm_common.db import MemoryStore
from dcm_common.services.notification import (
    app_factory, HTTPMethod, Topic, NotificationAPIClient
)
from dcm_common.services.tests import run_service, external_service


@pytest.fixture(name="test_client_and_db")  # used for tests of blueprint
def _test_client_and_db():
    db = MemoryStore()
    app = app_factory(
        db, {
            "topicA": Topic("/a", HTTPMethod.GET, 200),
            "topicB": Topic("/b", HTTPMethod.POST, 404)
        }, 1.0, os.environ.get("NOTIFICATION_DEBUG_MODE", "0") == "1"
    )
    return app.test_client(), db


@pytest.fixture(name="notification_app")  # used for API-client tests
def _notification_app():
    return app_factory(
        MemoryStore(), {
            "topicA": Topic("/a", HTTPMethod.GET, 200),
            "topicB": Topic("/b", HTTPMethod.POST, 404)
        }, 1.0, os.environ.get("NOTIFICATION_DEBUG_MODE", "0") == "1"
    )


def test_config(test_client_and_db):
    """Test GET-/config endpoint."""
    client, _ = test_client_and_db

    response = client.get("/config")
    assert response.mimetype == "application/json"
    assert response.status_code == 200
    assert response.json == {
        "registry": {
            "backend": "MemoryStore"
        },
        "topics": {
            "topicA": {
                "method": "GET",
                "path": "/a",
                "statusOk": 200,
                "db": {
                    "backend": "MemoryStore"
                }
            },
            "topicB": {
                "method": "POST",
                "path": "/b",
                "statusOk": 404,
                "db": {
                    "backend": "MemoryStore"
                }
            }
        },
        "timeout": 1.0,
        "cors": False,
    }


def test_config_topics(test_client_and_db):
    """Test OPTIONS-/ endpoint."""
    client, _ = test_client_and_db

    response = client.options("/")
    assert response.mimetype == "application/json"
    assert response.status_code == 200
    assert response.json == ["topicA", "topicB"]


def test_ip(test_client_and_db):
    """Test GET-/ip endpoint."""
    client, _ = test_client_and_db

    response = client.get("/ip")
    assert response.mimetype == "application/json"
    assert response.status_code == 200
    assert response.json == {"ip": "127.0.0.1"}


def test_register(test_client_and_db):
    """Test /registration endpoints."""
    client, _ = test_client_and_db

    # empty
    assert client.get("/registration").status_code == 400
    assert client.get("/registration?token=unknown").status_code == 204
    assert client.options("/registration").status_code == 200
    assert len(client.options("/registration").json) == 0

    # register first
    assert client.post("/registration", json={}).status_code == 400
    registration = client.post("/registration", json={"baseUrl": "url"})
    assert registration.status_code == 200
    assert "token" in registration.json
    assert registration.json["baseUrl"] == "url"
    registrations = client.options("/registration").json
    assert len(registrations) == 1
    assert registration.json in registrations

    # register second
    registration2 = client.post("/registration", json={"baseUrl": "url"})
    registrations = client.options("/registration").json
    assert len(registrations) == 2
    assert (
        registration.json in registrations
        and registration2.json in registrations
    )

    # deregister
    assert client.delete("/registration").status_code == 400
    assert client.delete("/registration?token=unknown").status_code == 404
    assert client.delete(f"/registration?token={registration.json['token']}").status_code == 200
    registrations = client.options("/registration").json
    assert len(registrations) == 1
    assert registration2.json in registrations


def test_subscription(test_client_and_db):
    """Test /subscription endpoints."""
    client, _ = test_client_and_db

    # empty
    assert client.get("/subscription").status_code == 400
    assert client.get("/subscription?token=unknown").status_code == 400
    assert client.get("/subscription?topic=topicA").status_code == 400
    assert client.get("/subscription?token=unknown&topic=topicA").status_code == 404
    assert client.options("/subscription?topic=topicA").status_code == 200
    assert len(client.options("/subscription?topic=topicA").json) == 0

    # make first subscription
    registration = client.post("/registration", json={"baseUrl": "url"}).json
    assert client.post("/subscription").status_code == 400
    assert client.post("/subscription?token=unknown").status_code == 400
    assert client.post("/subscription?topic=topicA").status_code == 400
    assert client.post("/subscription?token=unknown&topic=topicA").status_code == 404
    assert client.post(f"/subscription?token={registration['token']}&topic=topicC").status_code == 404
    assert client.post(f"/subscription?token={registration['token']}&topic=topicA").status_code == 200
    subscriptions = client.options("/subscription?topic=topicA").json
    assert len(subscriptions) == 1
    assert registration["token"] in subscriptions

    # repeat subscription (without effect)
    assert client.post(f"/subscription?token={registration['token']}&topic=topicA").status_code == 200
    assert len(client.options("/subscription?topic=topicA").json) == 1

    # make second subscription
    registration2 = client.post("/registration", json={"baseUrl": "url"}).json
    assert client.post(f"/subscription?token={registration2['token']}&topic=topicA").status_code == 200
    subscriptions = client.options("/subscription?topic=topicA").json
    assert len(subscriptions) == 2
    assert registration["token"] in subscriptions and registration2["token"] in subscriptions

    # unsubscribe
    assert client.delete("/subscription").status_code == 400
    assert client.delete("/subscription?token=unknown").status_code == 400
    assert client.delete("/subscription?topic=topicA").status_code == 400
    assert client.delete("/subscription?token=unknown&topic=topicA").status_code == 404
    assert client.delete(f"/subscription?token={registration['token']}&topic=topicC").status_code == 404
    assert client.delete(f"/subscription?token={registration['token']}&topic=topicA").status_code == 200
    subscriptions = client.options("/subscription?topic=topicA").json
    assert len(subscriptions) == 1
    assert registration2["token"] in subscriptions

    # other topic
    assert len(client.options("/subscription?topic=topicB").json) == 0
    assert client.post(f"/subscription?token={registration['token']}&topic=topicB").status_code == 200
    assert len(client.options("/subscription?topic=topicB").json) == 1


def test_deregistering_with_subscriptions(test_client_and_db):
    """Test automatic cancellation of subscriptions on deregistering."""
    client, _ = test_client_and_db

    registration = client.post("/registration", json={"baseUrl": "url"}).json
    client.post(f"/subscription?token={registration['token']}&topic=topicA")
    assert len(client.options("/subscription?topic=topicA").json) == 1
    client.delete(f"/registration?token={registration['token']}")
    assert len(client.options("/subscription?topic=topicA").json) == 0


def test_notify_minimal(test_client_and_db, temporary_directory, run_service):
    """Test /notify-POST (submit) endpoint."""

    client, _ = test_client_and_db
    data = {
        "query": {
            "arg1": "value1",
            "arg2": True,
            "arg3": 1,
            "arg4": 0.1,
        },
        "json": {
            "field1": "value1",
            "field2": True,
            "field3": 1,
            "field4": 0.1,
            "field5": ["element1", True],
            "field6": {"more": "data"},
        },
        "headers": {"Custom-Header": "value"}
    }

    output_a = temporary_directory / str(uuid4())
    output_b = temporary_directory / str(uuid4())
    output_a.mkdir(parents=True, exist_ok=False)
    output_b.mkdir(parents=True, exist_ok=False)
    def view(output, msg, status):
        (output / "query.json").write_text(
            json.dumps(request.args)
        )
        (output / "json.json").write_text(
            json.dumps(request.get_json(silent=True))
        )
        (output / "headers.json").write_text(
            json.dumps(dict(request.headers))
        )
        return Response(msg, mimetype="text/plain", status=status)
    run_service(
        routes=[
            ("/a", lambda: view(output_a, "OK", 200), ["GET"]),
            ("/b", lambda: view(output_b, "OK", 404), ["POST"]),
        ],
        port=5000
    )

    registration = client.post("/registration", json={"baseUrl": "http://localhost:5000"}).json
    client.post(f"/subscription?token={registration['token']}&topic=topicA")
    client.post(f"/subscription?token={registration['token']}&topic=topicB")

    response = client.post("/notify?topic=topicA", json=data)
    assert response.mimetype == "text/plain"
    assert response.status_code == 200
    assert (output_a / "query.json").is_file()
    assert (output_a / "json.json").is_file()
    assert (output_a / "headers.json").is_file()
    assert json.loads((output_a / "query.json").read_text()) == {
        k: str(v) for k, v in data["query"].items()
    }
    assert json.loads((output_a / "json.json").read_text()) is None
    assert (
        json.loads((output_a / "headers.json").read_text())["Custom-Header"]
        == data["headers"]["Custom-Header"]
    )

    client.post("/notify?topic=topicB", json=data)
    assert (output_b / "query.json").is_file()
    assert (output_b / "json.json").is_file()
    assert (output_b / "headers.json").is_file()
    assert json.loads((output_b / "query.json").read_text()) == {
        k: str(v) for k, v in data["query"].items()
    }
    assert json.loads((output_b / "json.json").read_text()) == data["json"]
    assert (
        json.loads((output_b / "headers.json").read_text())["Custom-Header"]
        == data["headers"]["Custom-Header"]
    )


def test_notify_connection_error(test_client_and_db):
    """Test /notify-POST (submit) endpoint with connection error."""

    client, _ = test_client_and_db

    registration = client.post("/registration", json={"baseUrl": "http://localhost:5000"}).json
    client.post(f"/subscription?token={registration['token']}&topic=topicA")
    client.post("/notify?topic=topicA")
    assert client.get(f"/registration?token={registration['token']}").status_code == 204


def test_notify_wrong_method(test_client_and_db, run_service):
    """Test /notify-POST (submit) endpoint with wrong method."""

    client, _ = test_client_and_db
    run_service(
        routes=[
            ("/a", lambda: Response("OK", status=200), ["POST"]),
        ],
        port=5000
    )

    registration = client.post("/registration", json={"baseUrl": "http://localhost:5000"}).json
    client.post(f"/subscription?token={registration['token']}&topic=topicA")

    client.post("/notify?topic=topicA")
    assert client.get(f"/registration?token={registration['token']}").status_code == 204


def test_notify_timeout(run_service):
    """Test /notify-POST (submit) endpoint with timeout error."""

    duration = 0.1
    client = app_factory(
        MemoryStore(), {"topicA": Topic("/a", HTTPMethod.GET, 200)},
        duration, os.environ.get("NOTIFICATION_DEBUG_MODE", "0") == "1"
    ).test_client()
    run_service(
        routes=[
            ("/a", lambda: sleep(2*duration), ["GET"]),
        ],
        port=5000
    )

    registration = client.post("/registration", json={"baseUrl": "http://localhost:5000"}).json
    client.post(f"/subscription?token={registration['token']}&topic=topicA")
    time0 = time()
    client.post("/notify?topic=topicA")
    assert time() - time0 >= duration
    assert client.get(f"/registration?token={registration['token']}").status_code == 204


def test_notify_unexpected_response(test_client_and_db, run_service):
    """Test /notify-POST (submit) endpoint with unexpected response."""

    client, _ = test_client_and_db
    run_service(
        routes=[
            ("/a", lambda: Response("OK", status=201), ["GET"]),
        ],
        port=5000
    )

    registration = client.post("/registration", json={"baseUrl": "http://localhost:5000"}).json
    client.post(f"/subscription?token={registration['token']}&topic=topicA")

    client.post("/notify?topic=topicA")
    assert client.get(f"/registration?token={registration['token']}").status_code == 204


def test_notify_multiple_subscribers(
    temporary_directory, test_client_and_db, run_service
):
    """Test /notify-POST (submit) endpoint with multiple subscribers."""

    client, _ = test_client_and_db

    output_1 = temporary_directory / str(uuid4())
    output_2 = temporary_directory / str(uuid4())
    output_1.mkdir(parents=True, exist_ok=False)
    output_2.mkdir(parents=True, exist_ok=False)
    def view(output, msg, status):
        (output / request.args["filename"]).touch()
        return Response(msg, mimetype="text/plain", status=status)
    run_service(
        routes=[
            ("/a", lambda: view(output_1, "OK", 200), ["GET"]),
        ],
        port=5000
    )
    run_service(
        routes=[
            ("/a", lambda: view(output_2, "OK", 200), ["GET"]),
        ],
        port=5001
    )

    registration1 = client.post("/registration", json={"baseUrl": "http://localhost:5000"}).json
    registration2 = client.post("/registration", json={"baseUrl": "http://localhost:5001"}).json
    client.post(f"/subscription?token={registration1['token']}&topic=topicA")
    client.post(f"/subscription?token={registration2['token']}&topic=topicA")

    client.post(
        "/notify?topic=topicA", json={"query": {"filename": "test-file"}}
    )

    assert (output_1 / "test-file").is_file()
    assert (output_2 / "test-file").is_file()


def test_notify_skipped_origin(
    temporary_directory, test_client_and_db, run_service
):
    """Test /notify-POST (submit) endpoint with skipped origin."""

    client, _ = test_client_and_db

    output = temporary_directory / str(uuid4())
    output.mkdir(parents=True, exist_ok=False)
    def view(output, msg, status):
        (output / request.args["filename"]).touch()
        return Response(msg, mimetype="text/plain", status=status)
    run_service(
        routes=[("/a", lambda: view(output, "OK", 200), ["GET"]),],
        port=5000
    )

    registration = client.post("/registration", json={"baseUrl": "http://localhost:5000"}).json
    client.post(f"/subscription?token={registration['token']}&topic=topicA")

    client.post(
        "/notify?topic=topicA", json={
            "query": {"filename": "test-file"},
            "skip": registration["token"]
        }
    )
    assert not (output / "test-file").is_file()
    client.post(
        "/notify?topic=topicA", json={
            "query": {"filename": "test-file"},
        }
    )
    assert (output / "test-file").is_file()


def test_client_constructor(notification_app, run_service):
    """Test constructor of class `NotificationAPIClient`."""

    with pytest.raises(ValueError):
        NotificationAPIClient("http://localhost:8080", "topicA")

    run_service(notification_app, port=8080)

    client = NotificationAPIClient("http://localhost:8080", "topicA")
    assert "127.0.0.1" in client.callback_url


def test_client_ip(notification_app, run_service):
    """Test method `ip` of class `NotificationAPIClient`."""
    assert NotificationAPIClient.get_ip("http://localhost:8080") is None
    run_service(notification_app, port=8080)
    assert NotificationAPIClient.get_ip("http://localhost:8080") == "127.0.0.1"


def test_client_config(notification_app, run_service):
    """Test method `get_config` of class `NotificationAPIClient`."""
    run_service(notification_app, port=8080)

    assert (
        NotificationAPIClient("http://localhost:8080", "topicA").get_config()
        == {
            "registry": {
                "backend": "MemoryStore"
            },
            "topics": {
                "topicA": {
                    "method": "GET",
                    "path": "/a",
                    "statusOk": 200,
                    "db": {
                        "backend": "MemoryStore"
                    }
                },
                "topicB": {
                    "method": "POST",
                    "path": "/b",
                    "statusOk": 404,
                    "db": {
                        "backend": "MemoryStore"
                    }
                }
            },
            "timeout": 1.0,
            "cors": False,
        }
    )


def test_client_registration_subscription(notification_app, run_service):
    """
    Test registration/subscription-methods of class
    `NotificationAPIClient`.
    """
    run_service(notification_app, port=8080)

    client = NotificationAPIClient("http://localhost:8080", "topicA")
    assert len(client.list_registered()) == 0
    assert not client.registered()
    client.register()
    assert client.registered()
    assert len(client.list_registered()) == 1
    assert len(client.list_subscribed()) == 0
    assert not client.subscribed()
    client.subscribe()
    assert client.subscribed()
    assert len(client.list_subscribed()) == 1
    client.unsubscribe()
    assert len(client.list_subscribed()) == 0
    client.subscribe()
    client.deregister()
    assert len(client.list_subscribed()) == 0
    assert len(client.list_registered()) == 0

    client2 = NotificationAPIClient(
        "http://localhost:8080", "topicB", register=True
    )
    assert client2.registered()
    assert client2.subscribed()

    assert client.token != client2.token
    assert client.topic != client2.topic


def test_client_notify(notification_app, run_service, temporary_directory):
    """Test method `notify` of class `NotificationAPIClient`."""
    run_service(notification_app, port=8080)
    output = temporary_directory / str(uuid4())
    output.mkdir(parents=True, exist_ok=False)
    def view(output, msg, status):
        (output / request.args["filename"]).touch()
        return Response(msg, mimetype="text/plain", status=status)
    run_service(
        routes=[
            ("/a", lambda: view(output, "OK", 200), ["GET"]),
        ],
        port=5000
    )

    client = NotificationAPIClient(
        "http://localhost:8080", "topicA",
        callback_url="http://localhost:5000",
        register=True
    )

    assert not (output / "test-file").is_file()
    client.notify(
        query={"filename": "test-file"},
        skip_self=True
    )
    assert not (output / "test-file").is_file()
    client.notify(
        query={"filename": "test-file"},
        skip_self=False
    )
    assert (output / "test-file").is_file()
