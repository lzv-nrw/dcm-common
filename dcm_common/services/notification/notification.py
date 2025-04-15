"""
This module contains an implementation of the Notification API using
flask.
"""

from typing import Optional, Mapping
from threading import Lock
import os
import sys
from dataclasses import dataclass, field
from enum import Enum
from time import time
from uuid import uuid4

from flask import (
    Flask, Blueprint, Response, jsonify, request,
)
import requests

from dcm_common.db import KeyValueStore, MemoryStore


class HTTPMethod(Enum):
    """HTTP method"""
    GET = "get"
    POST = "post"
    PUT = "put"
    DELETE = "delete"
    OPTIONS = "options"
    PATCH = "patch"


class Topic:
    """Notification topic configuration."""

    def __init__(
        self,
        path: str,
        method: str | HTTPMethod,
        status_ok: int,
        db: Optional[KeyValueStore] = None
    ):
        self.path = path
        if isinstance(method, str):
            self.method = HTTPMethod(method.lower())
        else:
            self.method = method
        self.status_ok = status_ok
        self.db = db or MemoryStore()
        self.db_lock = Lock()

    @property
    def json(self):
        """Returns data as JSON-able dictionary."""
        return {
            "path": self.path,
            "method": self.method.name,
            "statusOk": self.status_ok,
            "db": _load_db_config_json(self.db)
        }


@dataclass
class Subscriber:
    """Subscriber configuration."""

    base_url: str
    token: str = field(default_factory=lambda: str(uuid4()))

    @property
    def json(self):
        """Returns data as JSON-able dictionary."""
        return {
            "baseUrl": self.base_url,
            "token": self.token,
        }


def _load_db_config_json(db: KeyValueStore) -> dict:
    """Returns json-able configuration for given `KeyValueStore`."""
    config = {
        "backend": db.__class__.__name__
    }
    if hasattr(db, "dir"):
        config["database"] = {"dir": str(db.dir.resolve())}
    return config


def _load_configuration_json(registry, topics, timeout, cors):
    """Returns configuration data as JSON-able dictionary."""
    config = {
        "registry": _load_db_config_json(registry),
        "topics": {
            k: v.json for k, v in topics.items()
        },
        "timeout": timeout,
        "cors": cors is not None
    }
    return config


def bp_factory(
    registry: KeyValueStore, topics: Mapping[str, Topic], timeout: float = 1.0,
    debug: bool = False, name: Optional[str] = None, cors=None
) -> Blueprint:
    """
    Returns a flask-Blueprint with endpoints for the Notification API.
    """

    bp = Blueprint(name or "notification", __name__)

    config = _load_configuration_json(registry, topics, timeout, cors)
    time0 = time()

    registry_lock = Lock()

    def _get_debug_prefix():
        return "\033[33mDEBUG\033[0m " + f"[{str(time() - time0)[:12]}] "

    def _print_debug(msg):
        print(_get_debug_prefix() + msg, file=sys.stderr)

    @bp.route("/config", methods=["GET"])
    def get_config():
        return jsonify(config), 200

    @bp.route("/", methods=["OPTIONS"])
    def get_topics():
        return jsonify(list(topics.keys())), 200

    @bp.route("/ip", methods=["GET"])
    def get_ip():
        return jsonify({"ip": request.remote_addr}), 200

    @bp.route("/registration", methods=["GET"], provide_automatic_options=False)
    def registration_status():
        """Returns registration status."""
        if "token" not in request.args:
            return Response("Missing token", mimetype="text/plain", status=400)
        with registry_lock:
            if registry.read(request.args["token"]) is None:
                return Response("Not registered", mimetype="text/plain", status=204)
            return Response("OK", mimetype="text/plain", status=200)

    @bp.route("/registration", methods=["POST"], provide_automatic_options=False)
    def register():
        """Register new `Subscriber`."""
        json = request.get_json(silent=True)
        if "baseUrl" not in json:
            return Response("Missing url", mimetype="text/plain", status=400)
        with registry_lock:
            while (subscriber := Subscriber(json["baseUrl"])).token in registry.keys():
                pass
            registry.write(subscriber.token, subscriber.json)
        if debug:
            _print_debug(
                f"user '{subscriber.token}' registered with url '{subscriber.base_url}'"
            )
        return jsonify(subscriber.json), 200

    @bp.route("/registration", methods=["DELETE"], provide_automatic_options=False)
    def deregister():
        """Revoke registration for `Subscriber`."""
        if "token" not in request.args:
            return Response("Missing token", mimetype="text/plain", status=400)
        with registry_lock:
            if request.args["token"] not in registry.keys():
                if debug:
                    _print_debug(
                        f"unknown user '{request.args['token']}' tried to "
                        + f"revoke their registration ({request.remote_addr})"
                    )
                return Response(
                    f"Unknown token '{request.args['token']}'",
                    mimetype="text/plain",
                    status=404
                )
            registry.delete(request.args["token"])
        for topic in topics.values():
            with topic.db_lock:
                topic.db.delete(request.args["token"])
        if debug:
            _print_debug(
                f"user '{request.args['token']}' revoked their registration"
            )
        return Response("OK", mimetype="text/plain", status=200)

    @bp.route("/registration", methods=["OPTIONS"], provide_automatic_options=False)
    def list_registrations():
        """Returns a list of registered users."""
        with registry_lock:
            return jsonify(
                [registry.read(token) for token in registry.keys()]
            ), 200

    @bp.route("/subscription", methods=["GET"], provide_automatic_options=False)
    def subscription_status():
        """Returns subscription status."""
        if "token" not in request.args:
            return Response("Missing token", mimetype="text/plain", status=400)
        if "topic" not in request.args:
            return Response("Missing topic", mimetype="text/plain", status=400)
        if request.args["topic"] not in topics:
            return Response(
                f"Unknown topic '{request.args['topic']}'",
                mimetype="text/plain",
                status=404
            )
        with registry_lock:
            if request.args["token"] not in registry.keys():
                return Response(
                    f"Unknown token '{request.args['token']}'",
                    mimetype="text/plain",
                    status=404
                )
        with topics[request.args["topic"]].db_lock:
            if request.args["token"] in topics[request.args["topic"]].db.keys():
                return Response("OK", mimetype="text/plain", status=200)
            return Response("Not subscribed", mimetype="text/plain", status=204)

    @bp.route("/subscription", methods=["POST"], provide_automatic_options=False)
    def subscribe():
        """Subscribe to given topic."""
        if "token" not in request.args:
            return Response("Missing token", mimetype="text/plain", status=400)
        if "topic" not in request.args:
            return Response("Missing topic", mimetype="text/plain", status=400)
        if request.args["topic"] not in topics:
            return Response(
                f"Unknown topic '{request.args['topic']}'",
                mimetype="text/plain",
                status=404
            )
        with registry_lock:
            if request.args["token"] not in registry.keys():
                if debug:
                    _print_debug(
                        f"unknown user '{request.args['token']}' tried to "
                        + f"make a subscription for '{request.args['topic']}' "
                        + f"({request.remote_addr})"
                    )
                return Response(
                    f"Unknown token '{request.args['token']}'",
                    mimetype="text/plain",
                    status=404
                )
        with topics[request.args["topic"]].db_lock:
            topics[request.args["topic"]].db.write(
                request.args["token"], request.args["token"]
            )
        if debug:
            _print_debug(
                f"user '{request.args['token']}' made a subscription for "
                + f"'{request.args['topic']}'"
            )
        return Response("OK", mimetype="text/plain", status=200)

    @bp.route("/subscription", methods=["DELETE"], provide_automatic_options=False)
    def unsubscribe():
        """Revoke subscription for given topic."""
        if "token" not in request.args:
            return Response("Missing token", mimetype="text/plain", status=400)
        if "topic" not in request.args:
            return Response("Missing topic", mimetype="text/plain", status=400)
        if request.args["topic"] not in topics:
            return Response(
                f"Unknown topic '{request.args['topic']}'",
                mimetype="text/plain",
                status=404
            )
        with registry_lock:
            if request.args["token"] not in registry.keys():
                if debug:
                    _print_debug(
                        f"unknown user '{request.args['token']}' tried to "
                        + f"revoke subscription for '{request.args['topic']}' "
                        + f"({request.remote_addr})"
                    )
                return Response(
                    f"Unknown token '{request.args['token']}'",
                    mimetype="text/plain",
                    status=404
                )
        with topics[request.args["topic"]].db_lock:
            topics[request.args["topic"]].db.delete(request.args["token"])
        if debug:
            _print_debug(
                f"user '{request.args['token']}' cancelled subscription for "
                + f"'{request.args['topic']}'"
            )
        return Response("OK", mimetype="text/plain", status=200)

    @bp.route("/subscription", methods=["OPTIONS"], provide_automatic_options=False)
    def list_subscriptions():
        """Returns a list of subscribed users for the given topic."""
        if "topic" not in request.args:
            return Response("Missing topic", mimetype="text/plain", status=400)
        if request.args["topic"] not in topics:
            return Response(
                f"Unknown topic '{request.args['topic']}'",
                mimetype="text/plain",
                status=404
            )
        with topics[request.args["topic"]].db_lock:
            return jsonify(
                [
                    topics[request.args["topic"]].db.read(token)
                    for token in topics[request.args["topic"]].db.keys()
                ]
            ), 200

    @bp.route("/notify", methods=["POST"])
    def notify():
        """Broadcast to subscribers in given topic."""
        if "topic" not in request.args:
            return Response("Missing topic", mimetype="text/plain", status=400)
        if request.args["topic"] not in topics:
            return Response(
                f"Unknown topic '{request.args['topic']}'",
                mimetype="text/plain",
                status=404
            )
        topic = topics[request.args["topic"]]

        # process request data
        json = request.get_json(silent=True) or {}
        if "skip" not in json:
            json["skip"] = None
        request_data = {}
        if topic.method is not HTTPMethod.GET:
            request_data["json"] = json.get("json", {})
        request_data["params"] = json.get("query", {})
        request_data["headers"] = json.get("headers", {})

        request_id = str(uuid4())[0:8]
        if debug:
            _print_debug(
                f"got broadcast request for topic '{request.args['topic']}' "
                + f"({request.remote_addr}); request data: {request_data}; "
                + f"request id: {request_id}"
            )

        # collect info from db
        with topic.db_lock:
            tokens = topic.db.keys()
        with registry_lock:
            subscribers = [registry.read(token) for token in tokens]

        # run broadcast
        bad_subscriptions = []
        for subscriber in subscribers:
            if subscriber["token"] == json["skip"]:
                continue
            url = subscriber["baseUrl"] + topic.path
            if debug:
                _print_debug(
                    f"forwarding request '{request_id}' to '{url}' "
                    + f"(user '{subscriber['token']}')"
                )
            try:
                response = requests.request(
                    topic.method.value, url, **request_data, timeout=timeout
                )
            except requests.exceptions.RequestException as e:
                bad_subscriptions.append(subscriber)
                if debug:
                    _print_debug(
                        f"got '{type(e).__name__}' from '{url}'-"
                        + f"{topic.method.name}: {e}"
                    )
            else:
                if response.status_code != topic.status_ok:
                    if debug:
                        _print_debug(
                            f"bad response code {response.status_code}"
                            + f" from '{url}'-{topic.method.name} "
                            + f"(expected {topic.status_ok}): "
                            + f"{response.text}"
                        )
                    bad_subscriptions.append(subscriber)

        # delete problematic subscriptions + registration
        if debug and bad_subscriptions:
            _print_debug(
                "deleting bad registrations and their subscriptions: "
                + ", ".join(
                    subscriber["token"] for subscriber in bad_subscriptions
                )
            )
        for subscriber in bad_subscriptions:
            for topic in topics.values():
                with topic.db_lock:
                    topic.db.delete(subscriber["token"])
            with registry_lock:
                registry.delete(subscriber["token"])
        return Response("OK", mimetype="text/plain", status=200)

    return bp


def app_factory(
    registry: KeyValueStore, topics: Mapping[str, Topic],
    timeout: float = 1.0, debug: bool = False, name: Optional[str] = None
) -> Flask:
    """
    Returns a flask-app object that implements the Notification API
    with user-registry `registry` and topic-configuration as given by
    `topics`.
    """

    app = Flask(name or __name__)

    cors = None
    if os.environ.get("ALLOW_CORS") == "1":
        try:
            from flask_cors import CORS
        except ImportError:
            print(
                "WARNING: Missing package 'Flask-CORS' for 'ALLOW_CORS=1'. "
                + "CORS-requests will not work.",
                file=sys.stderr
            )
        else:
            cors = CORS(app)

    app.register_blueprint(
        bp_factory(registry, topics, timeout, debug, "notifications", cors),
        url_prefix="/"
    )

    return app
