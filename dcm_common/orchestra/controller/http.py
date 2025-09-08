"""Definition of an http-based `orchestra.Controller`."""

from typing import Optional, Mapping, Any
from time import sleep
from uuid import uuid4
from datetime import datetime
import socket

from flask import Blueprint, request, Response, jsonify
import requests

from ..models import JobInfo, Token, Lock, Message
from .interface import Controller
from .sqlite import SQLiteController
from ..logging import Logging


class HTTPController(Controller):
    """
    An orchestra-controller working over the HTTP-API defined by
    `get_http_controller_bp`.

    Keyword arguments:
    base_url -- base url for controller API
    timeout -- request timeout in seconds
               (default 1)
    name -- optional name tag for this controller (used in logging)
            (default None; generates unique name from hostname)
    max_retries -- number of retries if an HTTP-error occurs during a
                   request
                   (default 1)
    retry_interval -- interval between retries in seconds
                      (default 0)
    request_kwargs -- additional kwargs that are passed when calling
                      `requests.request`
                      (default None)
    """

    def __init__(
        self,
        base_url: str,
        timeout: float = 1,
        name: Optional[str] = None,
        max_retries: int = 1,
        retry_interval: float = 0,
        request_kwargs: Optional[Mapping] = None,
    ):
        self.base_url = base_url
        self.timeout = timeout
        if name is None:
            self._name = (
                f"Controller-{socket.gethostname()}-{str(uuid4())[:8]}"
            )
        else:
            self._name = name
        self.max_retries = max_retries
        self.retry_interval = retry_interval
        self.request_kwargs = request_kwargs or {}

    @property
    def name(self):
        """Returns controller name."""
        return self._name

    def _run(
        self,
        method: str,
        endpoint: str,
        json: Optional[dict] = None,
        *,
        skip_retry: bool = False,
    ) -> requests.Response:
        """
        Runs the given api_request while respecting timeout and retry-behavior
        """
        for i in range(self.max_retries * (0 if skip_retry else 1) + 1):
            try:
                return requests.request(
                    method,
                    self.base_url + endpoint,
                    json=json,
                    timeout=self.timeout,
                    **self.request_kwargs,
                )
            except requests.exceptions.RequestException as exc_info:
                Logging.print_to_log(
                    f"Controller '{self._name}' failed to make a {method}-"
                    + f"request to '{self.base_url + endpoint}'"
                    + (
                        ""
                        if skip_retry
                        else f" (attempt {i + 1}/{self.max_retries + 1})"
                    )
                    + f": {exc_info}",
                    Logging.LEVEL_ERROR,
                )
                if skip_retry or i >= self.max_retries:
                    raise exc_info
                sleep(self.retry_interval)

    def queue_push(self, token: str, info: Mapping | JobInfo) -> Token:
        """
        Add job to queue, returns `Token` if successful or already
        existing or `None` otherwise.

        If `info` is not passed as `JobInfo`, adds the `token` and
        `produced`-metadata before submission.
        """
        r = self._run(
            "POST",
            "/queue/push",
            {
                "token": token,
                "info": (
                    info
                    if isinstance(info, Mapping) or info is None
                    else info.json
                ),
            },
        )
        if r.status_code == 200:
            return Token.from_json(r.json())
        raise ValueError(r.text)

    def queue_pop(self, name: str) -> Optional[Lock]:
        """Request a lock on a job from the queue."""
        try:
            r = self._run(
                "POST",
                "/queue/pop",
                {"name": self._name},
                skip_retry=True,
            )
        except requests.exceptions.RequestException:
            return None
        if r.status_code == 200:
            return Lock.from_json(r.json())
        return None

    def release_lock(self, lock_id: str) -> None:
        """Releases a lock on a job from the queue."""
        r = self._run(
            "DELETE",
            "/lock",
            {"id": lock_id},
        )
        if r.status_code == 200:
            return
        raise ValueError(r.text)

    def refresh_lock(self, lock_id: str) -> Lock:
        """
        Refreshes a lock on a job from the queue. Raises `ValueError` if
        not successful.
        """
        r = self._run(
            "PUT",
            "/lock",
            {"id": lock_id},
        )
        if r.status_code == 200:
            return Lock.from_json(r.json())
        raise ValueError(r.text)

    def get_token(self, token: str) -> Token:
        """Fetch token-data from registry."""
        r = self._run(
            "GET",
            f"/registry/token?token={token}",
            {"token": token},
        )
        if r.status_code == 200:
            return Token.from_json(r.json())
        raise ValueError(r.text)

    def get_info(self, token: str) -> Any:
        """Fetch info from registry as JSON."""
        r = self._run(
            "GET",
            f"/registry/info?token={token}",
        )
        if r.status_code == 200:
            return r.json()
        raise ValueError(r.text)

    def get_status(self, token: str) -> str:
        """Fetch status from registry."""
        r = self._run(
            "GET",
            f"/registry/status?token={token}",
        )
        if r.status_code == 200:
            return r.text
        raise ValueError(r.text)

    def registry_push(
        self,
        lock_id: str,
        *,
        status: Optional[str] = None,
        info: Optional[Mapping | JobInfo] = None,
    ) -> None:
        """Push new data to registry."""
        r = self._run(
            "PUT",
            "/registry",
            {
                "lockId": lock_id,
                "status": status,
                "info": (
                    info
                    if isinstance(info, Mapping) or info is None
                    else info.json
                ),
            },
        )
        if r.status_code == 200:
            return
        raise ValueError(r.text)

    def message_push(
        self, token: str, instruction: str, origin: str, content: str
    ) -> None:
        """Posts message."""
        r = self._run(
            "POST",
            "/messages",
            {
                "token": token,
                "instruction": instruction,
                "origin": origin,
                "content": content,
            },
        )
        if r.status_code == 200:
            return
        raise ValueError(r.text)

    def message_get(self, since: Optional[datetime | int]) -> list[Message]:
        """Returns a list of relevant messages."""
        if since is None:
            _since = 0
        elif isinstance(since, datetime):
            _since = int(since.timestamp())
        else:
            _since = since
        r = self._run(
            "GET",
            f"/messages?since={_since}",
        )
        if r.status_code == 200:
            return [Message.from_json(m) for m in r.json()]
        raise ValueError(r.text)


def get_http_controller_bp(
    controller: SQLiteController,
    name: Optional[str] = None,
    import_name: Optional[str] = None,
) -> Blueprint:
    """
    Returns Flask-blueprint that implements the controller-interface via
    an HTTP-API.
    """
    bp = Blueprint(name or "orchestra-controller-api", import_name or __name__)

    # pylint: disable=broad-exception-caught

    @bp.route("/queue/push", methods=["POST"])
    def queue_push():
        """Push to queue."""
        try:
            token = controller.queue_push(
                request.json["token"], JobInfo.from_json(request.json["info"])
            )
        except Exception as exc_info:
            return Response(
                f"Failed submission to queue: {exc_info}",
                mimetype="text/plain",
                status=500,
            )
        return jsonify(token.json), 200

    @bp.route("/queue/pop", methods=["POST"])
    def queue_pop():
        """Pop from queue."""
        try:
            lock = controller.queue_pop(request.json["name"])
        except Exception as exc_info:
            return Response(
                f"Failed to pop queue: {exc_info}",
                mimetype="text/plain",
                status=500,
            )
        if lock is None:
            return Response("Empty queue.", status=204, mimetype="text/plain")
        return jsonify(lock.json), 200

    @bp.route("/lock", methods=["DELETE"])
    def release_lock():
        """Release lock."""
        try:
            controller.release_lock(request.json["id"])
        except Exception as exc_info:
            return Response(
                f"Failed to release lock: {exc_info}",
                mimetype="text/plain",
                status=500,
            )
        return Response("OK", status=200, mimetype="text/plain")

    @bp.route("/lock", methods=["PUT"])
    def refresh_lock():
        """Refresh lock."""
        try:
            lock = controller.refresh_lock(request.json["id"])
        except Exception as exc_info:
            return Response(
                f"Failed to refresh lock: {exc_info}",
                mimetype="text/plain",
                status=500,
            )
        return jsonify(lock.json), 200

    @bp.route("/registry/token", methods=["GET"])
    def get_token():
        """Get token."""
        try:
            token = controller.get_token(request.args["token"])
        except Exception as exc_info:
            return Response(
                f"Failed to get token: {exc_info}",
                mimetype="text/plain",
                status=500,
            )
        return jsonify(token.json), 200

    @bp.route("/registry/info", methods=["GET"])
    def get_info():
        """Get info."""
        try:
            info = controller.get_info(request.args["token"])
        except Exception as exc_info:
            return Response(
                f"Failed to get info: {exc_info}",
                mimetype="text/plain",
                status=500,
            )
        return jsonify(info), 200

    @bp.route("/registry/status", methods=["GET"])
    def get_status():
        """Get status."""
        try:
            status = controller.get_status(request.args["token"])
        except Exception as exc_info:
            return Response(
                f"Failed to get status: {exc_info}",
                status=500,
                mimetype="text/plain",
            )
        return Response(status, mimetype="text/plain", status=200)

    @bp.route("/registry", methods=["PUT"])
    def registry_push():
        """Push to registry."""
        try:
            controller.registry_push(
                request.json["lockId"],
                status=request.json.get("status"),
                info=request.json.get("info"),
            )
        except Exception as exc_info:
            return Response(
                f"Failed to push to registry: {exc_info}",
                status=500,
                mimetype="text/plain",
            )
        return Response("OK", mimetype="text/plain", status=200)

    @bp.route("/messages", methods=["POST"])
    def message_push():
        """Push message."""
        try:
            controller.message_push(
                request.json["token"],
                request.json["instruction"],
                request.json["origin"],
                request.json["content"],
            )
        except Exception as exc_info:
            return Response(
                f"Failed to push message: {exc_info}",
                status=500,
                mimetype="text/plain",
            )
        return Response("OK", mimetype="text/plain", status=200)

    @bp.route("/messages", methods=["GET"])
    def message_get():
        """Get message."""
        try:
            messages = controller.message_get(int(request.args.get("since")))
        except Exception as exc_info:
            return Response(
                f"Failed to push message: {exc_info}",
                status=500,
                mimetype="text/plain",
            )
        return jsonify([m.json for m in messages]), 200

    return bp
