"""
Contains a factory for Blueprints defining the default-endpoints of DCM
web-services.
"""

from typing import Callable
from flask import Blueprint, Response, jsonify
from data_plumber_http.decorators import flask_handler, flask_args

from dcm_common.services.config import BaseConfig
from dcm_common.services.handlers import no_args_handler
from .interface import View


class DefaultView(View):
    """
    View-class with routes and logic for the default-endpoints
    of a DCM web-service.
    """

    NAME = "default"

    def __init__(
        self,
        config: BaseConfig,
        ready: Callable[[], bool],
    ) -> None:
        View.__init__(self, config)
        self.ready = ready

    def configure_bp(self, bp: Blueprint, *args, **kwargs):

        @bp.route("/ping", methods=["GET"])
        @flask_handler(  # unknown query
            handler=no_args_handler, json=flask_args
        )
        def ping():
            """Handle ping-request."""

            return Response(response="pong", status=200, mimetype="text/plain")

        @bp.route("/ready", methods=["GET"])
        @flask_handler(  # unknown query
            handler=no_args_handler, json=flask_args
        )
        def ready():
            """Handle ready-request."""
            if self.ready():
                return Response("OK", mimetype="text/plain", status=200)
            return Response("BUSY", mimetype="text/plain", status=503)

        @bp.route("/status", methods=["GET"])
        @flask_handler(  # unknown query
            handler=no_args_handler, json=flask_args
        )
        def status():
            """Handle status-request."""

            return jsonify(ready=self.ready()), 200

        @bp.route("/identify", methods=["GET"])
        @flask_handler(  # unknown query
            handler=no_args_handler, json=flask_args
        )
        def identify():
            """Handle identify-request."""

            return jsonify(self.config.CONTAINER_SELF_DESCRIPTION), 200
