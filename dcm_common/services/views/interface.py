"""
View-class interface definition
"""

from typing import Optional, Mapping, Callable
import abc
from time import time, sleep

import requests
from flask import Blueprint, Response
from data_plumber_http.decorators import flask_handler, flask_args, flask_json

from dcm_common import services, LoggingContext
from dcm_common.services.config import BaseConfig


class View(metaclass=abc.ABCMeta):
    """
    Interface for the definition of a Flask-view-function and its
    related components.

    Keyword arguments:
    config -- `BaseConfig`-object
    """

    NAME = "undefined"

    def __init__(
        self,
        config: BaseConfig,
    ) -> None:
        self.config = config

    @abc.abstractmethod
    def configure_bp(self, bp: Blueprint, *args, **kwargs) -> None:
        """
        Configures and adds routes to the given `bp`.
        """
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method "
            + "'configure_bp'."
        )

    def get_blueprint(
        self,
        *args,
        name: Optional[str] = None,
        import_name: Optional[str] = None,
        **kwargs,
    ) -> Blueprint:
        """
        Returns `Blueprint` instance. All positional and keyword args
        are passed into the call to `View.configure_bp`.

        Keyword arguments:
        name -- `Blueprint`'s name
                (default None; uses `self.NAME`)
        import_name -- `Blueprint`'s import-name
                       (default None; uses `__name__`)
        """
        bp = Blueprint(name or self.NAME, import_name or __name__)
        self.configure_bp(bp, *args, **kwargs)
        return bp


class OrchestratedView(View, metaclass=abc.ABCMeta):
    """
    Interface for `View`s that utilized the orchestra-subpackage. An
    implementation requires a definition of the `register_job_type`-
    method which should register all view-related job types with the
    `WorkerPool`. It is expected to be used in conjunction with an
    `OrchestratedAppConfig`.
    """

    # pylint: disable=abstract-method

    @abc.abstractmethod
    def register_job_types(self) -> None:
        """Register all view-related job types with the `WorkerPool`."""
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method "
            + "'register_job_types'."
        )

    def _run_callback(self, context, info, callback_url):
        if callback_url is not None:
            # make callback
            response = requests.post(
                callback_url, json=info.token.json, timeout=10
            )

            # if unexpected response code, write to log
            if response.status_code == 200:
                info.report.log.log(
                    LoggingContext.ERROR,
                    body=f"Made callback to '{callback_url}'.",
                )
            else:
                info.report.log.log(
                    LoggingContext.ERROR,
                    body=(
                        f"Failed callback to '{callback_url}'. Expected "
                        + f"status '200' but got '{response.status_code}'."
                    ),
                )
            context.push()

    def _register_abort_job(
        self,
        bp: Blueprint,
        rule: str,
        options: Optional[Mapping] = None,
        *,
        post_abort_hook: Optional[Callable[[str], None]] = None,
    ) -> None:
        """
        Can be used to register a default abort-route for the given
        `Blueprint` at the path `rule`.

        Keyword arguments:
        bp -- `Blueprint`-instance to be configured
        rule -- url path for this route (e.g. '/import')
        options -- additional werkzeug-options
        post_abort_hook -- hook that gets executed after the default
                           abort-routine is completed; gets passed the
                           token as positional argument
                           (default None)
        """

        @bp.route(rule, methods=["DELETE"], **(options or {}))
        @flask_handler(
            handler=services.abort_query_handler,
            json=flask_args,
        )
        @flask_handler(
            handler=services.abort_body_handler,
            json=flask_json,
        )
        def abort(
            token: str,
            origin: Optional[str] = None,
            reason: Optional[str] = None,
        ):
            """Abort job."""
            self.config.controller.message_push(token, "abort", origin, reason)

            try:
                self.config.controller.get_status(token)
            except ValueError as exc_info:
                if "Unknown job token" in str(exc_info):
                    # job no longer exists or has never existed
                    # either way, there is nothing to do
                    if post_abort_hook is not None:
                        post_abort_hook(token)
                    return Response("OK", mimetype="text/plain", status=200)
                return Response(
                    f"FAILED: {exc_info}", mimetype="text/plain", status=500
                )

            time0 = time()
            while (
                self.config.controller.get_status(token)
                not in ("completed", "aborted", "failed")
                and time() - time0 < self.config.ORCHESTRA_ABORT_TIMEOUT
            ):
                sleep(self.config.ORCHESTRA_WORKER_INTERVAL)
            if post_abort_hook is not None:
                post_abort_hook(token)
            if self.config.controller.get_status(token) in (
                "completed",
                "aborted",
                "failed",
            ):
                return Response("OK", mimetype="text/plain", status=200)
            return Response("FAILED", mimetype="text/plain", status=500)
