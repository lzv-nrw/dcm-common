"""
View-class interface definition
"""

from typing import Optional, Mapping, Callable
import abc

from flask import Blueprint, request, Response
from data_plumber_http.decorators import flask_handler, flask_args, flask_json

from dcm_common.models import Report
from dcm_common.orchestration import (
    JobConfig, Job, ScalableOrchestrator
)
from dcm_common.db import KeyValueStoreAdapter
from dcm_common.services.config import (
    BaseConfig, OrchestratedAppConfig
)
from dcm_common import services
from dcm_common.services.hooks import (
    pre_queue_hook_factory, pre_exec_hook_factory
)


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
        *args, name: Optional[str] = None, import_name: Optional[str] = None,
        **kwargs
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
        bp = Blueprint(
            name or self.NAME,
            import_name or __name__
        )
        self.configure_bp(bp, *args, **kwargs)
        return bp


class JobFactory(metaclass=abc.ABCMeta):
    """
    Interface for the definition of a job factory class and its
    related components.
    """
    def get_job(self, config: JobConfig) -> Job:
        """
        Returns a `Job` based on `config`.

        Keyword arguments:
        config -- configuration details for this job
        """
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method "
            + "'get_job'."
        )


class OrchestratedView(View, JobFactory):  # pylint: disable=abstract-method
    """
    An `OrchestratedView` combines `View` and `JobFactory` to bundle up
    all view-related information. The constructor creates a pre-
    configured `ScalableOrchestrator` that is exposed as
    `OrchestratedView.orchestrator`.

    Alternatively, an existing `ScalableOrchestrator` can be passed into
    the constructor. In this orchestrator the `OrchestratedView.get_job`
    is registered as factory under `context`. Similarly, the default
    hooks are registered using that `context`.

    Keyword arguments:
    config -- `OrchestratedAppConfig`-object
    orchestrator -- `ScalableOrchestrator`-object
                    (default None; uses the following args to initialize
                    default orchestrator)
    context -- only relevant when passing `orchestrator`; used to
               register associated job-factory and default hooks
               (default None)
    report_type -- `Report`-class used to initialize `JobInfo`-objects
                   in the orchestrator
                   (default None; uses dcm-common.models base-report
                   type)
    queue -- override for queue-adapter
             (default None; uses `config`'s value)
    registry -- override for registry-adapter
                (default None; uses `config`'s value)
    """
    def __init__(
        self,
        config: OrchestratedAppConfig,
        orchestrator: Optional[ScalableOrchestrator] = None,
        context: Optional[str] = None,
        report_type: Optional[type[Report]] = None,
        queue: Optional[KeyValueStoreAdapter] = None,
        registry: Optional[KeyValueStoreAdapter] = None,
    ) -> None:
        View.__init__(self, config)
        JobFactory.__init__(self)
        if orchestrator:
            self.orchestrator = orchestrator
            self.orchestrator.register_factory(context, self.get_job)
            self.orchestrator.register_queue_hooks(
                context, {
                    "pre-queue":
                        pre_queue_hook_factory(report_type or Report)
                }
            )
            self.orchestrator.register_exec_hooks(
                context, {
                    "pre-execution":
                        pre_exec_hook_factory(report_type or Report)
                }
            )
        else:
            self.orchestrator = ScalableOrchestrator(
                self.get_job,
                queue or config.queue,
                registry or config.registry,
                queue_hooks={
                    "pre-queue":
                        pre_queue_hook_factory(report_type or Report)
                },
                exec_hooks={
                    "pre-execution":
                        pre_exec_hook_factory(report_type or Report)
                },
                _debug=config.ORCHESTRATION_DEBUG
            )

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
            token: str, broadcast: bool = True, re_queue: bool = False,
            origin: Optional[str] = None, reason: Optional[str] = None
        ):
            """Abort job."""
            if not re_queue:
                self.orchestrator.dequeue(token, origin=origin, reason=reason)
            r = None
            if broadcast and self.config.ORCHESTRATION_ABORT_NOTIFICATIONS:
                try:
                    self.config.abort_notification_client.notify(
                        query={
                            "token": token,
                            "broadcast": "false",
                            "re-queue": "true" if re_queue else "false"
                        },
                        json=request.json
                    )
                # pylint: disable=broad-exception-caught
                except Exception as exc_info:
                    r = Response(
                        "error while making abort-request to notification "
                        + f"service for token '{token}': {exc_info}",
                        mimetype="text/plain",
                        status=502
                    )
            self.orchestrator.abort(
                token,
                origin=origin,
                reason=reason,
                block=True,
                re_queue=re_queue
            )

            if post_abort_hook is not None:
                post_abort_hook(token)

            return r or Response(
                f"successfully aborted '{token}'",
                mimetype="text/plain",
                status=200
            )
