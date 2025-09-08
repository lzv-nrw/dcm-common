"""
Demo View-class definition
"""

from typing import Optional
from time import sleep
from uuid import uuid4

from flask import Blueprint, jsonify, Response, request
from data_plumber_http.decorators import flask_handler, flask_args, flask_json
from dcm_common import LoggingContext as Context
from dcm_common.plugins.demo import DemoPlugin
from dcm_common.orchestra.models import (
    JobConfig,
    JobInfo,
    JobContext,
    AbortContext,
    ChildJob,
)
from dcm_common import services

from .config import AppConfig
from .handlers import get_demo_handler
from .models import DemoConfig, Report

try:
    import dcm_demo_sdk  # pylint: disable=wrong-import-order
except ImportError as e:
    raise RuntimeError(
        "Cannot import 'dcm_demo_sdk' for DCM Demo Service API. See "
        + "'dcm_common/services/demo/README.md' for build and installation "
        + "instructions."
    ) from e


class DemoAdapter(services.ServiceAdapter):
    """`ServiceAdapter` for the Demo service."""

    _SERVICE_NAME = "Demo"
    _SDK = dcm_demo_sdk

    def _get_api_clients(self):
        client = self._SDK.ApiClient(self._SDK.Configuration(host=self._url))
        return self._SDK.DefaultApi(client), self._SDK.DemoApi(client)

    def _get_api_endpoint(self):
        return self._api_client.demo

    def _get_abort_endpoint(self):
        return self._api_client.abort

    def _build_request_body(self, base_request_body, target):
        return base_request_body

    def success(self, info) -> bool:
        return info.report.get("data", {}).get("success", False)


class DemoView(services.OrchestratedView):
    """View-class for demonstration."""

    NAME = "demo"

    def __init__(self, config: AppConfig, *args, **kwargs) -> None:
        super().__init__(config, *args, **kwargs)

    def register_job_types(self):
        self.config.worker_pool.register_job_type("demo", self.demo, Report)

    def configure_bp(self, bp: Blueprint, *args, **kwargs) -> None:

        @bp.route("/demo", methods=["POST"])
        @flask_handler(
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(
            handler=get_demo_handler(self.config.AVAILABLE_PLUGINS),
            json=flask_json,
        )
        def demo(
            demo: DemoConfig,
            token: Optional[str] = None,
            callback_url: Optional[str] = None,
        ):
            """Submit job."""
            try:
                token = self.config.controller.queue_push(
                    token or str(uuid4()),
                    JobInfo(
                        JobConfig(
                            "demo",
                            original_body=request.json,
                            request_body={
                                "demo": demo.json,
                                "callback_url": callback_url,
                            },
                        ),
                        report=Report(
                            host=request.host_url, args=request.json
                        ),
                    ),
                )
            # pylint: disable=broad-exception-caught
            except Exception as exc_info:
                return Response(
                    f"Submission rejected: {exc_info}",
                    mimetype="text/plain",
                    status=500,
                )
            return jsonify(token.json), 201

        self._register_abort_job(bp, "/demo")

    def demo(self, context: JobContext, info: JobInfo):
        """Job instructions for the '/demo' endpoint."""
        demo_config = DemoConfig.from_json(info.config.request_body["demo"])
        info.report.log.set_default_origin("Demo-Service")

        # set progress info
        info.report.progress.verbose = "preparing.."
        context.push()
        sleep(0.5 * demo_config.duration)

        if demo_config.success_plugin is not None:
            info.report.progress.verbose = (
                f"calling plugin '{demo_config.success_plugin.plugin}' .."
            )
            info.report.log.log(
                context=Context.INFO,
                body=f"Running plugin '{demo_config.success_plugin.plugin}'.",
            )
            context.push()
            plugin: DemoPlugin = self.config.AVAILABLE_PLUGINS[
                demo_config.success_plugin.plugin
            ]
            context = plugin.create_context(
                info.report.progress.create_verbose_update_callback(
                    plugin.display_name
                ),
                context.push,
            )
            result = plugin.get(
                context, **plugin.hydrate(demo_config.success_plugin.args)
            )
            info.report.log.merge(result.log.pick(Context.ERROR))
            context.push()
            success = [result.success]
        else:
            success = [demo_config.success]

        for i, child in enumerate(demo_config.children or []):
            if info.report.children is None:
                info.report.children = {}
            child_name = f"child-{i}@demo"
            # write log
            info.report.progress.verbose = (
                f"making request to '{child.host}' (name '{child_name}').."
            )
            info.report.log.log(
                context=Context.INFO,
                body=f"Making request to '{child.host}' (name '{child_name}').",
            )
            context.push()
            adapter = DemoAdapter(child.host, 0.01, child.timeout)
            info.report.children[child_name] = {}
            child_token = str(uuid4())
            # add to children
            context.add_child(
                ChildJob(
                    child_token,
                    child_name,
                    adapter.get_abort_callback(
                        child_token, child_name, "Demo-Service"
                    ),
                )
            )
            context.push()
            adapter.run(
                child.body | {"token": child_token},
                None,
                child_info := services.APIResult(
                    report=info.report.children[child_name]
                ),
                update_hooks=(lambda data: context.push(),),
                post_submission_hooks=(
                    # post to log
                    lambda token, info=info: (
                        info.report.log.log(
                            Context.INFO,
                            body=f"Got token '{token}' from external service.",
                        ),
                        context.push(),
                    ),
                ),
            )
            context.remove_child(child_token)
            # collect results
            success.append(adapter.success(child_info))
            if not success[-1]:
                info.report.log.log(
                    context=Context.ERROR,
                    body=(
                        f"Request to '{child.host}' returned with an error. "
                        + f"See child-report '{child_name}' for details."
                    ),
                )
            context.push()

        info.report.progress.verbose = "evaluating.."
        context.push()
        sleep(0.5 * demo_config.duration)

        info.report.data.success = all(success)
        context.push()

        # make callback; rely on _run_callback to push progress-update
        info.report.progress.complete()
        self._run_callback(
            context, info, info.config.request_body.get("callback_url")
        )
