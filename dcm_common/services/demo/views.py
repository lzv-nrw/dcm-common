"""
Demo View-class definition
"""

from typing import Optional
from time import sleep

from flask import Blueprint, jsonify
from data_plumber_http.decorators import flask_handler, flask_args, flask_json
from dcm_common import LoggingContext as Context
from dcm_common.orchestration import JobConfig, Job, Children
from dcm_common import services

from .config import AppConfig
from .handlers import demo_handler
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

    def _build_request_body(self, base_request_body, target):
        return base_request_body

    def success(self, info) -> bool:
        return info.report.get("data", {}).get("success", False)


class DemoView(services.OrchestratedView):
    """View-class for demonstration."""
    NAME = "demo"

    def __init__(
        self, config: AppConfig, *args, **kwargs
    ) -> None:
        super().__init__(config, *args, **kwargs)

    def configure_bp(self, bp: Blueprint, *args, **kwargs) -> None:
        @bp.route("/demo", methods=["POST"])
        @flask_handler(
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(
            handler=demo_handler,
            json=flask_json,
        )
        def demo(
            demo: DemoConfig,
            callback_url: Optional[str] = None
        ):
            """Submit job."""
            token = self.orchestrator.submit(
                JobConfig(
                    request_body={
                        "demo": demo.json,
                        "callback_url": callback_url
                    },
                    context=self.NAME
                )
            )
            return jsonify(token.json), 201

        self._register_abort_job(bp, "/demo")

    def get_job(self, config: JobConfig) -> Job:
        return Job(
            cmd=lambda push, data, children: self.demo(
                push, data, children, DemoConfig.from_json(
                    config.request_body["demo"]
                )
            ),
            hooks={
                "startup": services.default_startup_hook,
                "success": services.default_success_hook,
                "fail": services.default_fail_hook,
                "abort": services.default_abort_hook,
                "completion": services.termination_callback_hook_factory(
                    config.request_body.get("callback_url", None),
                )
            },
            name="Demo Service"
        )

    def demo(
        self, push, report: Report, children: Children,
        demo_config: DemoConfig,
    ):
        """
        Job instructions for the '/demo' endpoint.

        Orchestration standard-arguments:
        push -- (orchestration-standard) push `report` to host process
        report -- (orchestration-standard) common report-object shared
                  via `push`
        children -- (orchestration-standard) `ChildJob`-registry shared
                    via `push`

        Keyword arguments:
        demo_config -- a `DemoConfig`-config
        """

        # set progress info
        report.progress.verbose = "preparing.."
        push()
        sleep(0.5*demo_config.duration)

        success = [demo_config.success]
        for i, child in enumerate(demo_config.children or []):
            if report.children is None:
                report.children = {}
            child_id = f"child-{i}@demo"
            # write log
            report.progress.verbose = (
                f"making request to '{child.host}' (id '{child_id}').."
            )
            report.log.log(
                context=Context.INFO,
                body=f"Making request to '{child.host}' (id '{child_id}')."
            )
            push()
            # initialize adapter, allocate and link child-report, make call
            report.children[child_id] = {}
            adapter = DemoAdapter(child.host, 0.01, child.timeout)
            adapter.run(
                child.body, None, info := services.APIResult(
                    report=report.children[child_id]
                ),
                post_submission_hooks=(
                    # link to children
                    children.link_ex(
                        url=child.host,
                        abort_path="/demo",
                        tag=f"demo-child-{i}",
                        child_id=child_id,
                        post_link_hook=push
                    ),
                    # post to log
                    lambda token: (
                        report.log.log(
                            Context.INFO,
                            body=f"Got token '{token}' from external service."
                        ),
                        push()
                    ),
                ),
                update_hooks=(lambda data: push(),)
            )
            children.remove(f"demo-child-{i}")
            # collect results
            success.append(adapter.success(info))
            if not success[-1]:
                report.log.log(
                    context=Context.ERROR,
                    body=(
                        f"Request to '{child.host}' returned with an error. "
                        + f"See child-report '{child_id}' for details."
                    )
                )
            push()

        report.progress.verbose = "evaluating.."
        push()
        sleep(0.5*demo_config.duration)

        report.data.success = all(success)
        push()
