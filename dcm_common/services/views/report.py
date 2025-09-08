"""
Contains a factory for Blueprints defining the report-endpoint of DCM
web-services.
"""

from flask import Blueprint, Response, jsonify
from data_plumber_http.decorators import flask_handler, flask_args

from dcm_common.services.config import OrchestratedAppConfig
from dcm_common.services.handlers import report_handler
from dcm_common.models import JSONObject
from dcm_common.orchestra.models import Status
from .interface import View


class ReportView(View):
    """
    View-class with routes and logic for the report-related endpoints
    of a DCM web-service.
    """

    NAME = "report"

    def __init__(self, config: OrchestratedAppConfig) -> None:
        View.__init__(self, config)

    def _get_status_code(self, info: JSONObject) -> int:
        if any(
            k in info.get("metadata", {})
            for k in [Status.ABORTED.value, Status.COMPLETED.value]
        ):
            return 200
        return 503

    def configure_bp(self, bp: Blueprint, *args, **kwargs):
        @bp.route("/report", methods=["GET"])
        @flask_handler(handler=report_handler, json=flask_args)
        def get_report(token: str):
            """Get report by job_token."""

            try:
                info = self.config.controller.get_info(token)
            except ValueError:
                return Response(
                    f"Unknown job-token '{token}'.",
                    status=404,
                    mimetype="text/plain",
                )

            # return results
            return jsonify(info.get("report", {})), self._get_status_code(info)

        @bp.route("/progress", methods=["GET"])
        @flask_handler(handler=report_handler, json=flask_args)
        def get_progress(token: str):
            """Get progress by job_token."""
            try:
                info = self.config.controller.get_info(token)
            except ValueError:
                return Response(
                    f"Unknown job-token '{token}'.",
                    status=404,
                    mimetype="text/plain",
                )

            # return the progress of the report
            return (
                jsonify(info.get("report", {}).get("progress", {})),
                self._get_status_code(info),
            )
