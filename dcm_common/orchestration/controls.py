"""
This module contains a flask-Blueprint definition for controlling the
orchestrator of a dcm-service.
"""

from typing import Optional
from pathlib import Path

from flask import Blueprint, jsonify, request, Response

from dcm_common.daemon import Daemon
from .scalable_orchestrator import JobConfig, ScalableOrchestrator


def get_orchestration_controls(
    orchestrator: ScalableOrchestrator,
    daemon: Daemon,
    name: Optional[str] = None,
    orchestrator_settings: Optional[dict] = None,
    daemon_settings: Optional[dict] = None,
) -> Blueprint:
    """
    Returns a blueprint with routes for basic control over the given
    `ScalableOrchestrator` and (optionally) an associated `Daemon`.

    Keyword arguments:
    orchestrator -- instance of a `ScalableOrchestrator`
    daemon -- pre-configured `FDaemon` instance
    name -- `Blueprint`'s name
            (default None; uses 'Orchestrator Controls')
    orchestrator_settings -- kwargs for running the orchestrator; only
                             relevant when using PUT with until-idle-arg
                             (default None)
    daemon_settings -- kwargs for running the daemon
                       (default None)
    """
    bp = Blueprint(name or "Orchestrator Controls", __name__)

    @bp.route("/orchestration", methods=["GET"])
    def get():
        """
        Returns status of queue, registry, orchestrator, and daemon.
        """
        return (
            jsonify(
                {
                    "queue": {
                        "size": len(orchestrator.queue.keys()),
                    },
                    "registry": {
                        "size": len(orchestrator.registry.keys()),
                    },
                    "orchestrator": {
                        "ready": orchestrator.ready,
                        "idle": orchestrator.idle,
                        "running": orchestrator.running,
                        "jobs": list(orchestrator.jobs),
                    },
                    "daemon": {
                        "active": daemon.active,
                        "status": daemon.status,
                    },
                }
            ),
            200,
        )

    @bp.route("/orchestration", methods=["PUT"])
    def put():
        """
        Manually start the orchestrator.

        Use the query argument `until-idle` to start as a separate
        thread that automatically terminates when the queue is empty.
        """
        if "until-idle" in request.args:
            try:
                t = orchestrator.as_thread(**(orchestrator_settings or {}))
                t.start()
                orchestrator.stop_on_idle()
            except RuntimeError:
                return Response(
                    "BUSY (already running)", mimetype="text/plain", status=503
                )
        else:
            daemon.run(**(daemon_settings or {}), block=True)
        return Response(
            "OK", mimetype="text/plain", status=200
        )

    @bp.route("/orchestration", methods=["DELETE"])
    def delete():
        """
        Quickly and gracefully shut down the orchestration or abort/kill
        jobs.

        Accepts json (all optional):
        {
            "mode":
                "stop" (default; stop orchestrator + stop daemon)
                | "kill" (kill orchestrator + stop daemon)
                | "abort", (send abort to orchestrator)
            "options": {
                "token": "token.value for abort", (applies only to "abort")
                "reason": "reason for request", (applies only to "kill"/"abort")
                "origin": "origin of request", (applies only to "kill"/"abort")
                "block": true | false
            }
        }
        """
        mode = request.json.get("mode", "stop")
        if mode not in ("stop", "kill", "abort"):
            return f"unknown 'mode={mode}'", 400
        if mode == "abort":
            orchestrator.abort(**request.json.get("options", {}))
            return Response(
                "OK", mimetype="text/plain", status=200
            )
        daemon.stop(True)
        getattr(orchestrator, mode)(**request.json.get("options", {}))
        return Response(
            "OK", mimetype="text/plain", status=200
        )

    return bp


# FIXME: may become deprecated soon
def orchestrator_controls_bp(
    orchestrator: ScalableOrchestrator,
    daemon: Optional[Daemon] = None,
    name: Optional[str] = None,
    default_orchestrator_settings: Optional[dict] = None,
    default_daemon_settings: Optional[dict] = None,
) -> Blueprint:
    """
    Returns a blueprint with routes for control over the given
    `ScalableOrchestrator` and (optionally) an associated `FDaemon` (
    expected to be configured to use `ScalableOrchestrator.as_thread`
    as factory). The `Daemon` is expected to be configured to use the
    orchestrator's default signals.

    Keyword arguments:
    orchestrator -- instance of a `ScalableOrchestrator`
    daemon -- pre-configured `FDaemon` instance
              (default None)
    name -- `Blueprint`'s name
            (default None; uses 'Orchestrator Controls')
    default_orchestrator_settings -- default set of settings that are
                                     used for running orchestrator as
                                     dictionary
                                     (default None)
    default_daemon_settings -- default set of settings that are used for
                               running daemon as dictionary
                               (default None)
    """
    bp = Blueprint(name or "Orchestrator Controls", __name__)

    _default_orchestrator_settings = default_orchestrator_settings or {}
    _default_daemon_settings = default_daemon_settings or {}

    @bp.route("/orchestration", methods=["GET"])
    def get():
        """
        Returns status of queue, registry, orchestrator, and (if
        available) daemon.
        """
        return jsonify(
            {
                "queue": {
                    "size": len(orchestrator.queue.keys()),
                },
                "registry": {
                    "size": len(orchestrator.registry.keys()),
                },
                "orchestrator": {
                    "ready": orchestrator.ready,
                    "idle": orchestrator.idle,
                    "running": orchestrator.running,
                    "jobs": list(orchestrator.jobs)
                }
            } | (
                {
                    "daemon": {
                        "active": daemon.active,
                        "status": daemon.status
                    }
                }
                if daemon else {}
            )
        ), 200

    @bp.route("/orchestration", methods=["PUT"])
    def put():
        """
        Manually start the orchestrator.

        Note that for the given json to take effect, the orchestration
        has to be stopped first. Furthermore, orchestration-settings
        can only be changed when using no `Daemon` at all or a
        `CDaemon`.

        Accepts json to overwrite defaults (all optional):
        {
            "orchestrator": {
                "interval": ...,
                "cwd": ...,
                "daemon": ...
            },
            "daemon": {
                "interval": ...,
                "daemon": ...
            }
        }
        """
        if "cwd" in request.json.get("orchestrator", {}):
            request.json["orchestrator"]["cwd"] = (
                Path(request.json["orchestrator"]["cwd"])
            )
        orchestrator_settings = (
            _default_orchestrator_settings
            | request.json.get("orchestrator", {})
        )
        daemon_settings = (
            _default_daemon_settings
            | request.json.get("daemon", {})
        )
        if daemon:
            daemon.reconfigure(**orchestrator_settings)
        if "until-idle" in request.args:
            try:
                t = orchestrator.as_thread(**orchestrator_settings)
                t.start()
                orchestrator.stop_on_idle()
            except RuntimeError:
                return Response(
                    "BUSY (already running)", mimetype="text/plain", status=503
                )
        else:
            if daemon:
                daemon.run(**daemon_settings, block=True)
            else:
                try:
                    orchestrator.run(**orchestrator_settings)
                except RuntimeError:
                    return Response(
                        "BUSY (already running)", mimetype="text/plain",
                        status=503
                    )
        return Response(
            "OK", mimetype="text/plain", status=200
        )

    @bp.route("/orchestration", methods=["POST"])
    def post():
        """Manually submit given json-JobConfig to queue."""
        token = orchestrator.submit(JobConfig(**request.json))
        return jsonify(token.json), 200

    @bp.route("/orchestration", methods=["DELETE"])
    def delete():
        """
        Quickly and gracefully shut down the orchestration or abort/kill
        jobs.

        Accepts json (all optional):
        {
            "mode":
                "stop" (default; stop orchestrator + stop daemon)
                | "kill" (kill orchestrator + stop daemon)
                | "abort", (send abort to orchestrator)
            "options": {
                "token": "token.value for abort", (applies only to "abort")
                "reason": "reason for request", (applies only to "kill"/"abort")
                "origin": "origin of request", (applies only to "kill"/"abort")
                "block": true | false
            }
        }
        """
        mode = request.json.get("mode", "stop")
        if mode not in ("stop", "kill", "abort"):
            return f"unknown 'mode={mode}'", 400
        if mode == "abort":
            orchestrator.abort(**request.json.get("options", {}))
            return Response(
                "OK", mimetype="text/plain", status=200
            )
        if daemon:
            daemon.stop(True)
        getattr(orchestrator, mode)(**request.json.get("options", {}))
        return Response(
            "OK", mimetype="text/plain", status=200
        )

    return bp
