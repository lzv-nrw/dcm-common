"""Flask orchestration startup-extension."""

from typing import Optional, Iterable
import signal
from threading import Event

from dcm_common.daemon import CDaemon
from .common import (
    print_status,
    startup_flask_run,
    add_signal_handler,
    ExtensionLoaderResult,
    _ExtensionRequirement,
    ExtensionConditionRequirement,
)
from .notification import _connected


def _startup(
    config,
    orchestrator,
    abort: Event,
    result: ExtensionLoaderResult,
    requirements=Iterable[_ExtensionRequirement],
):
    """
    Attempts to start orchestrator (if required, blocks until
    requirements are met) or stops orchestrator if requirements are no
    longer met.
    """
    first_try = orchestrator.running
    if orchestrator.running:
        if _ExtensionRequirement.check_requirements(
            requirements, "Orchestrator startup delayed until '{}' is ready."
        ):
            return
        orchestrator.stop(True)
        print_status("Halting orchestrator due to unmet requirements.")
        result.ready.clear()

    while True:
        if _ExtensionRequirement.check_requirements(
            requirements, "Orchestrator startup delayed until '{}' is ready."
        ):
            break
        first_try = False
        abort.wait(config.ORCHESTRATION_ABORT_NOTIFICATIONS_STARTUP_INTERVAL)
        if abort.is_set():
            return
    kwargs = {"interval": config.ORCHESTRATION_ORCHESTRATOR_INTERVAL}
    if hasattr(config, "FS_MOUNT_POINT"):
        kwargs["cwd"] = config.FS_MOUNT_POINT
    orchestrator.run(**kwargs)
    result.ready.set()
    if not first_try:
        print_status("Orchestrator is ready.")


# FIXME: drop legacy support
def orchestration(app, config, orchestrator, name, as_process) -> CDaemon:
    """
    Register the `orchestration` extension.

    If `as_process`, the daemon-startup call is attached to the method
    `app.run` (such that it is automatically executed if the `app` is
    used by running in a separate process via `app.run`). Otherwise, the
    daemon is executed directly, i.e., in the same process from which
    this process has been called.
    """
    return orchestration_loader(
        app,
        config,
        orchestrator,
        name,
        as_process,
        [
            ExtensionConditionRequirement(
                lambda: not config.ORCHESTRATION_ABORT_NOTIFICATIONS
                or _connected(config.abort_notification_client),
                "connection to notification-service",
            )
        ],
    ).data


def orchestration_loader(
    app,
    config,
    orchestrator,
    name,
    as_process,
    requirements: Optional[Iterable[_ExtensionRequirement]] = None,
) -> ExtensionLoaderResult:
    """
    Register the `orchestration` extension.

    If `as_process`, the daemon-startup call is attached to the method
    `app.run` (such that it is automatically executed if the `app` is
    used by running in a separate process via `app.run`). Otherwise, the
    daemon is executed directly, i.e., in the same process from which
    this process has been called.
    """
    abort = Event()
    result = ExtensionLoaderResult()
    daemon = CDaemon(
        target=_startup,
        kwargs={
            "config": config,
            "orchestrator": orchestrator,
            "abort": abort,
            "result": result,
            "requirements": requirements or [],
        },
    )
    result.data = daemon
    if config.ORCHESTRATION_AT_STARTUP:
        if as_process:
            # app in separate process via app.run
            startup_flask_run(
                app,
                (lambda: daemon.run(config.ORCHESTRATION_DAEMON_INTERVAL),),
            )
        else:
            # app native execution
            daemon.run(config.ORCHESTRATION_DAEMON_INTERVAL)

    # perform clean shutdown on exit
    def _exit():
        """Stop daemon and orchestrator."""
        abort.set()
        if daemon.active:
            daemon.stop(block=True)
        if orchestrator.running:
            orchestrator.kill(
                origin=name,
                reason="Parent shutdown.",
                re_queue=True,
                block=True,
            )

    add_signal_handler(signal.SIGINT, _exit)
    add_signal_handler(signal.SIGTERM, _exit)

    return result
