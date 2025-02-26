"""Flask orchestration startup-extension."""

from time import time
import sys
import signal
from threading import Event

from dcm_common.daemon import CDaemon
from .common import startup_flask_run, add_signal_handler
from .notification import _connected


def _startup(config, orchestrator, abort: Event):
    """
    Attempts to start orchestrator (if required, blocks until
    notification-service is ready).
    """
    time0 = time()
    if orchestrator.running:
        return
    first_try = True
    while (
        config.ORCHESTRATION_ABORT_NOTIFICATIONS
        and not _connected(config.abort_notification_client)
    ):
        first_try = False
        print(
            f"[{int((time()-time0)*100)/100}] Orchestrator startup delayed "
            + "until notification service-subscription is ready.",
            file=sys.stderr
        )
        abort.wait(config.ORCHESTRATION_ABORT_NOTIFICATIONS_STARTUP_INTERVAL)
        if abort.is_set():
            return
    orchestrator.run(
        cwd=config.FS_MOUNT_POINT,
        interval=config.ORCHESTRATION_ORCHESTRATOR_INTERVAL
    )
    if not first_try:
        print(
            f"[{int((time()-time0)*100)/100}] Orchestrator is ready.",
            file=sys.stderr
        )


def orchestration(app, config, orchestrator, name, as_process) -> CDaemon:
    """
    Register the `orchestration` extension.

    If `as_process`, the daemon-startup call is attached to the method
    `app.run` (such that it is automatically executed if the `app` is
    used by running in a separate process via `app.run`). Otherwise, the
    daemon is executed directly, i.e., in the same process from which
    this process has been called.
    """
    abort = Event()
    daemon = CDaemon(
        target=_startup, kwargs={
            "config": config,
            "orchestrator": orchestrator,
            "abort": abort
        }
    )
    if config.ORCHESTRATION_AT_STARTUP:
        if as_process:
            # app in separate process via app.run
            startup_flask_run(
                app, (
                    lambda: daemon.run(config.ORCHESTRATION_DAEMON_INTERVAL),
                )
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

    return daemon
