"""Flask notification startup-extension."""

from typing import Optional
import sys
from time import time
import signal
from threading import Event

import requests

from dcm_common.daemon import CDaemon
from .common import startup_flask_run, add_signal_handler


def _connected(client):
    """
    Returns `True` if notification service is up and client is currently
    connected.
    """
    try:
        return client.registered() and client.subscribed()
    except requests.exceptions.RequestException:
        return False


def _connect(config, abort: Event):
    """
    Attempts to connect to notification service (blocks until
    connected).
    """
    time0 = time()
    first_try = True
    while not _connected(config.abort_notification_client):
        try:
            config.abort_notification_client.connect()
        except requests.exceptions.RequestException as exc_info:
            first_try = False
            print(
                f"[{int((time()-time0)*100)/100}] Cannot connect to "
                + f"notification service ({type(exc_info).__name__}).",
                file=sys.stderr
            )
            abort.wait(
                config.ORCHESTRATION_ABORT_NOTIFICATIONS_STARTUP_INTERVAL
            )
            if abort.is_set():
                return
    if not first_try:
        print(
            f"[{int((time()-time0)*100)/100}] Successfully connected to "
            + "notification service.",
            file=sys.stderr
        )


def notification(app, config, as_process) -> Optional[CDaemon]:
    """
    Register the `notification` extension.

    This extension runs a daemon until the registration with the
    notification-service is completed.

    If `as_process`, the daemon-startup call is attached to the method
    `app.run` (such that it is automatically executed if the `app` is
    used by running in a separate process via `app.run`). Otherwise, the
    daemon is executed directly, i.e., in the same process from which
    this process has been called.
    """
    if not config.ORCHESTRATION_ABORT_NOTIFICATIONS:
        return None

    abort = Event()
    daemon = CDaemon(
        target=_connect,
        args=(config, abort)
    )
    if as_process:
        # app in separate process via app.run
        startup_flask_run(
            app, (
                lambda: daemon.run(
                    config.ORCHESTRATION_ABORT_NOTIFICATIONS_RECONNECT_INTERVAL
                ),
            )
        )
    else:
        # app native execution
        daemon.run(config.ORCHESTRATION_ABORT_NOTIFICATIONS_RECONNECT_INTERVAL)

    # perform clean shutdown on exit
    def _exit():
        """Stop daemon."""
        abort.set()
        if daemon.active:
            daemon.stop(block=True)

    add_signal_handler(signal.SIGINT, _exit)
    add_signal_handler(signal.SIGTERM, _exit)

    return daemon
