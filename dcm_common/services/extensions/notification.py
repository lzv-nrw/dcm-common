"""Flask notification startup-extension."""

from typing import Optional, Iterable
import signal
from threading import Event

import requests

from dcm_common.daemon import CDaemon
from .common import (
    print_status,
    startup_flask_run,
    add_signal_handler,
    ExtensionLoaderResult,
    _ExtensionRequirement,
)


def setup_connection_test_callback_with_state(initial_state: bool):
    """
    Returns function to check connected-status.
    """
    current_state = {"ok": initial_state}

    def __connected(client):
        """
        Returns `True` if notification service is up and client is currently
        connected.
        """
        try:
            current_state["ok"] = client.registered() and client.subscribed()
        except requests.exceptions.RequestException as exc_info:
            if current_state["ok"]:
                print_status(
                    "Lost connection to notification service "
                    + f"({type(exc_info).__name__})."
                )
            current_state["ok"] = False
        return current_state["ok"]

    return __connected


_connected = setup_connection_test_callback_with_state(False)


def _connect(
    config,
    abort: Event,
    result: ExtensionLoaderResult,
    requirements: Iterable[_ExtensionRequirement],
):
    """
    Attempts to (re-)connect to notification service (blocks until
    connected).
    """
    while not _connected(config.abort_notification_client):
        if not _ExtensionRequirement.check_requirements(
            requirements,
            "Connecting to notifications delayed until '{}' is ready.",
        ):
            continue
        try:
            config.abort_notification_client.connect()
        except requests.exceptions.RequestException as exc_info:
            result.ready.clear()
            print_status(
                "Cannot connect to notification service "
                + f"({type(exc_info).__name__})."
            )
            abort.wait(
                config.ORCHESTRATION_ABORT_NOTIFICATIONS_RECONNECT_INTERVAL
            )
            if abort.is_set():
                return
        else:
            result.ready.set()
            print_status("Successfully connected to notification service.")


# FIXME: drop legacy support
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
    return notifications_loader(app, config, as_process).data


def notifications_loader(
    app,
    config,
    as_process,
    requirements: Optional[Iterable[_ExtensionRequirement]] = None,
) -> ExtensionLoaderResult:
    """
    Register the `notifications` extension.

    This extension runs a daemon that automatically (re-)connects to the
    notification-service.

    If `as_process`, the daemon-startup call is attached to the method
    `app.run` (such that it is automatically executed if the `app` is
    used by running in a separate process via `app.run`). Otherwise, the
    daemon is executed directly, i.e., in the same process from which
    this process has been called.
    """
    if not config.ORCHESTRATION_ABORT_NOTIFICATIONS:
        return ExtensionLoaderResult(None).toggle()

    abort = Event()
    result = ExtensionLoaderResult()
    daemon = CDaemon(
        target=_connect, args=(config, abort, result, requirements or [])
    )
    result.data = daemon
    if as_process:
        # app in separate process via app.run
        startup_flask_run(
            app,
            (
                lambda: daemon.run(
                    config.ORCHESTRATION_ABORT_NOTIFICATIONS_RECONNECT_INTERVAL
                ),
            ),
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

    return result
