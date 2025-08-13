"""CORS-extension."""

from typing import Optional, Iterable
from threading import Thread, Event
import signal

from flask import Flask

from dcm_common.db.sql.adapter.interface import PooledConnectionAdapter
from .common import (
    startup_flask_run,
    add_signal_handler,
    print_status,
    ExtensionLoaderResult,
    _ExtensionRequirement,
)


def _connect(config, db: PooledConnectionAdapter, abort, result, requirements):
    while not db.pool.is_open:
        if not _ExtensionRequirement.check_requirements(
            requirements,
            "Connecting to database delayed until '{}' is ready.",
        ):
            continue
        try:
            db.pool.init_pool()
        # pylint: disable=broad-exception-caught
        except Exception as exc_info:
            print_status(f"Cannot connect to database: {exc_info}")
            abort.wait(config.DB_ADAPTER_STARTUP_INTERVAL)
            if abort.is_set():
                return
        else:
            result.ready.set()
            print_status("Successfully connected to database.")


def db_loader(
    app: Flask,
    config,
    db: PooledConnectionAdapter,
    as_process,
    requirements: Optional[Iterable[_ExtensionRequirement]] = None,
) -> ExtensionLoaderResult:
    """
    Register the `db` extension.

    This extension runs a loop that attempts to initialize the
    connection-pool in the given `db`-object.

    If `as_process`, the startup call is attached to the method
    `app.run` (such that it is automatically executed if the `app` is
    used by running in a separate process via `app.run`). Otherwise, the
    function is executed directly, i.e., in the same process from which
    this process has been called.
    """

    abort = Event()
    result = ExtensionLoaderResult()
    thread = Thread(
        target=_connect, args=(config, db, abort, result, requirements or [])
    )
    result.data = thread
    if as_process:
        # app in separate process via app.run
        startup_flask_run(app, (thread.start,))
    else:
        # app native execution
        thread.start()

    # perform clean shutdown on exit
    def _exit():
        """Terminate connections."""
        abort.set()
        if db.pool.is_open:
            db.pool.close()

    add_signal_handler(signal.SIGINT, _exit)
    add_signal_handler(signal.SIGTERM, _exit)

    return result
