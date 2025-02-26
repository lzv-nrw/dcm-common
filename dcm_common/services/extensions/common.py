"""Common code for flask-extensions."""

from typing import Iterable, Callable
import sys
import signal

from flask import Flask


def startup_flask_run(
    app: Flask, tasks: Iterable[Callable[[], None]]
) -> None:
    """Prepends `tasks` to `app.run`."""
    _run = app.run

    def _(*args, **kwargs):
        for task in tasks:
            task()
        _run(*args, **kwargs)

    app.run = _


def add_signal_handler(signum: int, handler: Callable[[], None]) -> None:
    """
    Adds a `handler` for signal `signum` by prepending to the current
    handler.
    """

    original_handler = signal.getsignal(signum)

    def _handler(signum, frame):
        """Stop threads, run original handler, and exit."""
        handler()
        if callable(original_handler):
            original_handler(signum, frame)
        sys.exit(0)

    signal.signal(signum, _handler)
