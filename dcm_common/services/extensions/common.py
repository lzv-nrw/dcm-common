"""Common code for flask-extensions."""

from typing import Iterable, Callable

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
