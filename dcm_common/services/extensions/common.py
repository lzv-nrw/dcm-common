"""Common code for flask-extensions."""

from typing import Optional, Iterable, Callable, Any
import sys
from dataclasses import dataclass, field
from threading import Event
from time import time
import signal

from flask import Flask


@dataclass
class ExtensionLoaderResult:
    """Record-class for extension loader-functions."""

    data: Optional[Any] = None
    ready: Event = field(default_factory=Event)

    def toggle(self) -> "ExtensionLoaderResult":
        """
        Helper for initializing object in a ready-state: Toggles ready
        flag and return self.
        """
        if self.ready.is_set():
            self.ready.clear()
        else:
            self.ready.set()
        return self


class _ExtensionRequirement:
    @staticmethod
    def check_requirements(
        requirements: Iterable["_ExtensionRequirement"],
        fmt: Optional[str] = None,
    ) -> bool:
        unmet = [r for r in requirements if not r.met]
        if not unmet:
            return True
        if fmt:
            for r in unmet:
                print_status(
                    fmt.format(r.name)
                )
        return False


@dataclass
class ExtensionEventRequirement(_ExtensionRequirement):
    """Record-class for extension requirement based on an `Event`."""

    ready: Event
    name: str

    @property
    def met(self) -> bool:
        """Returns `True` if `ready` is set."""
        return self.ready.is_set()


@dataclass
class ExtensionConditionRequirement(_ExtensionRequirement):
    """
    Record-class for extension requirement based on a condition
    (callback).
    """

    ready: Callable[[], bool]
    name: str

    @property
    def met(self) -> bool:
        """Returns return value of `ready`."""
        return self.ready()


class PrintStatusSettings:
    """Settings for the `print_status` helper."""
    time0 = time()
    silent = False
    file = sys.stderr


def print_status(msg: str) -> None:
    """Prints `msg` in common format to stderr."""
    if not PrintStatusSettings.silent:
        print(
            f"[{int((time() - PrintStatusSettings.time0)*100)/100}] {msg}",
            file=PrintStatusSettings.file,
        )


def startup_flask_run(app: Flask, tasks: Iterable[Callable[[], None]]) -> None:
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
