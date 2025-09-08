"""Definition of the context-type models."""

from typing import Callable, Optional
from dataclasses import dataclass, field
from threading import Event

from .info import JobInfo


@dataclass
class StopContext:
    """Worker stopping-context."""

    stop: Event = field(default_factory=Event)
    stop_on_idle: Event = field(default_factory=Event)
    stopped: Event = field(default_factory=Event)


@dataclass
class AbortContext:
    """Worker abort-context."""

    origin: Optional[str] = field(default_factory=lambda: "unknown")
    reason: Optional[str] = field(default_factory=lambda: "unknown")


@dataclass
class ChildJob:
    """Record class for a child job."""

    id: str
    name: str
    abort: Callable[[JobInfo, AbortContext], None]


@dataclass
class ProcessContext:
    """
    Context for job execution and exchange format with `Worker`-process.
    """

    worker_id: str
    info: JobInfo
    children: list[ChildJob]
    started: bool = False
    completed: bool = False


@dataclass
class JobContext:
    """
    Context for job execution and controls for `Worker`-process to
    handle communication. Gets passed into job command.
    """

    push: Callable[[], None]
    # these are defined in the child-process
    add_child: Optional[Callable[[ChildJob], None]] = None
    remove_child: Optional[Callable[[str], None]] = None
