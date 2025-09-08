"""Definition of the `WorkerPool`-class."""

from typing import Optional, Mapping, Callable
import threading

from .models import JobContext, Report, JobInfo
from .controller import Controller
from .worker import Worker, WorkerState
from .logging import Logging


class WorkerPool:
    """
    Definition of a `Worker`-pool for vertical scaling.

    Keyword arguments:
    controller -- orchestra-controller
    size -- pool size
            (default 1)
    kwargs -- kwargs for `Worker`s
              (default None)
    """

    def __init__(
        self,
        controller: Controller,
        size: int = 1,
        kwargs: Optional[Mapping] = None,
    ) -> None:
        self.controller = controller
        self._size = size
        if kwargs is not None and "name" in kwargs:
            raise ValueError(
                "WorkerPool does not accept 'name' as argument for Workers."
            )
        self._kwargs = kwargs

        self._workers: Optional[dict[str, Worker]] = None
        self._pool_lock = threading.RLock()
        self._initialized = False

    @property
    def workers(self) -> Optional[dict[str, Worker]]:
        """Returns mapping of workers."""
        if self._workers is None:
            return None
        return self._workers.copy()

    @property
    def size(self) -> int:
        """Returns pool size."""
        return self._size

    @property
    def initialized(self) -> bool:
        """Returns `True` if already initialized."""
        return self._initialized

    @property
    def kwargs(self) -> Mapping:
        """Returns worker kwargs."""
        return self._kwargs.copy()

    def register_job_type(
        self,
        type_: str,
        job_cmd: Callable[[JobContext, JobInfo], None],
        report: type[Report],
    ) -> None:
        """
        Adds the provided type to job and report maps in `Worker`-
        `kwargs`.
        """
        with self._pool_lock:
            if self._kwargs is None:
                self._kwargs = {}
            if "job_factory_map" not in self._kwargs:
                self._kwargs["job_factory_map"] = {}
            if "report_type_map" not in self._kwargs:
                self._kwargs["report_type_map"] = {}

            self._kwargs["job_factory_map"][type_] = job_cmd
            self._kwargs["report_type_map"][type_] = report

    def init(self) -> None:
        """Initialize pool with the current `kwargs`."""
        with self._pool_lock:
            if self._initialized:
                raise RuntimeError("WorkerPool is already initialized.")

            self._workers = {}
            for _ in range(self._size):
                worker = Worker(self.controller, **self._kwargs)
                self._workers[worker.name] = worker
            self._initialized = True

    def start(self, **kwargs) -> None:
        """
        Start workers. If not already initialized, run initialization
        automatically.
        """

        with self._pool_lock:
            if not self._initialized:
                self.init()

            for worker in self._workers.values():
                if worker.state is not WorkerState.STOPPED:
                    continue
                try:
                    worker.start(**kwargs)
                except RuntimeError:
                    Logging.print_to_log(
                        f"Tried to start worker '{worker.name}' that is "
                        + "already running.",
                        Logging.LEVEL_ERROR,
                    )

    def stop(self, **kwargs) -> None:
        """Stop workers."""

        with self._pool_lock:
            if not self._initialized:
                return

            # trigger stop immediately for all workers
            for worker in self._workers.values():
                worker.stop(False)

            # respect block/timeout
            for worker in self._workers.values():
                worker.stop(**kwargs)

    def stop_on_idle(self, **kwargs) -> None:
        """Stop workers."""

        with self._pool_lock:
            if not self._initialized:
                return

            # trigger stop immediately for all workers
            for worker in self._workers.values():
                worker.stop_on_idle(False)

            # respect block/timeout
            for worker in self._workers.values():
                worker.stop_on_idle(**kwargs)

    def kill(
        self,
        origin: Optional[str] = None,
        reason: Optional[str] = None,
        **kwargs,
    ):
        """Kills all workers."""

        with self._pool_lock:
            if not self._initialized:
                return

            # trigger kill immediately for all workers
            for worker in self._workers.values():
                worker.kill(origin, reason)

            # respect block/timeout
            for worker in self._workers.values():
                worker.stop(**kwargs)
