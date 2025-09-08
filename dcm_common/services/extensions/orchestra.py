"""Flask orchestra startup-extension."""

from typing import Optional, Iterable
import signal
from threading import Event

from dcm_common.daemon import CDaemon
from dcm_common.orchestra import WorkerPool
from dcm_common.orchestra.worker import WorkerState
from .common import (
    print_status,
    startup_flask_run,
    add_signal_handler,
    ExtensionLoaderResult,
    _ExtensionRequirement,
)


def _startup(
    config,
    worker_pool: WorkerPool,
    abort: Event,
    result: ExtensionLoaderResult,
    requirements=Iterable[_ExtensionRequirement],
):
    """
    Attempts to start orchestrator (if required, blocks until
    requirements are met) or stops orchestrator if requirements are no
    longer met.
    """
    if _ExtensionRequirement.check_requirements(
        requirements, "Missing orchestra-requirement '{}'."
    ):
        for worker in (worker_pool.workers or {}).values():
            if worker.state is WorkerState.STOPPED:
                print_status(f"Starting worker '{worker.name}'.")
        worker_pool.start(interval=config.ORCHESTRA_WORKER_INTERVAL)
        result.ready.set()
        return

    result.ready.clear()
    if any(
        worker.state is not WorkerState.STOPPED
        for worker in (worker_pool.workers or {}).values()
    ):
        print_status("Halting worker pool until requirements are met.")
    worker_pool.stop(block=True)


def orchestra_loader(
    app,
    config,
    worker_pool: WorkerPool,
    name,
    as_process,
    requirements: Optional[Iterable[_ExtensionRequirement]] = None,
) -> ExtensionLoaderResult:
    """
    Register the `orchestra` extension.

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
            "worker_pool": worker_pool,
            "abort": abort,
            "result": result,
            "requirements": requirements or [],
        },
    )
    result.data = daemon
    if config.ORCHESTRA_AT_STARTUP:
        if as_process:
            # app in separate process via app.run
            startup_flask_run(
                app,
                (lambda: daemon.run(config.ORCHESTRA_DAEMON_INTERVAL),),
            )
        else:
            # app native execution
            daemon.run(config.ORCHESTRA_DAEMON_INTERVAL)

    # perform clean shutdown on exit
    def _exit(
        *args,
        block: bool = True,
        stop: bool = False,
        stop_on_idle: bool = False,
        timeout: Optional[float] = None,
        **kwargs,
    ):
        """Stop daemon and orchestrator."""
        abort.set()
        if daemon.active:
            # needs to block here to prevent immediate restart
            daemon.stop(block=True)
        if stop_on_idle:
            worker_pool.stop_on_idle(block=block, timeout=timeout)
        elif stop:
            worker_pool.stop(block=block, timeout=timeout)
        worker_pool.kill(
            origin=name,
            reason="Parent shutdown.",
            block=block,
            timeout=timeout,
        )

    result.stop = _exit

    add_signal_handler(signal.SIGINT, _exit)
    add_signal_handler(signal.SIGTERM, _exit)

    return result
