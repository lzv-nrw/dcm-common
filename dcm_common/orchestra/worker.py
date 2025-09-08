"""Definition of an `orchestra.Worker`."""

from typing import Optional, Mapping, Callable
import sys
import os
import threading
from time import sleep, time
from enum import Enum
import socket
from uuid import uuid4
import traceback
from datetime import datetime, timedelta
import signal
from functools import partial

from dcm_common import LoggingContext
from .controller import Controller
from .models import (
    Report,
    JobInfo,
    JobConfig,
    Lock,
    Instruction,
    StopContext,
    AbortContext,
    ProcessContext,
    JobContext,
    ChildJob,
)
from .dilled import DilledProcess, DilledConnection, DilledPipe, DillIgnore
from .logging import Logging


class WorkerState(Enum):
    """Enum for states of a Worker."""

    STOPPED = "stopped"
    IDLE = "idle"
    BUSY = "busy"


class Worker:
    """
    A `Worker`'s purpose is to request and process jobs from a
    `Controller` in a loop.

    Keyword arguments:
    controller -- orchestra-`Controller` to be used
    job_factory_map -- mapping of job-types to job-instructions
    report_type_map -- mapping of job-types to Report-class
    name -- optional name tag for this worker (used in logging)
            (default None; generates unique name from hostname)
    process_timeout -- timeout for individual jobs in seconds; exceeding
                       this value causes the worker to abort execution
                       (default None)
    registry_push_interval -- interval for pushes of job results to the
                              registry in seconds
                              (default 1)
    lock_refresh_interval -- interval for refreshes of locks on jobs in
                             queue in seconds
                             (default 1)
    message_interval -- interval for the message-polling in seconds
                        (default 1)
    """

    def __init__(
        self,
        controller: Controller,
        job_factory_map: Mapping[str, Callable[[JobContext, JobInfo], None]],
        report_type_map: Mapping[str, type[Report]],
        name: Optional[str] = None,
        *,
        process_timeout: Optional[float] = None,
        registry_push_interval: float = 1,
        lock_refresh_interval: float = 1,
        messages_interval: float = 1,
    ) -> None:
        self.controller = controller
        if len(job_factory_map) == 0 or len(report_type_map) == 0:
            raise ValueError(
                "At least one key in the map of job types is required."
            )
        if set(job_factory_map.keys()) != set(report_type_map.keys()):
            raise ValueError("Maps for job- and report-types do not match.")
        self.job_factory_map = job_factory_map
        self.report_type_map = report_type_map

        if name is None:
            self._name = f"Worker-{socket.gethostname()}-{str(uuid4())[:8]}"
        else:
            self._name = name

        self.process_timeout = process_timeout
        self.registry_push_interval = registry_push_interval
        self.lock_refresh_interval = lock_refresh_interval
        self.messages_interval = messages_interval

        # business logic
        self._thread: Optional[threading.Thread] = None
        self._process: Optional[DilledProcess] = None
        self._master_lock = threading.Lock()

        self._process_context: Optional[ProcessContext] = None

        self._stop_context = StopContext()
        self._stop_context.stopped.set()
        self._abort_context = AbortContext()

    @property
    def name(self) -> str:
        """Returns worker name."""
        return self._name

    @property
    def state(self) -> WorkerState:
        """Returns current state as `WorkerState`."""
        if self._thread is None or not self._thread.is_alive():
            return WorkerState.STOPPED
        if self._process is not None and self._process.is_alive():
            return WorkerState.BUSY
        return WorkerState.IDLE

    @staticmethod
    def _run_job_child(
        pipe: DilledConnection,
        process_context: ProcessContext,
        cmd: Callable[[JobContext, JobConfig], None],
    ) -> None:
        """Business logic for child-process."""
        # pylint: disable=consider-using-f-string
        try:
            pid = os.getpid()
            process_context.started = True
            pipe.send(process_context)
            Logging.print_to_log(
                f"Child-process with PID {pid} "
                + f"{os.environ.get('ORCHESTRA_MP_METHOD', 'spawn')}ed for "
                + f"job '{process_context.info.token.value}'.",
                Logging.LEVEL_DEBUG,
            )

            def handler(sig, frame, name):
                Logging.print_to_log(
                    f"Child process (PID {pid}) for job "
                    + f"'{process_context.info.token.value}' received {name}.",
                    Logging.LEVEL_INFO,
                )
                sys.exit(0)

            signal.signal(
                signal.SIGINT,
                partial(handler, name="SIGINT"),
            )
            signal.signal(
                signal.SIGTERM,
                partial(handler, name="SIGTERM"),
            )

            # setup child-process callbacks
            def add_child(child: ChildJob) -> None:
                process_context.children.append(child)

            def remove_child(id_: str) -> None:
                process_context.children = list(
                    filter(lambda c: c.id != id_, process_context.children)
                )

        # pylint: disable=broad-exception-caught
        except Exception as exc_info:
            Logging.print_to_log(
                "Exception during pre-initialization in child process (PID "
                + f"{pid}) for job '{process_context.info.token.value}': "
                + str(exc_info),
                Logging.LEVEL_ERROR,
            )
            Logging.print_to_log(
                traceback.format_exc(),
                Logging.LEVEL_DEBUG,
            )
            return

        try:
            # initialize report
            process_context.info.report.progress.run()

            # set and log job metadata
            if process_context.info.metadata.produced is not None:
                process_context.info.report.log.log(
                    LoggingContext.EVENT,
                    origin=process_context.worker_id,
                    body="Produced at {} by '{}'.".format(
                        process_context.info.metadata.produced.datetime,
                        process_context.info.metadata.produced.by,
                    ),
                )
            process_context.info.metadata.consume(process_context.worker_id)
            process_context.info.report.log.log(
                LoggingContext.EVENT,
                origin=process_context.worker_id,
                body="Consumed at {} by '{}'.".format(
                    process_context.info.metadata.consumed.datetime,
                    process_context.info.metadata.consumed.by,
                ),
            )
            pipe.send(process_context)
        # pylint: disable=broad-exception-caught
        except Exception as exc_info:
            Logging.print_to_log(
                f"Exception during initialization in child process (PID {pid})"
                + f" for job '{process_context.info.token.value}': {exc_info}",
                Logging.LEVEL_ERROR,
            )
            Logging.print_to_log(
                traceback.format_exc(),
                Logging.LEVEL_DEBUG,
            )
            process_context.info.report.log.log(
                LoggingContext.ERROR,
                origin=process_context.worker_id,
                body=(f"Job failed due to exception in worker: {exc_info}"),
            )
            process_context.info.metadata.complete(process_context.worker_id)
            process_context.info.report.progress.complete()
            process_context.info.report.progress.verbose = (
                "worker failed to run job"
            )
            process_context.completed = True
            pipe.send(process_context)
            pipe.close()
            return

        try:
            # run actual job
            cmd(
                JobContext(
                    lambda: pipe.send(process_context), add_child, remove_child
                ),
                process_context.info,
            )
        # pylint: disable=broad-exception-caught
        except Exception as exc_info:
            try:
                Logging.print_to_log(
                    f"Exception in child process (PID {pid}) for job "
                    + f"'{process_context.info.token.value}': {exc_info}",
                    Logging.LEVEL_ERROR,
                )
                Logging.print_to_log(
                    traceback.format_exc(),
                    Logging.LEVEL_DEBUG,
                )
                process_context.info.report.log.log(
                    LoggingContext.ERROR,
                    origin=process_context.worker_id,
                    body=(
                        "Job failed due to exception in child process: "
                        + f"{traceback.format_exc()}"
                    ),
                )
                process_context.info.metadata.complete(
                    process_context.worker_id
                )
                process_context.info.report.progress.complete()
                process_context.info.report.progress.verbose = "job failed"
                process_context.completed = True
                pipe.send(process_context)
                Logging.print_to_log(
                    f"Child-process (PID {pid}) for job "
                    + f"'{process_context.info.token.value}' will now "
                    + "terminate.",
                    Logging.LEVEL_DEBUG,
                )
                pipe.close()
                return
            # pylint: disable=broad-exception-caught
            except Exception as exc_info_inner:
                Logging.print_to_log(
                    "An exception occurred while handling an exception in "
                    + f"child process(PID {pid}) for job "
                    + f"'{process_context.info.token.value}': "
                    + str(exc_info_inner),
                    Logging.LEVEL_ERROR,
                )
                Logging.print_to_log(
                    traceback.format_exc(),
                    Logging.LEVEL_DEBUG,
                )
                pipe.close()
                return

        try:
            process_context.info.metadata.complete(process_context.worker_id)
            process_context.info.report.progress.complete()
            process_context.info.report.progress.verbose = "job completed"
            process_context.info.report.log.log(
                LoggingContext.EVENT,
                origin=process_context.worker_id,
                body="Completed at {} by '{}'.".format(
                    process_context.info.metadata.completed.datetime,
                    process_context.info.metadata.completed.by,
                ),
            )
            process_context.completed = True
            pipe.send(process_context)
            Logging.print_to_log(
                f"Child-process (PID {pid}) for job "
                + f"'{process_context.info.token.value}' will now terminate "
                + "normally.",
                Logging.LEVEL_DEBUG,
            )
            pipe.close()
        # pylint: disable=broad-exception-caught
        except Exception as exc_info:
            Logging.print_to_log(
                f"Exception during cleanup in child process (PID {pid}) for "
                + f"job '{process_context.info.token.value}': {exc_info}",
                Logging.LEVEL_ERROR,
            )
            Logging.print_to_log(
                traceback.format_exc(),
                Logging.LEVEL_DEBUG,
            )

    def _run_job_host(self, lock: Lock) -> None:
        """Business logic for host-process."""
        # pre-processing
        # * load existing data
        info = JobInfo.from_json(self.controller.get_info(lock.token))
        if info.token is None:
            info.token = self.controller.get_token(lock.token)
        # * create context for child-process
        self._process_context = ProcessContext(self._name, info, [])
        if info.config.type_ not in self.job_factory_map:
            self.controller.release_lock(lock.id)
            raise ValueError(
                f"Worker '{self._name}' encountered an unknown "
                + f"job type '{info.config.type_}'."
            )
        # * initialize report
        try:
            # if report has been set already, start from there
            self._process_context.info.report = self.report_type_map[
                info.config.type_
            ].from_json(
                (self._process_context.info.report or {})
                | {"token": info.token.json}
            )
        except ValueError:
            # otherwise use plain report as fallback
            self._process_context.info.report = self.report_type_map[
                info.config.type_
            ](token=info.token)

        # run job as process
        # * create pipe
        parent_pipe, child_pipe = DilledPipe()
        # * setup process
        self._process = DilledProcess(
            target=self._run_job_child,
            args=(
                DillIgnore(child_pipe),
                self._process_context,
                self.job_factory_map[info.config.type_],
            ),
            name=f"{self._name}-job-{lock.token}",
        )
        # * run
        self._process.start()

        # process results sent via pipe
        child_pipe.close()
        started = time()
        since_push = datetime.fromtimestamp(0)
        since_message = datetime.fromtimestamp(0)
        since_lock = datetime.fromtimestamp(0)
        while True:
            now = datetime.now()
            # process incoming data
            needs_push = False
            pipe_closed = False
            while parent_pipe.poll(timeout=0.01):
                try:
                    self._process_context = parent_pipe.recv()
                except EOFError:
                    pipe_closed = True
                    break
                else:
                    needs_push = True

            # check conditions for termination
            if self._process_context.completed:
                Logging.print_to_log(
                    f"Worker '{self._name}' will stop working on job "
                    + f"'{lock.token}': job completed.",
                    Logging.LEVEL_DEBUG,
                )
                break
            # exit if done
            if pipe_closed and not self._process.is_alive():
                Logging.print_to_log(
                    f"Worker '{self._name}' will stop working on job "
                    + f"'{lock.token}': child process terminated.",
                    Logging.LEVEL_DEBUG,
                )
                break

            # update registry
            if (
                needs_push
                and since_push + timedelta(seconds=self.registry_push_interval)
                < now
            ):
                # if something was read, push to registry
                try:
                    self.controller.registry_push(
                        lock.id,
                        status="running",
                        info=self._process_context.info,
                    )
                except ValueError as exc_info:
                    Logging.print_to_log(
                        f"Worker '{self._name}' encountered an "
                        + "unrecoverable error while attempting to push "
                        + f"job information ('{lock.token}') to the "
                        + f"controller: {exc_info}",
                        Logging.LEVEL_ERROR,
                    )
                    self._abort_context.origin = self._name
                    self._abort_context.reason = "cannot connect to controller"
                    self._process.kill()
                    break
                else:
                    since_push = now

            # refresh lock
            if (
                since_lock + timedelta(seconds=self.lock_refresh_interval)
                < now
            ):
                try:
                    lock = self.controller.refresh_lock(lock.id)
                except ValueError as exc_info:
                    Logging.print_to_log(
                        f"Worker '{self._name}' encountered an error "
                        + "while attempting to refresh lock on job "
                        + f"'{lock.token}': {exc_info}",
                        Logging.LEVEL_ERROR,
                    )
                    if lock.expires_at < datetime.now():
                        Logging.print_to_log(
                            f"Worker '{self._name}' encountered an "
                            + f"expired lock for token '{lock.token}' "
                            + "after a failed attempt to refresh. Job "
                            + "failed.",
                            Logging.LEVEL_ERROR,
                        )
                        self._abort_context.origin = self._name
                        self._abort_context.reason = "stale lock"
                        self._process.kill()
                        break
                else:
                    since_lock = now

            # check messages
            if since_message + timedelta(seconds=self.messages_interval) < now:
                try:
                    messages = self.controller.message_get(since_message)
                except ValueError as exc_info:
                    Logging.print_to_log(
                        f"Worker '{self._name}' encountered an error "
                        + "while attempting to fetch messages for job "
                        + f"'{lock.token}' via the controller: {exc_info}",
                        Logging.LEVEL_ERROR,
                    )
                since_message = now
                for message in messages:
                    if message.token != info.token.value:
                        continue

                    if message.instruction == Instruction.ABORT:
                        self._abort_context.origin = message.origin
                        self._abort_context.reason = message.content
                        self._process.kill()

            # check timeout
            if (
                self.process_timeout is not None
                and time() - started > self.process_timeout
            ):
                self._abort_context.origin = self._name
                self._abort_context.reason = (
                    f"process timeout after {self.process_timeout} seconds"
                )
                self._process.kill()

        # safeguard for stuck process (observed for regular exit)
        self._process.join(timeout=0.1)
        if self._process.is_alive():
            self._process.kill()

        # handle job exit
        if self._process_context.completed:
            # regular
            try:
                self.controller.registry_push(
                    lock.id,
                    status="completed",
                    info=self._process_context.info,
                )
            except ValueError as exc_info:
                Logging.print_to_log(
                    f"Worker '{self._name}' encountered an unrecoverable "
                    + "error while attempting to push job information "
                    + f"('{lock.token}') to the controller: {exc_info}",
                    Logging.LEVEL_ERROR,
                )
        else:
            if self._abort_context.reason is None:
                self._abort_context.reason = "unknown"
            # abort
            if self._abort_context.origin is None:
                Logging.print_to_log(
                    f"Job '{lock.token}' did not complete: "
                    + f"{self._abort_context.reason}. Worker"
                    + f"'{self._name}' will now abort this job.",
                    Logging.LEVEL_ERROR,
                )
            else:
                Logging.print_to_log(
                    f"Job '{lock.token}' aborted by "
                    + f"'{self._abort_context.origin}' "
                    + f"({self._abort_context.reason}).",
                    Logging.LEVEL_INFO,
                )
            if self._abort_context.origin is None:
                self._abort_context.origin = self._name
            # * handle children
            for child in self._process_context.children:
                Logging.print_to_log(
                    f"Aborting child of '{lock.token}'.",
                    Logging.LEVEL_DEBUG,
                )
                try:
                    child.abort(
                        self._process_context.info,
                        self._abort_context,
                    )
                # pylint: disable=broad-exception-caught
                except Exception as exc_info:
                    Logging.print_to_log(
                        f"Worker '{self._name}' failed to abort child "
                        + f"'{child.id}' ({child.name}) while aborting "
                        + f"job '{lock.token}': {exc_info}",
                        Logging.LEVEL_ERROR,
                    )
                    self._process_context.info.report.log.log(
                        LoggingContext.ERROR,
                        origin=self._name,
                        body=(
                            f"failed to abort child '{child.id}' "
                            + f"({child.name}): {exc_info}"
                        ),
                    )

            # * update metadata
            self._process_context.info.metadata.abort(
                self._abort_context.origin
            )
            self._process_context.info.report.progress.abort()
            self._process_context.info.report.progress.verbose = (
                f"job aborted ({self._abort_context.reason})"
            )
            self._process_context.info.report.log.log(
                # pylint: disable=consider-using-f-string
                LoggingContext.EVENT,
                origin=self._name,
                body="Aborted at {} by '{}'.".format(
                    self._process_context.info.metadata.aborted.datetime,
                    self._abort_context.origin,
                ),
            )
            self._process_context.info.report.log.log(
                LoggingContext.ERROR,
                origin=self._name,
                body=(
                    "Job aborted by "
                    + f"'{self._abort_context.origin}' "
                    + f"({self._abort_context.reason})."
                ),
            )
            try:
                self.controller.registry_push(
                    lock.id,
                    status="aborted",
                    info=self._process_context.info,
                )
            except ValueError as exc_info:
                Logging.print_to_log(
                    f"Worker '{self._name}' encountered an unrecoverable "
                    + "error while attempting to push job information "
                    + f"('{lock.token}') to the controller: {exc_info}",
                    Logging.LEVEL_ERROR,
                )

    def _work_loop(self, interval: float) -> None:
        """Runs worker loop until stopped."""
        try:
            while not self._stop_context.stop.is_set():
                now = time()
                # reset state
                self._process_context = None
                self._abort_context.origin = None
                self._abort_context.reason = None

                # find and start job
                try:
                    lock = self.controller.queue_pop(self._name)
                except ValueError as exc_info:
                    Logging.print_to_log(
                        f"Worker '{self._name}' failed to fetch current "
                        + f"queue from the controller: {exc_info}",
                        Logging.LEVEL_ERROR,
                    )
                if lock is None:
                    # no work left
                    if self._stop_context.stop_on_idle.is_set():
                        # detect request for early exit
                        self._stop_context.stop.set()
                else:
                    # run job
                    Logging.print_to_log(
                        f"Worker '{self._name}' starts working on job "
                        + f"'{lock.token}'.",
                        Logging.LEVEL_DEBUG,
                    )
                    self._run_job_host(lock)
                    Logging.print_to_log(
                        f"Worker '{self._name}' stops working on job "
                        + f"'{lock.token}'.",
                        Logging.LEVEL_DEBUG,
                    )
                    self.controller.release_lock(lock.id)

                self._stop_context.stop_on_idle.wait(
                    max(0, interval - (time() - now))
                )
        # pylint: disable=broad-exception-caught
        except Exception as exc_info:
            self._stop_context.stopped.set()
            if self._process is not None and self._process.is_alive():
                self._process.kill()
            Logging.print_to_log(
                f"Worker '{self._name}' encountered an error and has been "
                + f"stopped: {exc_info}",
                Logging.LEVEL_ERROR,
            )
            Logging.print_to_log(
                traceback.format_exc(),
                Logging.LEVEL_DEBUG,
            )
        else:
            self._stop_context.stopped.set()
            Logging.print_to_log(
                f"Worker '{self._name}' stopped.",
                Logging.LEVEL_INFO,
            )

    def start(self, interval: float = 1, daemon: bool = False) -> None:
        """
        Enter a loop where jobs are pulled from queue and then
        processed.

        Keyword arguments:
        interval -- polling interval for jobs
                    (default 1)
        daemon -- whether to run as daemon (only relevant if not `block`)
                  (default False)
        """
        with self._master_lock:
            if self.state != WorkerState.STOPPED:
                raise RuntimeError(
                    f"Worker '{self._name}' is already running."
                )

            # reset controls
            self._stop_context.stop.clear()
            self._stop_context.stop_on_idle.clear()
            self._stop_context.stopped.clear()

            # configure
            self._thread = threading.Thread(
                target=self._work_loop, args=(interval,), daemon=daemon
            )

            # run
            self._thread.start()
            Logging.print_to_log(
                f"Worker '{self._name}' started.",
                Logging.LEVEL_INFO,
            )

    def stop(
        self, block: bool = False, timeout: Optional[float] = None
    ) -> None:
        """Stops before entering next worker loop."""
        self._stop_context.stop.set()
        self._stop_context.stop_on_idle.set()
        if block:
            self._stop_context.stopped.wait(timeout)
            while self._thread is not None and self._thread.is_alive():
                sleep(0.01)

    def stop_on_idle(
        self, block: bool = False, timeout: Optional[float] = None
    ) -> None:
        """Stops the next time the queue is empty."""
        self._stop_context.stop_on_idle.set()
        if block:
            self._stop_context.stopped.wait(timeout)
            while self._thread is not None and self._thread.is_alive():
                sleep(0.01)

    def kill(
        self,
        origin: Optional[str] = None,
        reason: Optional[str] = None,
        block: bool = False,
        timeout: Optional[float] = None,
    ):
        """Kills any currently running job and stops the worker."""
        if self.state is not WorkerState.STOPPED:
            Logging.print_to_log(
                f"Worker '{self._name}' received kill from "
                + f"'{origin or 'unknown'}' ({reason or 'unknown'}).",
                Logging.LEVEL_INFO,
            )

        self._stop_context.stop.set()
        self._stop_context.stop_on_idle.set()
        if self.state is WorkerState.BUSY:
            self._abort_context.origin = origin
            self._abort_context.reason = reason
            self._process.kill()

        if block:
            self._stop_context.stopped.wait(timeout)
            while self._thread is not None and self._thread.is_alive():
                sleep(0.01)
