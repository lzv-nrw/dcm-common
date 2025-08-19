"""
Module for the `ScalableOrchestrator`-class definition.
"""

from typing import Optional, Callable, Mapping, TextIO
from dataclasses import dataclass, field
import socket
from threading import Thread, Event, Lock, get_native_id
from time import sleep
from pathlib import Path
import sys
import os

from flask import request

from dcm_common import LoggingContext as Context, LogMessage
from dcm_common.util import now
from dcm_common.db import (
    KeyValueStoreAdapter, MemoryStore, NativeKeyValueStoreAdapter
)
from dcm_common.models import DataModel, JSONObject, Token, Report
from .job import Job


class JobConfig(DataModel):
    """
    Datamodel for the configuration details of a `Job`.

    Keyword arguments:
    original_body -- original, i.e., unaltered JSON-request body
                     (default None; attempts to get json via
                     `flask.request.json`; uses `None` instead if
                     previous is not successful)
    request_body -- request body filled with additional information like
                    defaults
                    (default None)
    properties -- free field that can be used to store additional
                  information for the job-factory (e.g., specific
                  endpoint)
                  (default None)
    context -- orchestration-context identifier for the given `Job` (see
               `ScalableOrchestrator` for details)
               (default None)
    """

    original_body: Optional[JSONObject]
    request_body: Optional[JSONObject]
    properties: Optional[JSONObject]
    context: Optional[str]

    def __init__(
        self,
        original_body: Optional[JSONObject] = None,
        request_body: Optional[JSONObject] = None,
        properties: Optional[JSONObject] = None,
        context: Optional[str] = None,
    ) -> None:
        if original_body is None:
            try:
                self.original_body = request.json
            except RuntimeError:
                self.original_body = None
        else:
            self.original_body = original_body
        self.request_body = request_body
        self.properties = properties
        self.context = context

    @DataModel.serialization_handler("original_body")
    @classmethod
    def original_body_serialization_handler(cls, value):
        """Handle by always adding to json."""
        return value


@dataclass
class MetadataRecord(DataModel):
    """Datamodel for a single record in a `Job`'s metadata."""
    by: Optional[str] = None
    datetime: Optional[str] = field(
        default_factory=lambda: now(True).isoformat()
    )


@dataclass
class JobMetadata(DataModel):
    """Datamodel for `Job`-metadata."""
    produced: Optional[MetadataRecord] = None
    consumed: Optional[MetadataRecord] = None
    aborted: Optional[MetadataRecord] = None
    completed: Optional[MetadataRecord] = None


@dataclass
class JobInfo(DataModel):
    """
    Datamodel aggregating `Job`-related information (stored in
    registry).
    """
    config: JobConfig
    token: Token
    metadata: JobMetadata = field(default_factory=JobMetadata)
    report: Optional[DataModel | JSONObject] = None

    @DataModel.deserialization_handler("report")
    @classmethod
    def report_deserialization_handler(cls, value):
        """Manually handle 'report'; only supported as JSONObject."""
        if value is None:
            DataModel.skip()
        return value


@dataclass
class _AbortionContext:
    """Collects job abortion-related information."""
    event: Event = field(default_factory=Event)
    reason: Optional[str] = None
    origin: Optional[str] = None
    re_queue: bool = False


@dataclass
class _JobRecord:
    """Used to aggregate execution-related information for a job."""
    job: Job
    runner: Thread
    info: JobInfo
    abortion: _AbortionContext = field(default_factory=_AbortionContext)


class ScalableOrchestrator:
    """
    A `ScalableOrchestrator` can be used to process jobs (generated
    based on a `JobConfig` using a `factory`) from a `queue` and publish
    (intermediate and final) results to a `registry`.

    Keyword arguments:
    factory -- factory for creation of `Job`-instances based on a given
               `JobConfig`; note that
               * the data-object will be replaced by info.report
                 (use the 'pre-execution'-hook if a special setup is
                 required)
               * using this parameter is equivalent to passing
                 `{None: factory}` to the mutually exclusive parameter
                 `mv_factories` for multi-view applications
    queue -- `KeyValueStoreAdapter` for a (shared) queue; data written
             to queue consists of `Token.value`s
             (default None; corresponds to a combination of
             `MemoryStore` and `NativeKeyValueStoreAdapter`)
    registry -- `KeyValueStoreAdapter` for a (shared) job registry; data
                written to registry consists of `JobInfo`-objects
                (default None; corresponds to a combination of
                `MemoryStore` and `NativeKeyValueStoreAdapter`)
    nprocesses -- vertical scaling option, maximum number of concurrent
                  processes
                  (default None; uses `ORCHESTRATION_PROCESSES`-env
                  variable or 1)
    queue_hooks -- hook-dictionary similar to `Job`-hooks; available
                   keys:
                   * `pre-register`: ran before adding `Job` to the
                     registry
                   * `pre-queue`: ran after registering the `Job` and
                     before posting it to the queue
                   values should be callables that accept the associated
                   `JobInfo`-object as a positional argument

                   using this parameter is equivalent to passing
                   `{None: exec_hooks}` to the mutually exclusive
                   parameter `_mv_exec_hooks` for multi-view applications

                   (default None; no hooks are executed)
    exec_hooks -- hook-dictionary similar to `Job`-hooks; available
                  keys:
                  * `pre-execution`: ran before starting execution
                  * `completion`: ran before finalizing the associated
                    record
                  values should be callables that accept the associated
                  `JobInfo`- and `Job`-object as positional arguments

                  using this parameter is equivalent to passing
                  `{None: exec_hooks}` to the mutually exclusive
                  parameter `_mv_exec_hooks` for multi-view applications

                  (default None; no hooks are executed)
    _mv_factories -- job-factory mapping for multi-view applications;
                     when constructing `Job`s the field
                     `JobConfig.context` is used as key to select the
                     corresponding factory:
                     see the mutually exclusive parameter `factory` for
                     details
                     (default None)
    _mv_queue_hooks -- queue-hooks mapping for multi-view applications;
                       when hitting hook triggers, `JobConfig.context`
                       is used as key to look up the corresponding hook:
                       see the mutually exclusive parameter
                       `queue_hooks` for details
                       (default None)
    _mv_exec_hooks -- exec-hooks mapping for multi-view applications;
                      when hitting hook triggers, `JobConfig.context`
                      is used as key to look up the corresponding hook:
                      see the mutually exclusive parameter
                      `exec_hooks` for details
                      (default None)
    _hostname -- optionally set custom hostname
                 (default None; uses machine's hostname)
    _debug -- run in debug-mode where actions are printed to stderr
              (default None; uses `ORCHESTRATION_DEBUG`-env variable or
              `False`)
    _debug_file -- debug output file
                   (default None; uses `sys.stderr`)
    """
    def __init__(
        self,
        factory: Optional[Callable[[JobConfig], Job]] = None,
        queue: Optional[KeyValueStoreAdapter] = None,
        registry: Optional[KeyValueStoreAdapter] = None,
        nprocesses: Optional[int] = None,
        queue_hooks: Optional[Mapping[str, Callable[[JobInfo], None]]] = None,
        exec_hooks: Optional[Mapping[str, Callable[[JobInfo, Job], None]]] = None,
        _mv_factories: Optional[Mapping[Optional[str], Callable[[JobConfig], Job]]] = None,
        _mv_queue_hooks: Optional[Mapping[Optional[str], Mapping[str, Callable[[JobInfo], None]]]] = None,
        _mv_exec_hooks: Optional[Mapping[Optional[str], Mapping[str, Callable[[JobInfo, Job], None]]]] = None,
        _hostname: Optional[str] = None,
        _debug: Optional[bool] = None,
        _debug_file: Optional[TextIO] = None
    ) -> None:
        # job-factory
        if factory and _mv_factories:
            raise ValueError(
                "Passing both 'factory' and '_mv_factories' to "
                + "'ScalableOrchestrator'-constructor is not supported."
            )
        if factory:
            self.factories = {}
            self.register_factory(None, factory)
        else:
            self.factories = _mv_factories or {}

        # queue/registry
        if not queue:
            queue = NativeKeyValueStoreAdapter(MemoryStore())
        self.queue = queue
        if not registry:
            registry = NativeKeyValueStoreAdapter(MemoryStore())
        self.registry = registry

        # vertical scaling
        if nprocesses is None:
            self.nprocesses = int(
                os.environ.get("ORCHESTRATION_PROCESSES", 1)
            )
        else:
            self.nprocesses = nprocesses

        # hooks
        if queue_hooks and _mv_queue_hooks:
            raise ValueError(
                "Passing both 'queue_hooks' and '_mv_queue_hooks' to "
                + "'ScalableOrchestrator'-constructor is not supported."
            )
        if queue_hooks is not None:
            self.queue_hooks = {}
            self.register_queue_hooks(None, queue_hooks or {})
        else:
            self.queue_hooks = _mv_queue_hooks or {}
        if exec_hooks and _mv_exec_hooks:
            raise ValueError(
                "Passing both 'exec_hooks' and '_mv_exec_hooks' to "
                + "'ScalableOrchestrator'-constructor is not supported."
            )
        if exec_hooks is not None:
            self.exec_hooks = {}
            self.register_exec_hooks(None, exec_hooks or {})
        else:
            self.exec_hooks = _mv_exec_hooks or {}

        # business logic
        self._thread: Optional[Thread] = None
        self._stop = Event()
        self._stop_on_idle = Event()
        self._looking_for_work = Event()  # used to signal 'not idling' in
        # the short interval between popping
        # a token from the queue and
        # registering a new job in self._jobs
        self._skip_sleep = Event()
        self._jobs: dict[str, _JobRecord] = {}
        self._hostname = (
            socket.gethostname() if _hostname is None else _hostname
        )
        self._abort_lock = Lock()

        # debug
        if _debug is None:
            self._debug = os.environ.get("ORCHESTRATION_DEBUG", "0") == "1"
        else:
            self._debug = _debug
        self._debug_file = _debug_file or sys.stderr

    def register_factory(
        self, context: Optional[str], factory: Callable[[JobConfig], Job]
    ):
        """
        Register `factory` as `Job`-factory for the context `context`.

        Keyword arguments:
        context -- context identifier
        factory -- factory for creation of `Job`-instances based on a
                   given `JobConfig`
        """
        self.factories[context] = factory

    def register_queue_hooks(
        self,
        context: Optional[str],
        queue_hooks: Mapping[str, Callable[[JobInfo], None]]
    ):
        """
        Register `queue_hooks` as set of queue-hooks for the context
        `context`.

        Keyword arguments:
        context -- context identifier
        queue_hooks -- hook-dictionary similar to `Job`-hooks`
        """
        self.queue_hooks[context] = queue_hooks

    def register_exec_hooks(
        self,
        context: Optional[str],
        exec_hooks: Mapping[str, Callable[[JobInfo], None]]
    ):
        """
        Register `exec_hooks` as set of exec-hooks for the context
        `context`.

        Keyword arguments:
        context -- context identifier
        exec_hooks -- hook-dictionary similar to `Job`-hooks`
        """
        self.exec_hooks[context] = exec_hooks

    @property
    def running(self) -> bool:
        """Returns `True` if currently running."""
        return self._thread is not None and self._thread.is_alive()

    @property
    def idle(self) -> bool:
        """
        Returns `True` if currently no jobs being processed and none are
        in queue.
        """

        try:
            queue_length = len(self.queue.keys())
        # pylint: disable=broad-exception-caught
        except Exception as exc_info:
            print(
                "Orchestrator failed to poll the job-queue due to a "
                + f"'{type(exc_info).__name__}': {exc_info}.",
                file=sys.stderr
            )
            return False

        return (
            not self._looking_for_work.is_set()
            and queue_length == 0
            and len(self._jobs) == 0
        )

    @property
    def ready(self) -> bool:
        """Returns `True` if ready to pull from queue."""
        return len(self._jobs) < self.nprocesses

    @property
    def jobs(self) -> tuple[str, ...]:
        """
        Returns a tuple of token values for currently running jobs.
        """
        return tuple(self._jobs.keys())

    def _write_debug(self, msg: str):
        """Formats and writes debug-message to `self._debug_file`."""
        print(
            f"[{now(True).replace(tzinfo=None)}] "
            + f"{get_native_id()}@{self._hostname}: {msg}",
            file=self._debug_file
        )

    def submit(
        self, config: JobConfig, token: Optional[str | Token] = None
    ) -> Token:
        """
        Submit `config` to queue and create initial record in registry.
        Returns `Token`.

        Keyword arguments:
        config -- `JobConfig` to be added to the queue
        token -- override the automatic generation of a new token; if
                 provided, overwrite existing entries in queue and
                 registry, otherwise generate `Token` anew
                 (default None)
        """
        if token is None:
            # generate token
            _token = self.registry.push(None)
        else:
            # use existing token
            _token = self._get_token_value(token)
            # FIXME: with bad timing, this check can fail; a fix requires
            # refactoring of how queue and registry work
            info = self.registry.read(_token)
            if info is False:
                # in process of being created but not ready yet
                raise ValueError(
                    f"Job-creation for token '{_token}' still in progress."
                )
            if info is not None:
                if (
                    info.get("config", {}).get("original_body", {})
                    != config.original_body
                ):
                    raise ValueError(
                        f"Duplicate request token '{_token}' with different "
                        + "request body."
                    )
                return Token.from_json({"value": _token} | info.get("token"))
            self.registry.write(_token, False)

        # create info-record
        info = JobInfo(
            config,
            token=Token(value=_token),  # TODO: add expiration-args from config
            metadata=JobMetadata(
                produced=MetadataRecord(by=self._hostname)
            )
        )

        # post to registry
        if "pre-register" in self.queue_hooks.get(config.context, {}):
            self.queue_hooks[config.context]["pre-register"](info)
        self.registry.write(_token, info.json)

        # post to queue
        if "pre-queue" in self.queue_hooks.get(config.context, {}):
            self.queue_hooks[config.context]["pre-queue"](info)
        self.registry.write(_token, info.json)
        try:
            self.queue.write(_token, _token)
        # pylint: disable=broad-exception-caught
        except Exception as exc_info:
            self.registry.delete(_token)
            raise exc_info

        if self._debug:
            self._write_debug(f"Submitted job with token '{_token}'.")

        return info.token

    @staticmethod
    def _get_token_value(token: str | Token) -> str:
        """Returns either `token` or `token.value` depending on type."""
        return token if isinstance(token, str) else token.value

    def get_report(self, token: str | Token) -> Optional[JSONObject]:
        """
        Returns report associated with `Token.value` as `JSONObject` or
        `None` (if not successful).
        """
        info = self.get_info(token)
        return (info or {}).get("report", None)

    def get_info(self, token: str | Token) -> Optional[JSONObject]:
        """
        Returns `JobInfo` associated with `Token.value` as `JSONObject`
        or `None` (if not successful).
        """
        info = self.registry.read(self._get_token_value(token))
        if not info:
            return None
        return info

    def _update_registry(
        self, token: str, record, force=False, override=False
    ) -> None:
        """
        Updates data for `record` in the job registry (if there are
        changes).

        Keyword arguments:
        token -- associated `Token.value`
        record -- associated `_JobRecord`
        force -- if `True`, write to registry independent of changes
        override -- if `True`, skip reload data from `job.data`
        """
        # the pipe-system for `Job` replaces the `Job.data`-object, so
        # `record.info.report` remains unchanged and can be used as a
        # reference for detecting changes
        if force or record.info.report != record.job.data:
            if not override:
                record.info.report = record.job.data
            self.registry.write(token, record.info.json)

    def _cleanup_and_update(self) -> None:
        """
        Cleanup jobs (completed/aborted) and write updates to registry.
        """
        completed_jobs = []
        for token, record in self._jobs.items():
            force = False
            override = False
            if record.abortion.event.is_set():
                if self._debug:
                    self._write_debug(
                        f"Job '{record.info.token.value}' aborted by "
                        + f"{record.abortion.origin} due to "
                        + f"{record.abortion.reason}."
                    )
                record.job.abort(
                    reason=record.abortion.reason,
                    origin=record.abortion.origin
                )
                if record.abortion.re_queue:
                    if self._debug:
                        self._write_debug(
                            f"Re-submitting job with token '{token}'."
                        )
                    # reset registry and post to queue
                    self.registry.write(
                        token,
                        JobInfo(
                            record.info.config,
                            token=record.info.token,
                            metadata=JobMetadata(
                                produced=MetadataRecord(by=self._hostname)
                            ),
                        ).json,
                    )
                    self.queue.write(token, token)
                    completed_jobs.append(token)
                    # skip manual registry update (must not write to
                    # registry after submission to queue)
                    continue
            if record.job.completed or record.abortion.event.is_set():
                record.info.metadata.completed = MetadataRecord(
                    by=self._hostname
                )
                completed_jobs.append(token)
                if "completion" in self.exec_hooks.get(
                    record.info.config.context, {}
                ):
                    self.exec_hooks[record.info.config.context]["completion"](
                        record.info, record.job
                    )
                    # after completion, do not reload job.data after hook has
                    # run
                    override = True
                if self._debug:
                    self._write_debug(
                        f"Job '{record.info.token.value}' completed."
                    )
                force = True
            self._update_registry(
                token, record, force=force, override=override
            )
        for token in completed_jobs:
            del self._jobs[token]

    def _load_info(self, token: str) -> JobInfo:
        """
        Collect associated JobInfo-data from registry and initializes
        `JobInfo`-object.
        """
        _info = self.registry.read(token)
        if _info is None:
            return None
        return JobInfo.from_json(_info)

    def _write_loading_error(self, token: str, reason: str) -> None:
        """
        Abort a job at startup by
        * generating report with error-message
        * writing that report to the registry
        """
        _token = Token(value=token)
        info = JobInfo(
            config=JobConfig(
                original_body=None, request_body=None, properties=None
            ),
            token=_token,
            metadata=JobMetadata(
                consumed=MetadataRecord(by=self._hostname),
                completed=MetadataRecord(by=self._hostname),
            ),
            report=Report(host=self._hostname, token=_token, args=None)
        )
        info.report.progress.complete()
        info.report.progress.verbose = f"Failure: {reason}"
        info.report.log.log(
            Context.ERROR,
            origin=f"{self._hostname}-Orchestrator",
            body=f"Aborting job due to error: {reason}"
        )
        self.registry.write(token, info.json)

    def _run_next_in_queue(self, cwd: Path) -> None:
        """
        Attempt to load new job from queue and, if successful, run
        job.
        """
        # pop from queue
        try:
            token = self.queue.next(True)
        # pylint: disable=broad-exception-caught
        except Exception as exc_info:
            print(
                "Orchestrator failed to poll the job-queue due to a "
                + f"'{type(exc_info).__name__}': {exc_info}.",
                file=sys.stderr
            )
            return

        if token is None:
            return
        if self._debug:
            self._write_debug(f"Consumed token '{token[0]}'.")

        # load existing info from registry
        info = self._load_info(token[0])
        if info is None:
            self._write_loading_error(
                token[0],
                "Unable to load job config from registry."
            )
            if self._debug:
                self._write_debug(
                    "Failed to load info from registry for "
                    + f"'{token[0]}'."
                )
            return

        # validate context
        if info.config.context not in self.factories:
            self._write_loading_error(
                token[0],
                f"Unknown job-context '{info.config.context}'."
            )
            if self._debug:
                self._write_debug(
                    "Failed to load factory for job-context "
                    + f"'{info.config.context}' "
                    + f"(token='{token[0]}')."
                )
            return

        # finalize configuration and run job
        info.metadata.consumed = MetadataRecord(by=self._hostname)
        self._jobs[token[0]] = _JobRecord(
            job=(
                job := self.factories[info.config.context](
                    info.config
                )
            ),
            runner=(runner := Thread(target=job.run, args=(cwd,))),
            info=info,
        )
        if "pre-execution" in self.exec_hooks.get(
            info.config.context, {}
        ):
            self.exec_hooks[info.config.context]["pre-execution"](
                info, job
            )
        job.token = info.token  # TODO: remove Token requirement from Job constructor
        job.configure(data=info.report)
        if self._debug:
            self._write_debug(f"Starting job '{token[0]}'.")
        runner.start()

    def _run(
        self, interval: float, cwd: Path, stop: Event, stop_on_idle: Event
    ) -> None:
        """Processing loop definition."""
        if self._debug:
            self._write_debug("Starting orchestrator.")
        while True:
            self._looking_for_work.clear()

            self._skip_sleep.wait(interval)
            self._skip_sleep.clear()

            # cleanup and update registry
            self._cleanup_and_update()

            # detect stop signal
            if not self._jobs and stop.is_set():
                break

            # detect stop signal on idle
            if self.idle and stop_on_idle.is_set():
                break

            # start new job if needed and available
            if len(self._jobs) < self.nprocesses and not stop.is_set():
                with self._abort_lock:  # ensure all abortions are registered
                    # either via queue or self._jobs
                    self._looking_for_work.set()
                    self._run_next_in_queue(cwd)

        self._looking_for_work.clear()
        if self._debug:
            self._write_debug("Stopping orchestrator.")

    def as_thread(
        self,
        interval: Optional[float] = None,
        cwd: Optional[Path] = None,
        daemon: bool = False,
        stop: Optional[Event] = None,
        stop_on_idle: Optional[Event] = None,
    ) -> Thread:
        """
        Returns `Thread` that, when executed, enters a processing loop.

        Note that only a single instance of the orchestrator-loop
        should run at a given time. Hence, this method raises a
        `RuntimeError` if a new thread is requested while a previous
        thread is still running.

        By default the various methods involving to `stop` the
        orchestrator share the same event-signals that are used when
        calling `run` directly. However, a separate set of
        events can be specified explicitly.

        Keyword arguments:
        interval -- interval with which new jobs are pulled from the
                    queue
        cwd -- working directory for `Job`s
               (default None; uses current cwd)
        daemon -- whether to run as daemon (only relevant if not `block`)
                  (default False)
        stop -- optional `threading.Event` for stopping the loop
                (default None; uses native signals)
        stop_on_idle -- optional `threading.Event` for stopping the loop
                        upon reaching an idle state
                        (default None; uses native signals)
        """
        if self.running:
            raise RuntimeError(
                "Tried to create Orchestrator-thread while already running."
            )
        # reset and configure
        self._stop.clear()
        self._stop_on_idle.clear()
        self._thread = Thread(
            target=self._run,
            args=(
                interval or 1.0, cwd or Path("."),
                stop or self._stop,
                stop_on_idle or self._stop_on_idle
            ),
            daemon=daemon
        )
        return self._thread

    def run(
        self,
        interval: Optional[float] = None,
        cwd: Optional[Path] = None,
        block: bool = False,
        daemon: bool = False
    ) -> bool:
        """
        Enter a loop where jobs are pulled from queue and then
        processed (non-blocking).

        Keyword arguments:
        interval -- interval with which new jobs are pulled from the
                    queue
                    (default None; uses `as_thread`-default)
        cwd -- working directory for `Job`s
               (default None; uses current cwd)
        block -- whether to run in a blocking or non-blocking way
                 (default False)
        daemon -- whether to run as daemon (only relevant if not `block`)
                  (default False)
        """
        if self.running:
            raise RuntimeError(
                "Tried to create Orchestrator-thread while already running."
            )
        # reset and configure
        self._stop.clear()
        self._stop_on_idle.clear()
        self._thread = self.as_thread(interval, cwd or Path("."), daemon)

        # run
        self._thread.start()
        while block and self.running:
            sleep(interval)
        return True

    def block_until_idle(self) -> None:
        """Block execution until orchestrator enters the idle state."""
        while not self.idle:
            sleep(0.001)

    def stop(self, block: bool = False) -> None:
        """
        Stop processing new jobs from queue, currently running jobs are
        finished.

        Keyword arguments:
        block -- whether to block until orchestrator enters the idle
                 state
                 (default False)
        """
        if self._debug:
            self._write_debug("Received stop-signal.")
        self._stop.set()
        if block:
            while self.running:
                sleep(0.001)

    def stop_on_idle(self, block: bool = False) -> None:
        """
        Stop processing upon reaching the idle state.

        Keyword arguments:
        block -- whether to block until orchestrator enters the idle
                 state
                 (default False)
        """
        if self._debug:
            self._write_debug("Received stop-on-idle-signal.")
        self._stop_on_idle.set()
        if block:
            self.block_until_idle()

    def kill(
        self, origin: Optional[str] = None, reason: Optional[str] = None,
        re_queue: bool = False, block: bool = False
    ) -> None:
        """
        Stop processing jobs from queue and abort all currently running
        jobs.

        Keyword arguments:
        origin -- note of origin for request
        reason -- note of origin for request
        re_queue -- reset report in registry and insert token back into
                    queue
                    (default False)
        block -- whether to block until orchestrator's processing loop
                 stopped
                 (default False)
        """
        if self._debug:
            self._write_debug(
                f"Received kill-signal from {origin} with reason '{reason}'."
            )
        self._stop.set()
        self.abort(
            origin=origin, reason=reason, re_queue=re_queue, block=block
        )

    def dequeue(
        self,
        token: str | Token,
        origin: Optional[str] = None,
        reason: Optional[str] = None,
    ):
        """
        Abort job from queue. If job is not in queue, nothing is done.

        Keyword arguments:
        token -- job's token or token value
        origin -- note of origin for request
        reason -- note of origin for request
        """

        _token = self._get_token_value(token)
        if self._debug:
            self._write_debug(f"Attempting to abort '{_token}' from queue.")

        __token = self.queue.read(_token, pop=True)
        if __token is None:
            if self._debug:
                self._write_debug(
                    f"Token '{_token}' not in queue."
                )
            return

        # load existing info from registry
        info = self._load_info(_token)
        if info is None:
            self._write_loading_error(
                _token, "Unable to load job config from registry."
            )
            if self._debug:
                self._write_debug(
                    "Failed to load info from registry for "
                    + f"'{_token}'."
                )
            return

        # update record in registry
        info.metadata.aborted = MetadataRecord(by=origin)
        if "log" not in info.report:
            info.report["log"] = {}
        if Context.ERROR.name not in info.report["log"]:
            info.report["log"][Context.ERROR.name] = []
        info.report["log"][Context.ERROR.name].append(
            LogMessage(
                f"Removed job from queue: {reason}",
                origin=f"{self._hostname}-Orchestrator"
            ).json
        )
        if "progress" not in info.report:
            info.report["progress"] = {}
        info.report["progress"]["status"] = "aborted"
        self.registry.write(_token, info.json)

    def abort(
        self,
        token: Optional[str | Token] = None,
        origin: Optional[str] = None,
        reason: Optional[str] = None,
        re_queue: bool = False,
        block: bool = False
    ) -> None:
        """
        Abort either job associated with `token` (if available) or all
        currently running jobs.

        If `token.value` does not match with any running job, nothing is
        done.

        Keyword arguments:
        token -- specific job's token or token value
        origin -- note of origin for request
        reason -- note of origin for request
        re_queue -- reset report in registry and insert token back into
                    queue
                    (default False)
        block -- whether to block until
                 * the job has exited (if called with `token`)
                 * orchestrator's processing loop stopped (without
                 `token`)
                 (default False)
        """
        with self._abort_lock:  # ensure all abortions are registered
            # either via queue or self._jobs
            if token is None:
                records = list(self._jobs.values())
            else:
                _token = self._get_token_value(token)
                record = self._jobs.get(_token)
                if record:
                    records = [record]
                else:
                    records = []
        # run abort
        for record in records:
            record.abortion.origin = origin or "unknown"
            record.abortion.reason = reason or "unknown"
            record.abortion.re_queue = re_queue
            record.abortion.event.set()
            record.info.metadata.aborted = MetadataRecord(
                by=record.abortion.origin
            )
            if self._debug:
                self._write_debug(
                    f"Received abort-signal from {record.abortion.origin} with"
                    + f" reason '{record.abortion.reason}' for job "
                    + f"'{record.info.token.value}'."
                )
        # skip ahead to cleanup in processing loop
        self._skip_sleep.set()
        if block:
            if token is None:
                while self._thread.is_alive():
                    sleep(0.001)
            else:
                while _token in self._jobs:
                    sleep(0.001)
