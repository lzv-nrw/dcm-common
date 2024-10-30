"""
Module for the `Job`-class definition.
"""

from typing import Any, Optional, Callable, Mapping
from os import chdir
from pathlib import Path
import traceback
from uuid import uuid4
from dataclasses import dataclass, field
from multiprocessing import Process, Pipe
from threading import RLock, Event
import inspect

import requests

from dcm_common import LoggingContext as Context, Logger


def default_child_report_getter(data: Any, child: "ChildJobEx") -> Mapping:
    """Returns `data.children` if exists, otherwise an empty mapping."""
    if data.children is None:
        data.children = {}
    return data.children


# this class defines how to make calls instead of accepting callbacks/
# hooks since the contents are subject to (de-)pickling
@dataclass
class ChildJobEx:
    """
    Extension for legacy record-class `ChildJob`. Used to automatically
    abort children if owner is aborted.

    Keyword arguments:
    token -- associated JobToken value
    url -- base for request
    abort_path -- url path used for the abort call
    abort_method -- http-method used for the abort call
                    (default 'delete')
    abort_status_ok -- validation whether child completed abort
                       successfully
                       (default 200)
    abort_kwargs -- request kwargs (kwargs passed into
                    `requests.<method>`)
                    (default {})
    collect_report -- whether to collect report after aborting
                      (default True)
    report_path -- url path used for the call to collect a report
                   (default '/report')
    report_method -- http-method used for the report call
                     (default 'get')
    report_status_ok -- validation whether child report collection is
                        successfully
                        (default 200)
    id_ -- key for the target entry of this report in
           `Job.data['report']['children']`; required if
           `collect_report`
           (default None)
    report_target_destination -- getter for the target destination of
                                 the child report; is passed `Job.data`
                                 and the child-instance;
                                 note that since this object is subject
                                 to (un-)pickling, this getter must not
                                 be a local definition
                                 (default default_child_report_getter;
                                 points to data.children)
    report_kwargs -- request kwargs (kwargs passed into
                    `requests.<method>`)
                    (default {})
    """
    token: str
    url: str
    abort_path: str
    abort_method: str = field(default_factory=lambda: "delete")
    abort_status_ok: int = 200
    abort_kwargs: dict = field(default_factory=dict)
    collect_report: bool = True
    report_path: str = field(default_factory=lambda: "/report")
    report_method: str = field(default_factory=lambda: "get")
    report_status_ok: int = 200
    id_: Optional[str] = None
    report_target_destination: Callable[[Any], Mapping] = default_child_report_getter
    report_kwargs: dict = field(default_factory=dict)

    def _format_bad_response(self, msg: str) -> tuple[bool, str]:
        return (
            False,
            f"Abort for token '{self.token}' at '{self.url}' failed: {msg}."
        )

    def abort(
        self, origin: Optional[str] = None, reason: Optional[str] = None
    ) -> tuple[bool, str]:
        """
        Abort this child process. Returns tuple of success and (if
        failed) a message.

        Keyword arguments:
        reason -- reason for abortion; if provided, it is added to
                  `kwargs['json']`
                  (default None)
        origin -- origin of abortion; if provided, it is added to
                  `kwargs['json']`
                  (default None)
        """
        if "params" not in self.abort_kwargs:
            self.abort_kwargs["params"] = {}
        self.abort_kwargs["params"]["token"] = self.token
        if "json" not in self.abort_kwargs:
            self.abort_kwargs["json"] = {}
        if reason:
            self.abort_kwargs["json"]["reason"] = reason
        if origin:
            self.abort_kwargs["json"]["origin"] = origin
        try:
            response = requests.request(
                method=self.abort_method,
                url=f"{self.url}{self.abort_path}",
                **self.abort_kwargs
            )
        except requests.exceptions.RequestException as exc_info:
            return self._format_bad_response(str(exc_info))
        if response.status_code != self.abort_status_ok:
            return self._format_bad_response(
                f"Received unexpected status code '{response.status_code}' "
                + f"(expected '{self.abort_status_ok}')"
            )
        return True, ""

    def get_report(self) -> tuple[bool, str, Optional[dict]]:
        """
        Returns tuple of success, (if failed) a message, and (if
        successful) the report as dictionary.
        """
        try:
            response = requests.request(
                method=self.report_method,
                url=f"{self.url}{self.report_path}?token={self.token}",
                **self.report_kwargs
            )
        except requests.exceptions.RequestException as exc_info:
            return self._format_bad_response(str(exc_info)) + (None,)
        if response.status_code != self.report_status_ok:
            return self._format_bad_response(
                f"Received unexpected status code '{response.status_code}' "
                + f"(expected '{self.report_status_ok}')"
            ) + (None,)
        return True, "", response.json()


def ChildJob(  # FIXME: legacy-support
    url: str,
    token: str,
    status_ok: int = 200,
    kwargs: Optional[dict] = None,
    method: Optional[str] = None
) -> ChildJobEx:
    """
    DEPRECATED - USE class `ChildJobEx` instead

    Record for a Child-Job. Used to automatically abort children if
    owner is aborted.

    Keyword arguments:
    url -- url to which the request is sent
    token -- associated JobToken value
    status_ok -- validation whether child completed abort successfully
                 (default 200)
    kwargs -- request kwargs (kwargs passed into `requests.<method>`)
              (default {})
    method -- http-method used for the call
              (default "delete")
    """
    return ChildJobEx(
        token=token,
        url=url,
        abort_path="",
        abort_method=method or "delete",
        abort_status_ok=status_ok,
        abort_kwargs=kwargs or {},
        collect_report=False
    )


class Children(dict):
    """Container type for `ChildJobEx`s."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.lock = RLock()

    def add(
        self, child: ChildJobEx, tag: Optional[str] = None
    ) -> str:
        """
        Register `child` with `tag` (new tag is generated if none
        provided). Returns tag.
        """
        with self.lock:
            self[_tag := (tag or str(uuid4()))] = child
        return _tag

    def remove(self, tag: str) -> None:
        """Remove child associated with `tag`."""
        with self.lock:
            del self[tag]

    def link(  # FIXME: legacy-support
        self, url: str, tag: Optional[str] = None,
        push: Optional[Callable[[], None]] = None
    ) -> Callable[[str], None]:
        """
        Add `ChildJob` with generated token to this `Children`-
        instance.

        Keyword arguments:
        url -- base url for other service
        tag -- tag used to identify children in the context of this
               `Children` instance
               (default None; uses token)
        push -- hook that is executed after successful linking
                (default None)
        """
        def _(token: str) -> None:
            self.add(
                ChildJob(url, token), tag or token
            )
            push()
        return _

    def link_ex(
        self, url: str, abort_path: str, tag: Optional[str] = None,
        child_id: Optional[str] = None,
        post_link_hook: Optional[Callable[[], None]] = None,
        report_target_destination=default_child_report_getter
    ) -> Callable[[str], None]:
        """
        Add `ChildJobEx` with generated token to this `Children`-
        instance.

        Keyword arguments:
        url -- base url for other service
        abort_path -- url path for abort requests
        tag -- tag used to identify children in the context of this
               `Children` instance
               (default None; uses token)
        child_id -- child-report id (used when fetching child report
                    after abortion)
                    (default None)
        post_link_hook -- hook that is executed after successful linking
                          (default None)
        report_target_destination -- getter for child report target
                                     destination (see `ChildJobEx`)
                                     (default default_child_report_getter)
        """
        def _(token: str) -> None:
            self.add(
                ChildJobEx(
                    token=token, url=url, abort_path=abort_path, id_=child_id,
                    report_target_destination=report_target_destination
                ), tag or token
            )
            if post_link_hook:
                post_link_hook()
        return _

    # __getstate__ and __setstate__ are needed to skip attempting to
    # (un-)pickle _lock
    def __getstate__(self):
        """Return state values to be pickled."""
        state = self.__dict__.copy()
        del state["lock"]
        return state

    def __setstate__(self, state):
        """Restore state from the unpickled state values."""
        self.__dict__.update(state)
        if "lock" not in self.__dict__:
            self.lock = RLock()


@dataclass
class _JobStatus:
    """Job-Status record class."""
    running: bool = False
    completed: bool = False
    aborted: bool = False
    status_code: int = 0
    log: Logger = field(default_factory=Logger)
    data: Optional[Any] = None
    children: Children = field(default_factory=Children)  # dict[str, ChildJobEx]


class Job:
    """
    Record-class that stores `Job`-related information.

    The properties `completed`, `running`, and `aborted` are used by a
    `ScalableOrchestrator` to perform scheduling tasks. The actions of a
    `Job` are defined by the function `cmd`. These tasks are run as a
    separate process. In order to exchange data with this process, the
    object `data` can be defined here. This chunk of data can then be
    sent back to the host processes within the given `cmd` by calling
    `push`. The `Job.log` (`Logger`) can be used to log messages.

    Keyword arguments:
    name -- used to set `default_origin` in `Logger`
            (default None; uses uuid4)
    cmd -- job-instructions as function
            def cmd(data: Any, push: Callable):
                ...
            where
            data -- any object that serves as input and output for
                    the child process
            push -- callable that has to be executed to update the
                    `data`-object (like progress) in the host-process
            (default None: log a default-message in `Job.log` and
                            exit (ignore hooks))
    data -- any object, is made available in cmd
            (default None)
    hooks -- hook-dictionary containing keys for key-stages in `Job`-
             execution:
             * `startup`: startup hook
             * `progress`: executed on `push()`
             * `fail`: fail hook
             * `success`: success hook
             * `completion`: completion hook
             * `abort`: abortion hook
             where the values are functions with positional arguments
             f(data: Any, push: Callable[[],])

             (default None: no hooks are executed)
            (default None)
    """

    def __init__(
        self,
        name: Optional[str] = None,
        cmd: Optional[Callable] = None,
        data: Optional[Any] = None,
        hooks: Optional[dict[str, Callable]] = None,
    ) -> None:
        self._name = name if name is not None else str(uuid4())
        self._status = _JobStatus(log=Logger(default_origin=self._name))
        self._p = None
        self._run = None
        self._hooks = {}
        self.configure(cmd=cmd, data=data, hooks=hooks)
        self._abort = Event()
        self._status_lock = RLock()

    def configure(
        self,
        cmd: Optional[Callable] = None,
        data: Optional[Any] = None,
        hooks: Optional[dict[str, Callable]] = None
    ) -> None:
        """
        (Re-)configure `Job`.

        Keyword arguments:
        cmd -- job-instructions as function
               def cmd(data: Any, push: Callable, children: Children):
                   ...
               where
               data -- any object that serves as input and output for
                       the child process
               push -- callable that has to be executed to update the
                       `data`-object (like progress) in the host-process
               children -- optional `Children`-container; can be omitted
               lock -- optional `threading.RLock` also used in push and
                       `Children`-methods; can be omitted
               (default None: log a default-message in `Job.log` and
                              exit (ignore hooks))
        data -- any object, is made available in cmd
                (default None)
        hooks -- hook-dictionary containing keys for key-stages in `Job`-
                execution:
                * `startup`: startup hook
                * `progress`: executed on `push()`
                * `fail`: fail hook
                * `success`: success hook
                * `completion`: completion hook
                * `abort`: abortion hook
                where the values are functions with positional arguments
                f(data: Any, push: Callable[[],])

                (default None: no hooks are executed)
        """
        if cmd is not None or self._run is None:
            # decorate cmd-instructions
            def _run(pipe) -> None:
                status: _JobStatus = pipe.recv()
                lock = status.children.lock

                def _push():
                    with lock:
                        self._execute_hook(
                            "progress", status.log, status.data, lambda: None
                        )
                        pipe.send(status)

                status.log.log(
                    Context.EVENT,
                    body="Start executing Job."
                )
                status.running = True
                with lock:
                    pipe.send(status)

                # dummy-jobs
                if cmd is None:
                    status.log.log(
                        Context.INFO,
                        body="Dummy Job finished successfully."
                    )
                    status.running = False
                    status.completed = True
                    with lock:
                        pipe.send(status)
                    return

                # non-dummy jobs
                # this try-except-block is needed to prevent crashed jobs
                # getting stuck in queue, i.e., to reset job-state booleans
                self._execute_hook(
                    "startup", status.log, status.data, _push
                )
                try:
                    kwargs = {
                        "data": status.data,
                        "push": _push
                    }
                    if "children" in inspect.getfullargspec(cmd).args:
                        kwargs["children"] = status.children
                    if "lock" in inspect.getfullargspec(cmd).args:
                        kwargs["lock"] = lock
                    cmd(**kwargs)
                except Exception:
                    status.status_code = 1
                    status.log.log(
                        Context.ERROR,
                        body="Unhandled Exception.\n"
                        + traceback.format_exc()
                    )
                    self._execute_hook(
                        "fail", status.log, status.data, _push
                    )
                else:
                    status.log.log(
                        Context.EVENT,
                        body="Job terminated normally."
                    )
                    self._execute_hook(
                        "success", status.log, status.data, _push
                    )
                self._execute_hook(
                    "completion", status.log, status.data, _push
                )
                status.running = False
                status.completed = True
                with lock:
                    pipe.send(status)
            self._run = _run
        if data is not None:
            self._status.data = data
        if hooks is not None:
            self._hooks.update(hooks)

    @property
    def name(self) -> str:
        """
        Returns job's name.
        """
        return self._name

    @property
    def hooks(self) -> dict[str, Callable]:
        "Returns a shallow copy of the current hook-configuration."
        return self._hooks.copy()

    @property
    def aborted(self) -> bool:
        """
        Returns `True` if the `Job` has been aborted.
        """
        return self._status.aborted

    @property
    def running(self) -> bool:
        """
        Returns `True` if the `Job` is currently running and `False`
        otherwise.
        """
        return self._status.running

    @property
    def completed(self) -> bool:
        """
        Returns `True` if the `Job` is completed and `False` otherwise.
        """
        return self._status.completed

    @property
    def exit_code(self) -> int:
        """
        Returns 0 if the command exited normally and 1 if an error occurred.
        """
        return self._status.status_code

    @property
    def log(self) -> Logger:
        """
        Returns `log`.
        """
        return self._status.log

    @property
    def data(self):
        """
        Returns `data`-object.
        """
        return self._status.data

    @property
    def children(self):
        """
        Returns `Children`-object.
        """
        return self._status.children

    @property
    def _is_alive(self) -> bool:
        """
        Returns `True` if a `Process` has been defined and returns o
        `is_alive` with `True`.
        """
        if isinstance(self._p, Process):
            return self._p.is_alive()
        return False

    def run(self, cwd: Optional[Path] = None) -> int:
        """
        Start `Job` execution.

        Keyword arguments:
        cwd -- can be used to set a different working dir for this`Job`
               (default None; corresponds to unchanged cwd)
        """
        if not self._is_alive:
            if self._status.completed \
                    or self._status.aborted \
                    or self._status.running:
                raise RuntimeError(
                    "Attempted to start job which has been started previously."
                    + f"(name: {self._name}, status: {self._status})."
                )
            def __run(pipe, cwd):
                if cwd is not None:
                    chdir(cwd)
                self._run(pipe)

            pipe_parent, pipe_child = Pipe()
            self._p = Process(target=__run, args=(pipe_child, cwd))
            self._p.start()
            pipe_parent.send(self._status)
            # fetch updates until job is completed
            while not (self._status.completed or self._status.aborted):
                if pipe_parent.poll(timeout=0.1):
                    # duplicate abort>break-check since poll is used
                    # with timeout (needs to be checked before actually
                    # applying the result) but abort should work even
                    # when nothing is written to pipe
                    if self._abort.is_set():
                        break
                    with self._status_lock:
                        self._status = pipe_parent.recv()
                # second abort>break-check
                if self._abort.is_set():
                    break
            self._p.join(timeout=0.1)
            if self._p.is_alive():
                self._p.kill()
            return self._status.status_code
        return 2  # cannot start job twice at the same

    def abort(
        self, reason: Optional[str] = None, origin: Optional[str] = None
    ) -> None:
        """
        Abort `Job` if currently running and mark as aborted.
        """
        self._abort.set()
        if self._is_alive:
            self._p.kill()
        with self._status_lock:
            self._status.running = False
            self._status.completed = True
            self._status.aborted = True
            self._status.status_code = 3
            if reason is not None:
                _reason = f" ({reason})"
            else:
                _reason = ""
            if origin is not None:
                _origin = f" by '{origin}'"
            else:
                _origin = ""
            self._status.log.log(
                Context.ERROR,
                body=f"Received SIGKILL{_reason}{_origin}."
            )
            for tag, child in self._status.children.items():
                try:
                    self._status.log.log(
                        Context.INFO,
                        body=f"Aborting child '{tag}' at '{child.url}' using "
                        + f"token '{child.token}'."
                    )
                    result, msg = child.abort(origin=origin, reason=reason)
                    if not result:
                        self._status.log.log(
                            Context.ERROR,
                            body=f"Error while aborting child '{tag}' at "
                            + f"'{child.url}' using token '{child.token}': "
                            + msg
                        )
                        continue
                    if not child.collect_report:
                        continue
                    result, msg, report = child.get_report()
                    if not result:
                        self._status.log.log(
                            Context.ERROR,
                            body="Error while fetching report of child "
                            + f"'{tag}' at '{child.url}' using token "
                            + f"'{child.token}': {msg}"
                        )
                        continue
                    try:
                        child_report_target = child.report_target_destination(
                            self._status.data, child
                        )
                    except AttributeError:
                        self._status.log.log(
                            Context.ERROR,
                            body=f"Error while writing report of child '{tag}'"
                            + f" at '{child.url}' using token '{child.token}':"
                            + " Report does not define a 'children'-attribute."
                        )
                    child_report_target[child.id_].clear()
                    child_report_target[child.id_].update(report)
                # pylint: disable=broad-exception-caught
                except Exception as exc_info:
                    self._status.log.log(
                        Context.ERROR,
                        body=(
                            f"Unknown error while aborting child '{tag}': "
                            + str(exc_info)
                        )
                    )

            self._execute_hook(
                "abort",
                self._status.log,
                self._status.data,
                lambda: self._execute_hook(
                    "progress",
                    self._status.log,
                    self._status.data,
                    lambda: None
                )
            )
        self._abort.clear()

    def reset(self, data: Optional[Any] = None) -> None:
        """
        Reset `Job` after previous `run`.

        Keyword arguments:
        data -- any object, is made available in cmd
                (default None)
        """
        self.abort("Resetting.")
        self._abort.clear()
        self.configure(data=data)
        self._status = _JobStatus(
            data=data or self._status.data,
            log=Logger(default_origin=self._name)
        )

    def _execute_hook(
        self,
        hook: str,
        log: Logger,
        data: Any,
        push: Callable[[], None]
    ) -> None:
        try:
            if hook in self._hooks:
                self._hooks[hook](data, push)
        except Exception:
            log.log(
                Context.ERROR,
                body=f"Unhandled Exception in hook '{hook}'.\n"
                    + traceback.format_exc()
            )
