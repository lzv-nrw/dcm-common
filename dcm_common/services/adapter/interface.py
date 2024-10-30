"""
This module defines the `ServiceAdapter`-interface.
"""

from typing import Optional, Any, Callable
from dataclasses import dataclass
import abc
from time import sleep, time
import json
from urllib3.exceptions import MaxRetryError, ReadTimeoutError

from dcm_common.logger import LoggingContext as Context, LogMessage
from dcm_common.models.report import Status
from dcm_common.models import Token
from dcm_common.models import JSONObject, DataModel


@dataclass
class APIResult(DataModel):
    """
    An `APIResult`-object aggregates relevant results of a single call
    to a DCM-API.
    """
    completed: bool = False
    success: Optional[bool] = None
    report: Optional[JSONObject] = None

    @DataModel.serialization_handler("report")
    @classmethod
    def report_serialization(cls, value):
        """Performs `report`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("report")
    @classmethod
    def report_deserialization(cls, value):
        """Performs `report`-deserialization."""
        if value is None:
            DataModel.skip()
        return value


class ServiceAdapter(metaclass=abc.ABCMeta):
    """
    Interface for service-specific instructions on how to run requests
    as well as retrieve and evaluate results.

    Keyword arguments:
    url -- service host-address
    interval -- polling interval for service reports in seconds
                (default 1)
    timeout -- timeout duration for the completion of a service job
               in seconds
               (default 360)
    """

    @classmethod
    def __subclasshook__(cls, subclass):
        return (
            hasattr(subclass, "_get_api_clients")
            and hasattr(subclass, "_get_api_endpoint")
            and hasattr(subclass, "_build_request_body")
            and hasattr(subclass, "success")
            and callable(subclass._get_api_clients)
            and callable(subclass._get_api_endpoint)
            and callable(subclass._build_request_body)
            and callable(subclass.success)
            or NotImplemented
        )

    _SERVICE_NAME = "<Interface>"
    _SDK = None
    REQUEST_TIMEOUT = 1

    def __init__(
        self, url: str, interval: float = 1, timeout: float = 360
    ) -> None:
        self._url = url
        self.interval = interval
        self.timeout = timeout
        self._default_api_client, self._api_client = self._get_api_clients()
        self._progress_endpoint = self._get_progress_endpoint(self._api_client)
        self._report_endpoint = self._get_report_endpoint(self._api_client)
        try:
            from pydantic_core import ValidationError
            self._ValidationError = ValidationError
        except ImportError:
            self._ValidationError = ValueError

    @abc.abstractmethod
    def _get_api_clients(self) -> tuple[Any, Any]:
        """
        Returns a tuple of default- and submission-related API clients.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} missing implementation of "
            + "`_get_api_clients`."
        )

    @abc.abstractmethod
    def _get_api_endpoint(self) -> Callable:
        """
        Returns the callable that is called for an API-request.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} missing implementation of "
            + "`_get_api_endpoint`."
        )

    @abc.abstractmethod
    def _build_request_body(
        self, base_request_body: dict, target: Any
    ) -> dict:
        """
        Returns the request body used for an API-request.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} missing implementation of "
            + "`_build_request_body`."
        )

    def _get_progress_endpoint(self, api: Any) -> Callable:
        """
        Returns the callable that is called for progress-updates during
        polling.

        It requires a signature of positional arg `token` (string) and
        keyword arg `_request_timeout`.
        """
        return getattr(api, "get_report")

    def _get_report_endpoint(self, api: Any) -> Callable:
        """
        Returns the callable that is called for fetching the current
        report.

        It requires a signature of positional arg `token` (string) and
        keyword arg `_request_timeout`.
        """
        return getattr(api, "get_report")

    @abc.abstractmethod
    def success(self, info: APIResult) -> bool:
        """
        Returns `True` if the `Stage` associated with this adapter has
        been completed and was successful (based on the given
        `APIResult.report`).
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} missing implementation of `success`."
        )

    def _finalize_with_error(
        self, info: APIResult, status_msg: str, log_msg: str,
        args: Optional[dict] = None
    ) -> None:
        """
        Helper for (initializing a report-JSON if previously empty and)
        finalizing a report on error.
        """
        info.success = False
        info.completed = True
        if info.report is None:
            info.report = {}
        if "host" not in info.report:
            info.report["host"] = self._url
        if "token" not in info.report:
            info.report["token"] = Token(True, 0).json
        if "args" not in info.report:
            info.report["args"] = args or {}
        if "progress" not in info.report:
            info.report["progress"] = {"numeric": 0}
        info.report["progress"]["status"] = Status.COMPLETED.value
        info.report["progress"]["verbose"] = status_msg
        if "log" not in info.report:
            info.report["log"] = {}
        if Context.ERROR.name not in info.report["log"]:
            info.report["log"][Context.ERROR.name] = []
        info.report["log"][Context.ERROR.name].append(
            LogMessage(
                body=log_msg,
                origin=f"{self._SERVICE_NAME}-Adapter"
            ).json
        )

    def submit(
        self, endpoint: Optional[Callable], request_body: dict, info: APIResult
    ) -> Optional[Any]:
        """
        Attempt to submit a job to a service. Returns token-object if
        successful.

        Keyword arguments:
        endpoint -- override for the API endpoint to submit to
        request_body -- submission's request body
        info -- `APIResult` associated with this call
        """
        return self._submit(
            endpoint or self._get_api_endpoint(), request_body, info
        )

    def _submit(
        self, endpoint: Callable, request_body: dict, info: APIResult
    ) -> Optional[Any]:
        """
        Attempt to submit a job to a service. Returns token-object if
        successful.
        """
        try:
            response = endpoint(
                request_body, _request_timeout=self.REQUEST_TIMEOUT
            )
        except ReadTimeoutError as exc_info:
            self._finalize_with_error(
                info,
                status_msg="connection timed out",
                log_msg=f"Cannot connect to service at '{self._url}' "
                + f"({exc_info}).",
                args=request_body
            )
            return None
        except MaxRetryError as exc_info:
            self._finalize_with_error(
                info,
                status_msg="no connection",
                log_msg=f"Cannot connect to service at '{self._url}' "
                + f"({exc_info}).",
                args=request_body
            )
            return None
        except self._SDK.exceptions.ApiException as exc_info:
            self._finalize_with_error(
                info,
                status_msg=f"submission rejected ({exc_info.body})",
                log_msg=f"Service at '{self._url}' rejected submission: "
                + f"{exc_info.body} ({exc_info.status})",
                args=request_body
            )
            return None
        except self._ValidationError as exc_info:
            if not hasattr(exc_info, "errors"):  # ValueError
                self._finalize_with_error(
                    info,
                    status_msg="error while making request",
                    log_msg="An Error occurred while making a request: "
                    + f"{exc_info.title}.",
                    args=request_body
                )
                return None
            for error in exc_info.errors():  # pydantic_core.ValidationError
                self._finalize_with_error(
                    info,
                    status_msg="invalid request body",
                    log_msg=f"Bad request body for '{exc_info.title}' "
                    + f"({error['msg']}; {error['type']} at {error['loc']}).",
                    args=request_body
                )
            return None
        self._update_info_report(
            {
                "host": self._url,
                "token": response.to_dict(),
                "args": request_body,
                "progress": {
                    "status": Status.QUEUED.value,
                    "verbose": f"queued by {self._SERVICE_NAME}-adapter",
                    "numeric": 0
                }
            },
            info
        )
        return response

    def poll(
        self, token: str, info: APIResult,
        update_hooks: Optional[tuple[Callable[[str], None], ...]] = None
    ) -> None:
        """
        Enter loop to poll service for job reports until termination or
        timeout.

        Keyword arguments:
        token -- job token value
        info -- `APIResult` associated with this call
        update_hooks -- hooks that are run when info is updated during
                        polling; get passed the token value
                        (default None)
        """
        self._poll(token, info, update_hooks)

    @staticmethod
    def _run_hooks(hooks, data):
        """Run `hooks` if defined and pass `data` as positional arg."""
        if hooks:
            for hook in hooks:
                hook(data)

    def _poll(
        self, token: str, info: APIResult,
        update_hooks: Optional[tuple[Callable[[str], None], ...]] = None
    ) -> None:
        """
        Enter loop to poll service for job reports until termination or
        timeout.
        """
        t0 = time()
        while time() - t0 < self.timeout:
            self.get_info(token, info=info)
            self._run_hooks(update_hooks, info)
            if info.completed:
                return
            sleep(self.interval)
        self._finalize_with_error(
            info,
            status_msg="service timed out",
            log_msg=f"Service at '{self._url}' has timed out after "
            + f"{self.timeout} seconds."
        )
        self._run_hooks(update_hooks, info)

    def run(
        self, base_request_body: dict, target: Any, info: APIResult,
        post_submission_hooks: Optional[tuple[Callable[[str], None], ...]] = None,
        update_hooks: Optional[tuple[Callable[[str], None], ...]] = None
    ) -> None:
        """
        Make a synchronous call to the associated service and
        continuously poll for the report (stored in given
        `info.report`).

        Keyword arguments:
        base_request_body -- request body for the given service as
                             passed to the Job Processor service
        target -- additional target-information from previous `Stage` if
                  applicable
        info -- `APIResult` associated with this call
        post_submission_hooks -- hooks that are run after successful
                                 submission; get passed the token value
                                 (default None)
        update_hooks -- hooks that are run when info is updated during
                        polling; get passed the info value
                        (default None)
        """
        # submit job
        response = self._submit(
            self._get_api_endpoint(),
            self._build_request_body(base_request_body, target),
            info
        )
        if not response:
            return

        self._run_hooks(post_submission_hooks, response.value)

        # poll for results
        self._poll(response.value, info, update_hooks or ())

    def _update_info_report(self, data: Any, info: APIResult) -> None:
        """
        Writes updates from data to info (e.g. during polling, progress
        information is written to info that way).
        """
        if info.report is None:
            info.report = {}
        info.report.clear()
        info.report.update(data or {})

    def get_info(
        self,
        token: str,
        endpoint: Optional[Callable] = None,
        info: Optional[APIResult] = None
    ) -> APIResult:
        """
        Returns the current `APIResult` for the given `token`.

        The optional arg `endpoint` can be used as override for the
        adapter's pre-defined polling-progress endpoint. It requires a
        signature of positional arg `token` (string) and keyword arg
        `_request_timeout`. Setting this will skip evaluation of
        `APIResult.success` and `APIResult.completed`.

        The arg `info` can be used to automatically apply results
        instead of creating a new instance.

        Keyword arguments:
        token -- token value associated with the job
        endpoint -- override for the default progress-endpoint
                    (default None)
        info -- `APIResult`-instance for output
                (default None)
        """
        if info is None:
            _info = APIResult()
        else:
            _info = info
        try:
            response = (endpoint or self._progress_endpoint)(
                token, _request_timeout=self.REQUEST_TIMEOUT
            )
            self._update_info_report(response.to_dict(), _info)
            if endpoint is None:
                _info.success = self.success(_info)
                _info.completed = True
        except ReadTimeoutError as exc_info:
            self._finalize_with_error(
                _info,
                status_msg="connection timed out",
                log_msg=f"Cannot connect to service at '{self._url}' "
                + f"({exc_info})."
            )
        except MaxRetryError as exc_info:
            self._finalize_with_error(
                _info,
                status_msg="no connection",
                log_msg=f"Cannot connect to service at '{self._url}' "
                + f"({exc_info})."
            )
        except self._SDK.exceptions.ApiException as exc_info:
            if exc_info.status == 503:
                self._update_info_report(json.loads(exc_info.data), _info)
            else:
                self._finalize_with_error(
                    _info,
                    status_msg=f"unknown error ({exc_info.status})",
                    log_msg=f"Service at '{self._url}' responded with an "
                    + f"unknown error: {exc_info.body} ({exc_info.status})"
                )
        return _info

    def get_report(self, token: str) -> Optional[JSONObject]:
        """
        Get the report for a job and update the APIResult.
        Returns False if the report is incomplete.

        Keyword arguments:
        token -- token value associated with the job
        """
        return self.get_info(token, self._report_endpoint).report
