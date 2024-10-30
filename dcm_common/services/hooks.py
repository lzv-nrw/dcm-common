"""Hooks used in combination with the `orchestration`-subpackage."""

from typing import Optional, Callable

import requests
from flask import request

from dcm_common.logger import LoggingContext as Context
from dcm_common.models import Report
from dcm_common.orchestration import JobInfo, Job


def default_startup_hook(data, push):
    """
    Default hook for `Job` startup. Initializes `report.progress`.
    """
    data.progress.run()
    data.progress.numeric = 0
    data.progress.verbose = "starting up"
    push()


def default_fail_hook(data, push):
    """
    Default hook for `Job` fail. Finalizes `report.progress`.
    """
    data.progress.complete()
    data.progress.verbose = "shutting down after failure"
    push()


def default_abort_hook(data, push):
    """
    Default hook for `Job` abortion. Finalizes `report.progress`.
    """
    data.progress.abort()
    data.progress.verbose = "aborted"
    push()


def default_success_hook(data, push):
    """
    Default hook for `Job` success. Finalizes `report.progress`.
    """
    data.progress.complete()
    data.progress.numeric = 100
    data.progress.verbose = "shutting down after success"
    push()


def termination_callback_hook_factory(callback_url: Optional[str] = None):
    """
    Factory for termination callbacks. Returns function to be used as a
    `Job`'s 'completion'-hook.

    Keyword arguments:
    callback_url -- callback url
    """
    # pylint: disable=unused-argument
    def termination_callback_hook(data, push):
        if callback_url is not None:
            # make callback
            response = requests.post(
                callback_url,
                json=data.token.json,
                timeout=10
            )

            # if unexpected response code, write to log
            if response.status_code != 200:
                data.log.log(
                    Context.ERROR,
                    body=f"Failed callback to '{callback_url}'. Expected "
                    + f"status '200' but got '{response.status_code}'."
                )
    return termination_callback_hook


def pre_queue_hook_factory(report: type[Report]) -> Callable[[JobInfo], None]:
    """
    Returns a callable that can be used for the 'pre-queue'-hook of a
    `ScalableOrchestrator`.

    If executed, that callable instantiates a minimal `Report` of type
    `report`.
    """
    def pre_queue_hook(info: JobInfo):
        """Generate empty `Report`."""
        info.report = report(
            host=request.host_url,
            token=info.token,
            args=info.config.original_body
        )
    return pre_queue_hook


def pre_exec_hook_factory(
    report: type[Report]
) -> Callable[[JobInfo, Job], None]:
    """
    Returns a callable that can be used for the 'pre-exec'-hook of a
    `ScalableOrchestrator`.

    If executed, that callable
    * instantiates a `Report` of type `report`,
    * links the `job.log` to `report.log`, and
    * initializes that log with EVENT-messages regarding production
      and consumption of the associated token in the queue.
    """
    def pre_exec_hook(info: JobInfo, job: Job):
        """Generate and initialize `Report` using `info` and `job.log`."""
        info.report = report(
            host=info.report["host"],
            token=info.token,
            args=info.config.original_body,
            log=job.log
        )
        info.report.log.log(
            Context.EVENT,
            body=f"Produced at {info.metadata.produced.datetime} "
            + f"by '{info.metadata.produced.by}'."
        )
        info.report.log.log(
            Context.EVENT,
            body=f"Consumed at {info.metadata.consumed.datetime} "
            + f"by '{info.metadata.consumed.by}'."
        )
    return pre_exec_hook
