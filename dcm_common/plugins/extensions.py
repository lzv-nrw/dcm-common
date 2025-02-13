"""
Plugin-system interface extensions.
"""

from typing import Optional, Callable, Any, Mapping
from pathlib import Path
from time import sleep

from dcm_common.util import get_output_path
from dcm_common.logger import LoggingContext as Context, Logger


class FSPlugin:
    """
    PluginInterface-extension for plugins that use the file-system.

    Keyword arguments:
    working_dir -- working directory
    """

    def __init__(
        self,
        working_dir: Path,
        **kwargs,  # pylint: disable=unused-argument
    ) -> None:
        self.working_dir = working_dir
        super().__init__(**kwargs)

    def new_output(self) -> Optional[Path]:
        """
        Generates a unique identifier in `self.working_dir` and sets an
        fs-lock by creating the corresponding directory. Returns this
        directory as `Path` (`None` if not successful).
        """
        return get_output_path(self.working_dir)


class TimeoutAndRetryPlugin:
    """
    PluginInterface-extension for plugins that may encounter timeouts
    and perform retries. It implements a generic retry-mechanism that
    can be used in a plugin's `get`-method.

    Keyword arguments:
    timeout -- timeout duration in seconds; set to `None` for unlimited
               (default 30)
    retries -- maximum number of retries; set to `-1` for unlimited
               (default 1)
    retry_interval -- interval between retries in seconds
                      (default 10)
    """

    def __init__(
        self,
        timeout: Optional[float] = 30,
        retries: int = 1,
        retry_interval: float = 10,
        **kwargs,  # pylint: disable=unused-argument
    ) -> None:
        self.timeout = timeout
        self.retries = retries
        self.retry_interval = retry_interval
        super().__init__(**kwargs)

    def _retry(
        self,
        cmd: Callable[[], Any],
        args: Optional[tuple[Any, ...]] = None,
        kwargs: Optional[Mapping[str, Any]] = None,
        description: Optional[str] = None,
        exceptions: type[Exception] | tuple[type[Exception]] = TimeoutError,
    ) -> tuple[Logger, Optional[Any]]:
        """
        Execute `cmd` with `args` and `kwargs` up to `self._retries`
        times and return results if successful. On failed attempts, log
        accordingly.

        Keyword arguments:
        cmd -- callable that should be executed
        description -- task description used in generated `Logger`
                       (default None)
        exceptions -- tuple of exceptions identified as timeout
                      (default TimeoutError)
        """

        def try_again(retry: int) -> bool:
            """Returns `True` if another try should be made."""
            return self.retries < 0 or 0 <= retry <= self.retries

        result = None
        try:
            log = Logger(default_origin=self.name)  # pylint: disable=no-member
        except AttributeError:
            log = Logger(default_origin="Anonymous Plugin")
        retry = 0
        while try_again(retry):
            try:
                result = cmd(*(args or ()), **(kwargs or {}))
                break
            except exceptions:
                log.log(
                    Context.ERROR,
                    body="Encountered timeout"
                    + (f" while '{description}'" if description else "")
                    + f". (Attempt {retry + 1}/{self.retries + 1})",
                )
                retry += 1
                if try_again(retry):
                    sleep(self.retry_interval)
        return log, result
