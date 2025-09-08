"""
This module contains the definition for different versions of the
`Daemon`-class.
"""

from typing import Optional, Callable, Mapping
import abc
from threading import Thread, Event
import sys


class Daemon(metaclass=abc.ABCMeta):
    """
    Base class for different types of daemon-implementations.
    """

    def __init__(self) -> None:
        self._daemon: Optional[Thread] = None
        self._stop = Event()
        self._service: Optional[Thread] = None
        self._skip_sleep = Event()

    @property
    def active(self) -> bool:
        """
        Returns `True` if the `Daemon` is currently running.
        """
        return self._daemon is not None and self._daemon.is_alive()

    @property
    def status(self) -> bool:
        """
        Returns `True` if currently both the `Daemon` is active and the
        service is alive.
        """
        return (
            self.active
            and self._service is not None
            and self._service.is_alive()
        )

    @abc.abstractmethod
    def _restart_service(self):
        """
        Generates and runs new `Thread`.
        """
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method "
            + "'_restart_service'."
        )

    def _serve(self, interval: float):
        """
        Loops until stopped. If `self._service` is down, restart.
        """
        while not self._stop.is_set():
            if self._service is None or not self._service.is_alive():
                try:
                    self._restart_service()
                except Exception as exc_info:
                    print(
                        "\033[31mERROR\033[0m Daemon encountered an "
                        + "unrecoverable error while trying to (re-)start a "
                        + f"service: {exc_info} Shutting down now..",
                        file=sys.stderr,
                    )
                    self._stop.set()
                    break
            self._skip_sleep.wait(interval)
            self._skip_sleep.clear()

    def run(
        self,
        interval: Optional[float] = None,
        daemon: bool = False,
        block: bool = False,
    ) -> None:
        """
        Start providing the service.

        Keyword arguments:
        interval -- interval for monitoring the service's status
                    (default 0.1)
        daemon -- run the `Daemon` as python `threading`-daemon
                  (default False)
        block -- if `True`, blocks until service is alive
                 (default False)
        """
        if self._daemon is not None and self._daemon.is_alive():
            return

        self._stop.clear()
        self._daemon = Thread(
            target=self._serve, daemon=daemon, args=(interval or 0.1,)
        )
        self._daemon.start()

        if block:
            while not self.status:
                pass

    def stop(self, block: bool = False) -> None:
        """
        Stop restarting the service.

        Keyword arguments:
        block -- if `True`, blocks until `Daemon` is inactive
                 (default False)
        """
        self._stop.set()
        self._skip_sleep.set()
        if block:
            while self.active:
                pass


class CDaemon(Daemon):
    """
    A `CDaemon` can be used to run a `threading.Thread` in the
    background. The `Daemon` will automatically restart a new `Thread`
    if the previous one has terminated.

    All positional and keyword arguments passed to the constructor
    are forwarded to the generated `Thread`s. Raises KeyError if
    `target` is not given as keyword argument (see `Thread`
    documentation).

    Note that the `Daemon` cannot stop the service itself. This has to
    be implemented by injecting custom `threading.Event`s via the
    configuration and setting up the `Thread`'s target to handle that
    `Event`.
    """

    def __init__(self, *args, **kwargs) -> None:
        self.configure(*args, **kwargs)
        super().__init__()

    @property
    def configuration(self) -> tuple[tuple, dict]:
        """
        Returns the current configuration as a tuple of `args` and
        `kwargs`.
        """
        return self._args, self._kwargs

    def reconfigure(self, **kwargs) -> None:
        """
        Re-configure keyword arguments for service.
        """
        self._kwargs.update(kwargs)

    def configure(self, *args, **kwargs) -> None:
        """
        Configure the service. Previous configuration is overwritten.
        """
        self._args = args
        if "target" not in kwargs:
            raise KeyError("Missing required keyword argument 'target'.")
        self._kwargs = kwargs

    def _restart_service(self):
        """
        Generates and runs new `Thread` using current configuration.
        """
        self._service = Thread(*self._args, **self._kwargs)
        self._service.start()


class FDaemon(Daemon):
    """
    A `FDaemon` can be used to run a `threading.Thread` in the
    background. The `Daemon` will automatically restart a new `Thread`
    if the previous one has terminated.

    It accepts a factory function that generates `threading.Thread`s as
    well as positional and keyword arguments that will then be forwarded
    into the factory when starting the service-thread.

    Note that the `Daemon` cannot stop the service itself. This has to
    be implemented by injecting custom `threading.Event`s via the
    configuration and setting up the `Thread`'s target to handle that
    `Event`.
    """

    def __init__(
        self,
        factory: Callable[[...], Thread],
        args: Optional[tuple] = None,
        kwargs: Optional[Mapping] = None,
    ) -> None:
        self._factory = factory
        self._args = args or ()
        self._kwargs = kwargs or {}
        super().__init__()

    @property
    def configuration(self) -> tuple[tuple, dict]:
        """
        Returns the current configuration as a tuple of `args` and
        `kwargs`.
        """
        return self._args, self._kwargs

    def reconfigure(self, **kwargs) -> None:
        """
        Re-configure keyword arguments for service.
        """
        self._kwargs.update(kwargs)

    def configure(self, *args, **kwargs) -> None:
        """
        Configure the service. Previous configuration is overwritten.
        """
        self._args = args
        self._kwargs = kwargs

    def _restart_service(self):
        """
        Generates and runs new `Thread` using the `factory`.
        """
        self._service = self._factory(*self._args, **self._kwargs)
        self._service.start()
