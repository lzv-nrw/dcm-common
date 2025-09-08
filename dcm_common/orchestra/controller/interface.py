"""
Definition of an interface for `orchestra.Controller` implementations.
"""

from typing import Optional, Any, Mapping
import abc
from datetime import datetime

from ..models import (
    Token,
    JobInfo,
    Lock,
    Message,
)


class Controller(metaclass=abc.ABCMeta):
    """
    A Controller provides an interface for a worker to access a job-
    registry and the orchestration-related messaging system. This
    involves
    * keeping track of queued, running, .. jobs,
    * handling worker-requests for queued jobs,
    * storing job results posted by workers, and
    * keeping record of orchestration-related messages.
    """

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Returns controller name."""
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define property "
            + "'name'."
        )

    @abc.abstractmethod
    def queue_push(self, token: str, info: Mapping | JobInfo) -> Token:
        """
        Add job to queue, returns `Token` if successful or already
        existing or `None` otherwise.

        If `info` is not passed as `JobInfo`, adds the `token` and
        `produced`-metadata before submission.
        """
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method "
            + "'queue_push'."
        )

    @abc.abstractmethod
    def queue_pop(self, name: str) -> Optional[Lock]:
        """Request a lock on a job from the queue."""
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method "
            + "'queue_pop'."
        )

    @abc.abstractmethod
    def release_lock(self, lock_id: str) -> None:
        """Releases a lock on a job from the queue."""
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method "
            + "'release_lock'."
        )

    @abc.abstractmethod
    def refresh_lock(self, lock_id: str) -> Lock:
        """
        Refreshes a lock on a job from the queue. Raises `ValueError` if
        not successful.
        """
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method "
            + "'refresh_lock'."
        )

    @abc.abstractmethod
    def get_token(self, token: str) -> Token:
        """Fetch token-data from registry."""
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method "
            + "'get_token'."
        )

    @abc.abstractmethod
    def get_info(self, token: str) -> Any:
        """Fetch info from registry as JSON."""
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method "
            + "'get_info'."
        )

    @abc.abstractmethod
    def get_status(self, token: str) -> str:
        """Fetch status from registry."""
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method "
            + "'get_status'."
        )

    @abc.abstractmethod
    def registry_push(
        self,
        lock_id: str,
        *,
        status: Optional[str] = None,
        info: Optional[Mapping | JobInfo] = None,
    ) -> None:
        """Push new data to registry."""
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method "
            + "'registry_push'."
        )

    @abc.abstractmethod
    def message_push(
        self, token: str, instruction: str, origin: str, content: str
    ) -> None:
        """Posts message."""
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method "
            + "'message_push'."
        )

    @abc.abstractmethod
    def message_get(self, since: Optional[datetime | int]) -> list[Message]:
        """Returns a list of relevant messages."""
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method "
            + "'message_get'."
        )
