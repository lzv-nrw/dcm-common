"""
Report data-model definition
"""

from typing import Optional
from dataclasses import dataclass, field
from enum import Enum

from dcm_common import Logger
from . import JSONable
from .data_model import DataModel
from .token import Token


class Status(Enum):
    """
    Enum-class for different types of job-status.
    """

    QUEUED = "queued"
    RUNNING = "running"
    ABORTED = "aborted"
    COMPLETED = "completed"


@dataclass
class Progress(DataModel):
    """
    Progress `DataModel`

    Keyword arguments:
    status -- `Job`'s `Status` in processing pipeline
    verbose -- verbose description of `Job`-progress
    numeric -- numeric representation of `Progress` in percent
    """

    status: Status = Status.QUEUED
    verbose: str = field(default_factory=lambda: "")
    numeric: int = 0

    @DataModel.serialization_handler("status")
    @classmethod
    def status_serialization(cls, value):
        """Performs `status`-serialization."""
        return value.value

    @DataModel.deserialization_handler("status")
    @classmethod
    def status_deserialization(cls, value):
        """Performs `status`-deserialization."""
        return Status(value)

    def run(self) -> None:
        "Set `status`-property to RUNNING."
        self.status = Status.RUNNING

    def queue(self) -> None:
        "Set `status`-property to QUEUED."
        self.status = Status.QUEUED

    def abort(self) -> None:
        "Set `status`-property to ABORTED."
        self.status = Status.ABORTED

    def complete(self) -> None:
        "Set `status`-property to COMPLETED."
        self.status = Status.COMPLETED


@dataclass(kw_only=True)
class Report(DataModel):
    """
    Report `DataModel`

    Define child models based on `Report` by for example writing
     >>> @dataclass
     ... class ReportB(Report):
     ...    data: str
     ...    @property
     ...    def json(self) -> JSONable:
     ...        return super().json | {"data": self.data}

    Keyword arguments:
    host -- service url where this report has been generated
    token -- `Job`-token that is associated with this report
    args -- requestBody for this `Job`
    progress -- `Job` progress record
    log -- `Logger` containing `Job` related information
    """

    host: str
    token: Optional[Token] = None
    args: JSONable = None
    progress: Progress = field(default_factory=Progress)
    log: Logger = field(default_factory=Logger)

    @DataModel.serialization_handler("token")
    @classmethod
    def token_serialization(cls, value):
        """Performs `token`-serialization."""
        return value.json if value is not None else None

    @DataModel.serialization_handler("args")
    @classmethod
    def args_serialization(cls, value):
        """Performs `args`-serialization."""
        return value
