from .dilled import DillIgnore, DilledProcess, DilledPipe, dillignore
from .controller import (
    Controller,
    SQLiteController,
    HTTPController,
    get_http_controller_bp,
)
from .worker import Worker
from .pool import WorkerPool
from .models.context import ProcessContext, JobContext
from .models.info import JobConfig, MetadataRecord, JobMetadata, JobInfo
from .models.lock import Lock
from .models.message import Instruction, Message
from .models.report import Status, Progress, Report
from .models.token import Token


__all__ = [
    "DillIgnore",
    "DilledProcess",
    "DilledPipe",
    "dillignore",
    "Controller",
    "SQLiteController",
    "HTTPController",
    "get_http_controller_bp",
    "Worker",
    "WorkerPool",
    "ProcessContext",
    "JobContext",
    "JobConfig",
    "MetadataRecord",
    "JobMetadata",
    "JobInfo",
    "Lock",
    "Instruction",
    "Message",
    "Status",
    "Progress",
    "Report",
    "Token",
]


import os
import multiprocessing


try:
    multiprocessing.set_start_method(
        os.environ.get("ORCHESTRA_MP_METHOD", "spawn")
    )
except RuntimeError:
    pass
