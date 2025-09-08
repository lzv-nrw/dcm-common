"""Base configuration class for DCM web-services."""

import os
import sys
from pathlib import Path
import importlib.metadata
import json

from dcm_common.db import SQLiteAdapter3, PostgreSQLAdapter14
from dcm_common.orchestra import (
    SQLiteController,
    HTTPController,
    WorkerPool,
    dillignore,
)


# pylint: disable=invalid-name


class BaseConfig:
    """
    Base configuration class for DCM web-services.
    """

    # allow CORS (requires python package Flask-CORS)
    ALLOW_CORS = (int(os.environ.get("ALLOW_CORS") or 0)) == 1

    def __init__(self) -> None:
        self.CONTAINER_SELF_DESCRIPTION = {}
        self.set_identity()

    def set_identity(self) -> None:
        """
        Load dictionary with self-description based on current settings.

        When inheriting from this config-class with a custom
        self-description, the `set_identity`-default can be loaded with
        `super().set_identity()`.
        """
        self.CONTAINER_SELF_DESCRIPTION = {
            "description": "unconfigured service",
            "version": {
                "api": None,
                "app": None,
                "python": sys.version,
                "software": {},
                "lib": {
                    lib.name: lib.version
                    for lib in importlib.metadata.distributions()
                },
            },
            "configuration": {
                "settings": {
                    "allow_cors": self.ALLOW_CORS,
                },
                "plugins": {},
                "services": {},
            },
        }


@dillignore("controller", "worker_pool")
class OrchestratedAppConfig(BaseConfig):
    """
    Configuration class extension for DCM web-services that use an
    orchestrator.
    """

    ORCHESTRA_AT_STARTUP = (
        int(os.environ.get("ORCHESTRATION_AT_STARTUP") or 1)
    ) == 1
    ORCHESTRA_WORKER_POOL_SIZE = int(
        os.environ.get("ORCHESTRA_WORKER_POOL_SIZE") or 1
    )
    ORCHESTRA_DAEMON_INTERVAL = float(
        os.environ.get("ORCHESTRA_DAEMON_INTERVAL") or 10
    )
    ORCHESTRA_CONTROLLER = os.environ.get("ORCHESTRA_CONTROLLER", "sqlite")
    ORCHESTRA_CONTROLLER_ARGS = json.loads(
        os.environ.get("ORCHESTRA_CONTROLLER_ARGS", "{}")
    )
    ORCHESTRA_WORKER_ARGS = json.loads(
        os.environ.get("ORCHESTRA_WORKER_ARGS", "{}")
    )
    ORCHESTRA_WORKER_INTERVAL = float(
        os.environ.get("ORCHESTRA_WORKER_INTERVAL") or 1
    )
    ORCHESTRA_ABORT_TIMEOUT = float(
        os.environ.get("ORCHESTRA_ABORT_TIMEOUT", 30)
    )

    def __init__(self) -> None:
        match self.ORCHESTRA_CONTROLLER:
            case "sqlite":
                self.controller = SQLiteController(
                    **self.ORCHESTRA_CONTROLLER_ARGS.copy()
                )
            case "http":
                self.controller = HTTPController(
                    **self.ORCHESTRA_CONTROLLER_ARGS.copy()
                )
            case _:
                raise ValueError(
                    "Unknown orchestra-controller type "
                    + f"'{self.ORCHESTRA_CONTROLLER}'"
                )

        self.worker_pool = WorkerPool(
            self.controller,
            self.ORCHESTRA_WORKER_POOL_SIZE,
            self.ORCHESTRA_WORKER_ARGS.copy(),
        )

        super().__init__()

    def set_identity(self):
        super().set_identity()

        self.CONTAINER_SELF_DESCRIPTION["configuration"]["settings"][
            "orchestra"
        ] = {
            "atStartup": self.ORCHESTRA_AT_STARTUP,
            "poolSize": self.ORCHESTRA_WORKER_POOL_SIZE,
            "daemonInterval": self.ORCHESTRA_DAEMON_INTERVAL,
            "controller": {
                "type": self.ORCHESTRA_CONTROLLER,
                "args": self.ORCHESTRA_CONTROLLER_ARGS.copy(),
            },
            "worker": {
                "args": self.ORCHESTRA_WORKER_ARGS.copy(),
                "interval": self.ORCHESTRA_WORKER_INTERVAL,
            },
            "abortTimeout": self.ORCHESTRA_ABORT_TIMEOUT,
        }


class FSConfig(BaseConfig):
    """
    Configuration class extension for DCM web-services that access the
    file-storage.
    """

    # Path to the working directory (typically mount point of the
    # shared file system)
    FS_MOUNT_POINT = Path(os.environ.get("FS_MOUNT_POINT") or "/file_storage")

    def set_identity(self):
        super().set_identity()

        (
            self.CONTAINER_SELF_DESCRIPTION["configuration"]["settings"][
                "fs_mount_point"
            ]
        ) = str(self.FS_MOUNT_POINT)


class DBConfig(BaseConfig):
    """
    Configuration class extension for DCM web-services that access the
    database.
    """

    DB_ADAPTER = os.environ.get("DB_ADAPTER", "sqlite")
    DB_ADAPTER_POOL_SIZE = int(os.environ.get("DB_ADAPTER_POOL_SIZE", 1))
    DB_ADAPTER_POOL_OVERFLOW = (
        int(os.environ.get("DB_ADAPTER_POOL_OVERFLOW", "0")) == "1"
    )
    DB_ADAPTER_CONNECTION_TIMEOUT = (
        None
        if "DB_ADAPTER_CONNECTION_TIMEOUT" not in os.environ
        else float(os.environ["DB_ADAPTER_CONNECTION_TIMEOUT"])
    )
    SQLITE_DB_FILE = (
        None
        if "SQLITE_DB_FILE" not in os.environ
        else Path(os.environ["SQLITE_DB_FILE"])
    )
    POSTGRES_DB_NAME = os.environ.get("POSTGRES_DB_NAME", "dcm-database")
    POSTGRES_DB_HOST = os.environ.get("POSTGRES_DB_HOST")
    POSTGRES_DB_PORT = os.environ.get("POSTGRES_DB_PORT")
    POSTGRES_DB_USER = os.environ.get("POSTGRES_DB_USER")
    POSTGRES_DB_PASSWORD = os.environ.get("POSTGRES_DB_PASSWORD")
    POSTGRES_DB_PASSFILE = os.environ.get("POSTGRES_DB_PASSFILE")

    DB_ADAPTER_STARTUP_IMMEDIATELY = False
    DB_ADAPTER_STARTUP_INTERVAL = 1.0

    def __init__(self) -> None:
        self.init_adapter()
        super().__init__()

    def init_adapter(self) -> None:
        """
        Initializes database-adapter `self.db` based on current
        attributes.
        """
        match self.DB_ADAPTER:
            case "sqlite":
                self.db = SQLiteAdapter3(
                    db_file=self.SQLITE_DB_FILE,
                    pool_size=self.DB_ADAPTER_POOL_SIZE,
                    allow_overflow=self.DB_ADAPTER_POOL_OVERFLOW,
                    connection_timeout=self.DB_ADAPTER_CONNECTION_TIMEOUT,
                    connect_now=self.DB_ADAPTER_STARTUP_IMMEDIATELY,
                )
            case "postgres":
                self.db = PostgreSQLAdapter14(
                    host=self.POSTGRES_DB_HOST,
                    port=self.POSTGRES_DB_PORT,
                    database=self.POSTGRES_DB_NAME,
                    user=self.POSTGRES_DB_USER,
                    password=self.POSTGRES_DB_PASSWORD,
                    passfile=self.POSTGRES_DB_PASSFILE,
                    pool_size=self.DB_ADAPTER_POOL_SIZE,
                    allow_overflow=self.DB_ADAPTER_POOL_OVERFLOW,
                    connection_timeout=self.DB_ADAPTER_CONNECTION_TIMEOUT,
                    connect_now=self.DB_ADAPTER_STARTUP_IMMEDIATELY,
                )
            case _:
                raise ValueError(f"Unknown db-identifier '{self.DB_ADAPTER}'")

    def set_identity(self) -> None:
        super().set_identity()

        self.CONTAINER_SELF_DESCRIPTION["configuration"]["settings"][
            "database"
        ] = {
            "adapter": self.DB_ADAPTER,
            "settings": {
                "poolSize": self.DB_ADAPTER_POOL_SIZE,
                "poolOverflow": self.DB_ADAPTER_POOL_OVERFLOW,
                "connectionTimeout": self.DB_ADAPTER_CONNECTION_TIMEOUT,
            }
            | (
                {
                    "file": (
                        None
                        if self.SQLITE_DB_FILE is None
                        else str(self.SQLITE_DB_FILE)
                    )
                }
                if self.DB_ADAPTER == "sqlite"
                else {}
            )
            | (
                {
                    "host": self.POSTGRES_DB_HOST,
                    "name": self.POSTGRES_DB_NAME,
                    "user": self.POSTGRES_DB_USER,
                    "port": self.POSTGRES_DB_PORT,
                    "password": self.POSTGRES_DB_PASSWORD is not None,
                    "passfile": self.POSTGRES_DB_PASSFILE,
                }
                if self.DB_ADAPTER == "postgres"
                else {}
            ),
        }
