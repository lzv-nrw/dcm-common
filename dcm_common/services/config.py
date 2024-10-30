"""Base configuration class for DCM web-services."""

import os
import sys
from pathlib import Path
import importlib.metadata
import json

from dcm_common.db import (
    KeyValueStore, MemoryStore, JSONFileStore,
    KeyValueStoreAdapter, NativeKeyValueStoreAdapter, HTTPKeyValueStoreAdapter,
    PostgreSQLAdapter14,
)
from dcm_common.services.notification import NotificationAPIClient

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
                }
            },
            "configuration": {
                "settings": {
                    "allow_cors": self.ALLOW_CORS,
                },
                "plugins": {},
                "services": {},
            },
        }


_ORCHESTRATION_ADAPTER_OPTIONS = {
    "native": NativeKeyValueStoreAdapter,
    "http": HTTPKeyValueStoreAdapter,
    "postgres14": PostgreSQLAdapter14,
}
_ORCHESTRATION_BACKEND_OPTIONS = {
    "memory": MemoryStore,
    "disk": JSONFileStore,
}


class OrchestratedAppConfig(BaseConfig):
    """
    Configuration class extension for DCM web-services that use an
    orchestrator.
    """

    ORCHESTRATION_PROCESSES = int(
        os.environ.get("ORCHESTRATION_PROCESSES") or 1
    )
    ORCHESTRATION_AT_STARTUP = (
        (int(os.environ.get("ORCHESTRATION_AT_STARTUP") or 1)) == 1
    )
    ORCHESTRATION_TOKEN_EXPIRATION = (
        (int(os.environ.get("ORCHESTRATION_TOKEN_EXPIRATION") or 1)) == 1
    )
    ORCHESTRATION_TOKEN_DURATION = (
        int(os.environ.get("ORCHESTRATION_TOKEN_DURATION") or 3600)
    )
    ORCHESTRATION_DEBUG = (
        (int(os.environ.get("ORCHESTRATION_DEBUG") or 0)) == 1
    )
    ORCHESTRATION_CONTROLS_API = (
        (int(os.environ.get("ORCHESTRATION_CONTROLS_API") or 0)) == 1
    )
    ORCHESTRATION_QUEUE_ADAPTER = (
        os.environ.get("ORCHESTRATION_QUEUE_ADAPTER")
    )
    ORCHESTRATION_REGISTRY_ADAPTER = (
        os.environ.get("ORCHESTRATION_REGISTRY_ADAPTER")
    )
    # {"backend": "disk"|"memory", "dir": ..., "url": ..., "timeout": ...,
    # "proxies": ...}
    ORCHESTRATION_QUEUE_SETTINGS = (
        json.loads(os.environ["ORCHESTRATION_QUEUE_SETTINGS"])
        if "ORCHESTRATION_QUEUE_SETTINGS" in os.environ else None
    )
    ORCHESTRATION_REGISTRY_SETTINGS = (
        json.loads(os.environ["ORCHESTRATION_REGISTRY_SETTINGS"])
        if "ORCHESTRATION_REGISTRY_SETTINGS" in os.environ else None
    )
    ORCHESTRATION_DAEMON_INTERVAL = (
        float(os.environ["ORCHESTRATION_DAEMON_INTERVAL"])
        if "ORCHESTRATION_DAEMON_INTERVAL" in os.environ else None
    )
    ORCHESTRATION_ORCHESTRATOR_INTERVAL = (
        float(os.environ["ORCHESTRATION_ORCHESTRATOR_INTERVAL"])
        if "ORCHESTRATION_ORCHESTRATOR_INTERVAL" in os.environ else None
    )

    ORCHESTRATION_ABORT_NOTIFICATIONS = (
        (int(os.environ.get("ORCHESTRATION_ABORT_NOTIFICATIONS") or 0)) == 1
    )
    ORCHESTRATION_ABORT_NOTIFICATIONS_URL = (
        os.environ.get("ORCHESTRATION_ABORT_NOTIFICATIONS_URL")
    )
    ORCHESTRATION_ABORT_NOTIFICATIONS_CALLBACK = (
        os.environ.get("ORCHESTRATION_ABORT_NOTIFICATIONS_CALLBACK")
    )
    ORCHESTRATION_ABORT_TIMEOUT = (
        float(os.environ.get("ORCHESTRATION_ABORT_TIMEOUT") or 1.0)
    )
    ORCHESTRATION_ABORT_NOTIFICATIONS_STARTUP_INTERVAL = 1.0
    ORCHESTRATION_ABORT_NOTIFICATIONS_RECONNECT_INTERVAL = 60.0

    def __init__(self) -> None:
        # queue
        self._queue_settings = {
            "type": self.ORCHESTRATION_QUEUE_ADAPTER or "native",
            "settings": (
                self.ORCHESTRATION_QUEUE_SETTINGS
                or {"backend": "memory"}
            )
        }
        self.queue = self._load_adapter(
            "queue", self._queue_settings["type"],
            self._queue_settings["settings"]
        )
        # registry
        self._registry_settings = {
            "type": self.ORCHESTRATION_REGISTRY_ADAPTER or "native",
            "settings": (
                self.ORCHESTRATION_REGISTRY_SETTINGS
                or {"backend": "memory"}
            )
        }
        self.registry = self._load_adapter(
            "registry", self._registry_settings["type"],
            self._registry_settings["settings"]
        )

        # notifications
        if self.ORCHESTRATION_ABORT_NOTIFICATIONS:
            if self.ORCHESTRATION_ABORT_NOTIFICATIONS_URL is None:
                raise RuntimeError(
                    "Incomplete configuration: Abort notification-subscription"
                    + " requires 'ORCHESTRATION_ABORT_NOTIFICATIONS_URL'."
                )
            self.abort_notification_client = NotificationAPIClient(
                self.ORCHESTRATION_ABORT_NOTIFICATIONS_URL,
                "abort",
                callback_url=self.ORCHESTRATION_ABORT_NOTIFICATIONS_CALLBACK,
                timeout=self.ORCHESTRATION_ABORT_TIMEOUT
            )

        super().__init__()

    def _load_adapter(
        self, name: str, adapter: str, settings: dict
    ) -> KeyValueStoreAdapter:
        """
        If valid, returns initilized instance of the requested
        `KeyValueStoreAdapter`.
        """
        if (adapter not in _ORCHESTRATION_ADAPTER_OPTIONS):
            raise ValueError(
                f"Adapter '{adapter}' for orchestration-{name} "
                + "is not allowed. Possible values are: "
                + f"{', '.join(_ORCHESTRATION_ADAPTER_OPTIONS.keys())}."
            )
        if adapter == "native":
            return NativeKeyValueStoreAdapter(
                self._load_backend(
                    name, settings.get("backend", "memory"), settings
                )
            )
        if adapter == "http":
            kwargs = {
                k: settings.get(k) for k, v in settings.items()
                if k in ["url", "timeout", "proxies"]
            }
            return HTTPKeyValueStoreAdapter(**kwargs)
        kwargs = {
            k: settings.get(k) for k, v in settings.items()
            if k in [
                "key_name", "value_name", "table", "host", "port", "user",
                "password", "database", "pgpassfile", "additional_options"
            ]
        }
        return PostgreSQLAdapter14(**kwargs)

    def _load_backend(
        self, name: str, backend: str, settings: dict
    ) -> KeyValueStore:
        """
        If valid, returns initilized instance of the requested
        `KeyValueStore`.
        """
        if (backend not in _ORCHESTRATION_BACKEND_OPTIONS):
            raise ValueError(
                f"Backend '{backend}' for orchestration-{name} "
                + "is not allowed. Possible values are: "
                + f"{', '.join(_ORCHESTRATION_BACKEND_OPTIONS.keys())}."
            )
        if backend == "memory":
            return MemoryStore()
        return JSONFileStore(Path(settings["dir"]))

    def set_identity(self):
        super().set_identity()

        (self.CONTAINER_SELF_DESCRIPTION["configuration"]["settings"]
            ["orchestration"]) = {
                # FIXME: this should be added before in API-projects
                # "processes": self.ORCHESTRATION_PROCESSES,
                "at_startup": self.ORCHESTRATION_AT_STARTUP,
                "queue": self._queue_settings,
                "registry": self._registry_settings,
                "token": {
                    "expiration": self.ORCHESTRATION_TOKEN_EXPIRATION,
                    "duration": self.ORCHESTRATION_TOKEN_DURATION,
                },
                "debug": self.ORCHESTRATION_DEBUG,
                "controls_api": self.ORCHESTRATION_CONTROLS_API,
                "abort": {
                    "subscription": self.ORCHESTRATION_ABORT_NOTIFICATIONS,
                }
            }
        if self.ORCHESTRATION_ABORT_NOTIFICATIONS:
            (self.CONTAINER_SELF_DESCRIPTION["configuration"]["settings"]
                ["orchestration"]["abort"]["url"]) = (
                self.ORCHESTRATION_ABORT_NOTIFICATIONS_URL
            )
            (self.CONTAINER_SELF_DESCRIPTION["configuration"]["settings"]
                ["orchestration"]["abort"]["timeout"]) = (
                {"duration": self.ORCHESTRATION_ABORT_TIMEOUT}
            )
            if self.ORCHESTRATION_ABORT_NOTIFICATIONS_CALLBACK:
                (self.CONTAINER_SELF_DESCRIPTION["configuration"]["settings"]
                    ["orchestration"]["abort"]["callback"]) = (
                    self.ORCHESTRATION_ABORT_NOTIFICATIONS_CALLBACK
                )

        if self.ORCHESTRATION_DAEMON_INTERVAL is not None:
            (self.CONTAINER_SELF_DESCRIPTION["configuration"]["settings"]
                ["orchestration"]["daemon_interval"]) = (
                    self.ORCHESTRATION_DAEMON_INTERVAL
                )
        if self.ORCHESTRATION_ORCHESTRATOR_INTERVAL is not None:
            (self.CONTAINER_SELF_DESCRIPTION["configuration"]["settings"]
                ["orchestration"]["orchestrator_interval"]) = (
                    self.ORCHESTRATION_ORCHESTRATOR_INTERVAL
                )


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

        (self.CONTAINER_SELF_DESCRIPTION["configuration"]["settings"]
            ["fs_mount_point"]) = str(self.FS_MOUNT_POINT)
