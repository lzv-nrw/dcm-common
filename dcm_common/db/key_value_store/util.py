"""Utility definitions for the key-value-store subpackage."""

from pathlib import Path

from .adapter.interface import KeyValueStoreAdapter
from .adapter.native import NativeKeyValueStoreAdapter
from .adapter.http import HTTPKeyValueStoreAdapter
from .backend.interface import KeyValueStore
from .backend.memory import MemoryStore
from .backend.disk import JSONFileStore
from .backend.sqlite import SQLiteStore


ADAPTER_OPTIONS = {
    "native": NativeKeyValueStoreAdapter,
    "http": HTTPKeyValueStoreAdapter,
}
BACKEND_OPTIONS = {
    "memory": MemoryStore,
    "disk": JSONFileStore,
    "sqlite": SQLiteStore,
}


def load_adapter(
    name: str, adapter: str, settings: dict
) -> KeyValueStoreAdapter:
    """
    If valid, returns initilized instance of the requested
    `KeyValueStoreAdapter`.

    If the `adapter` is "native", `settings` are used to initialize a
    backend (e.g. `{"backend": "sqlite", "path": "<path/to/file.db>"}`).
    Whereas, if the `adapter` is "http", the settings are passed to the
    `HTTPKeyValueStoreAdapter` directly.
    """
    if adapter not in ADAPTER_OPTIONS:
        raise ValueError(
            f"Adapter '{adapter}' for {name} is not allowed. Possible values "
            + f"are: {', '.join(ADAPTER_OPTIONS.keys())}."
        )
    if adapter == "native":
        return NativeKeyValueStoreAdapter(
            load_backend(name, settings.get("backend", "memory"), settings)
        )
    if adapter == "http":
        kwargs = {
            k: settings.get(k)
            for k, v in settings.items()
            if k in ["url", "timeout", "proxies"]
        }
        return HTTPKeyValueStoreAdapter(**kwargs)
    raise ValueError(f"Unknown adapter type '{adapter}'.")


def load_backend(name: str, backend: str, settings: dict) -> KeyValueStore:
    """
    If valid, returns initilized instance of the requested
    `KeyValueStore`.
    """
    if backend not in BACKEND_OPTIONS:
        raise ValueError(
            f"Backend '{backend}' for {name} is not allowed. Possible values "
            + f"are: {', '.join(BACKEND_OPTIONS.keys())}."
        )
    if backend == "memory":
        return MemoryStore()
    if backend == "disk":
        return JSONFileStore(
            Path(settings["dir"] if "dir" in settings else settings["dir_"])
        )
    return SQLiteStore(
        settings.get("path"),
        settings.get("memory_id"),
        timeout=settings["timeout"] if "timeout" in settings else 5,
    )
