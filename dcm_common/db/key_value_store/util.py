"""Utility definitions for the key-value-store subpackage."""

from pathlib import Path

from .adapter.interface import KeyValueStoreAdapter
from .adapter.native import NativeKeyValueStoreAdapter
from .adapter.http import HTTPKeyValueStoreAdapter
from .backend.interface import KeyValueStore
from .backend.memory import MemoryStore
from .backend.disk import JSONFileStore


ADAPTER_OPTIONS = {
    "native": NativeKeyValueStoreAdapter,
    "http": HTTPKeyValueStoreAdapter,
}
BACKEND_OPTIONS = {
    "memory": MemoryStore,
    "disk": JSONFileStore,
}


def load_adapter(
    name: str, adapter: str, settings: dict
) -> KeyValueStoreAdapter:
    """
    If valid, returns initilized instance of the requested
    `KeyValueStoreAdapter`.
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
    return JSONFileStore(Path(settings["dir"]))
