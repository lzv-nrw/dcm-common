"""
This module contains a definition for an in-memory key-value store-type
database.
"""

from .interface import KeyValueStore


class MemoryStore(KeyValueStore):
    """
    A minimalistic implementation of a `KeyValueStore` working in
    memory (non-persistent).

    Notable properties:
    * Writing an already existing key simply overwrites data.
    * Reading a non-existing key returns `None` instead.
    * Deleting a non-existing key does not raise an exception.
    """

    def __init__(self) -> None:
        self._database = {}

    def _read(self, key):
        return self._database.get(key)

    def _write(self, key, value):
        self._database[key] = value

    def _delete(self, key):
        if key in self._database:
            del self._database[key]

    def keys(self):
        return tuple(self._database.keys())
