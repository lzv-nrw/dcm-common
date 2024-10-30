"""
This module contains a definition for a key-value store-type database
adapter that operates purely in native python.
"""

from uuid import uuid4
from threading import RLock

from dcm_common.db.key_value_store.backend.interface import KeyValueStore
from .interface import KeyValueStoreAdapter


class NativeKeyValueStoreAdapter(KeyValueStoreAdapter):
    """
    Implementation of a `KeyValueStoreAdapter` working in native python.
    It is designed to handle concurrent requests.

    Keyword arguments:
    db -- instance of a KeyValueStore-implementation
    """

    def __init__(self, db: KeyValueStore) -> None:
        self._db = db
        self._lock = RLock()

    def read(self, key, pop=False):
        with self._lock:
            value = self._db.read(key)
            if pop:
                self._db.delete(key)
        return value

    def next(self, pop=False):
        with self._lock:
            keys = self._db.keys()
            if not keys:
                return None
            value = self._db.read(keys[0])
            if pop:
                self._db.delete(keys[0])
        return keys[0], value

    def write(self, key, value):
        with self._lock:
            self._db.write(key, value)

    def push(self, value):
        with self._lock:
            while (key := str(uuid4())) in self._db.keys():
                pass
            self._db.write(key, value)
        return key

    def delete(self, key):
        with self._lock:
            self._db.delete(key)

    def keys(self):
        with self._lock:
            return self._db.keys()


class NonThreadedNativeKeyValueStoreAdapter(KeyValueStoreAdapter):
    """
    Implementation of a `KeyValueStoreAdapter` working in native python.
    It is not designed for handling concurrent requests.

    Keyword arguments:
    db -- instance of a KeyValueStore-implementation
    """

    def __init__(self, db: KeyValueStore) -> None:
        self._db = db

    def read(self, key, pop=False):
        value = self._db.read(key)
        if pop:
            self._db.delete(key)
        return value

    def next(self, pop=False):
        keys = self._db.keys()
        if not keys:
            return None
        value = self._db.read(keys[0])
        if pop:
            self._db.delete(keys[0])
        return keys[0], value

    def write(self, key, value):
        self._db.write(key, value)

    def push(self, value):
        while (key := str(uuid4())) in self._db.keys():
            pass
        self._db.write(key, value)
        return key

    def delete(self, key):
        self._db.delete(key)

    def keys(self):
        return self._db.keys()
