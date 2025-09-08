"""
This module contains a definition for an on-disk key-value store-type
database.
"""

from typing import Any, Optional
from pathlib import Path
from hashlib import md5
from dataclasses import dataclass
import json

from dcm_common.util import list_directory_content
from .interface import KeyValueStore


@dataclass
class Record:
    """Record-class storing information related to a database record."""

    value: Optional[Any] = None
    file: Optional[Path] = None


class JSONFileStore(KeyValueStore):
    """
    A minimalistic implementation of a `KeyValueStore` working on disk.
    It supports all objects that can be de-/serialized using the `json`
    module. Note that there are no measures to prevent concurrency-
    issues.

    Notable properties:
    * Writing an already existing key simply overwrites data.
    * Reading a non-existing key returns `None` instead.
    * Deleting a non-existing key does not raise an exception.

    Keyword arguments:
    dir -- path to working directory in filesystem
    """

    _SUPPORTED_TYPES = (str, int, float, dict, list, bool, None.__class__)

    def __init__(self, dir_: Path) -> None:
        self._dir = dir_.resolve()
        self._dir.mkdir(parents=True, exist_ok=True)
        self._database: dict[str, Record] = {}

    def _serialize(self, key: str, value: Optional[str] = None) -> str:
        """Returns serialized data."""
        if value is None:
            return json.dumps({"key": key, "value": self._database[key].value})
        return json.dumps({"key": key, "value": value})

    def _deserialize(self, data: str) -> tuple[str, str]:
        """Returns deserialized data as a tuple of `key` and `value`."""
        _json = json.loads(data)
        return _json["key"], _json["value"]

    def _cache_record(
        self,
        target: str | Path,
    ) -> None:
        """
        Caches database-record based on persistent storage.

        Keyword arguments:
        target -- either db-key (as string) or db-file (as `Path`)
        """
        # process input
        if isinstance(target, str):
            file = self._dir / self._get_key_hash(target)
        else:
            file = target
        # read and add to cache
        try:
            # check if record exists in persistent storage
            if not file.is_file():
                return
            key, value = self._deserialize(file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, PermissionError):
            return
        if isinstance(target, str) and target != key:  # skip bad record
            return
        self._database[key] = Record(value, file)

    @staticmethod
    def _get_key_hash(key: str):
        """Returns a hash-representation of `key`."""
        return md5(key.encode(encoding="utf-8")).hexdigest()

    def _encode(self, value):
        return json.dumps(value)

    def _decode(self, value):
        if value is None:
            return None
        return json.loads(value)

    def _read(self, key):
        if key not in self._database:
            # cache from disk
            self._cache_record(key)
        # return cache
        return self._database.get(key, Record()).value

    def _write(self, key, value):
        # store in cache
        self._database[key] = Record(
            value, self._dir / self._get_key_hash(key)
        )
        # persist to disk
        self._database[key].file.write_text(self._serialize(key, value))

    def _delete(self, key):
        # with cached record
        if key in self._database:
            self._database[key].file.unlink(missing_ok=True)
            del self._database[key]
            return
        # without cached record
        file = self._dir / self._get_key_hash(key)
        if file.is_file():
            file.unlink()

    def keys(self):
        # iterate storage to cache all records
        for file in list_directory_content(
            self._dir, condition_function=lambda p: p.is_file()
        ):
            if any(
                record.file.name == file.name
                for record in self._database.values()
            ):
                # skip previously cached data
                continue
            self._cache_record(file)
        return tuple(self._database.keys())

    @property
    def dir(self) -> Path:
        """Returns working directory as `Path`."""
        return self._dir
