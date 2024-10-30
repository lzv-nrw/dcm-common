"""
This module contains a definition for a key-value store-type database
adapter that operates based on a database that is reachable via HTTP.
"""

from typing import Optional, Mapping
from json import dumps

import requests

from .interface import KeyValueStoreAdapter


class HTTPKeyValueStoreAdapter(KeyValueStoreAdapter):
    """
    Implementation of a `KeyValueStoreAdapter` for interacting with a
    database over network via HTTP. The database is expected to
    implement the 'LZV.nrw - KeyValueStore-API' in version v0.

    Keyword arguments:
    url -- base url for database
    timeout -- timeout duration for database
               (default 1)
    proxies -- network proxy-configuration (see documentation of
               `requests` for details)
               (default None)
    """

    def __init__(
        self,
        url: str,
        timeout: float = 1.0,
        proxies: Optional[Mapping[str, str]] = None
    ) -> None:
        self._url = url
        self._timeout = timeout
        self._proxies = proxies
        self._default_kwargs = {
            "timeout": timeout, "proxies": proxies
        }

    def read(self, key, pop=False):
        response = requests.get(
            f"{self._url}/db/{key}{'?pop=' if pop else ''}",
            **self._default_kwargs
        )
        if response.status_code == 200:
            return response.json()
        return None

    def next(self, pop=False):
        response = requests.get(
            f"{self._url}/db{'?pop=' if pop else ''}", **self._default_kwargs
        )
        if response.status_code == 200:
            json = response.json()
            return json["key"], json["value"]
        return None

    def write(self, key, value):
        requests.post(
            f"{self._url}/db/{key}", data=dumps(value), **self._default_kwargs,
            headers={"Content-Type": "application/json"}
        )

    def push(self, value):
        return requests.post(
            f"{self._url}/db", data=dumps(value), **self._default_kwargs,
            headers={"Content-Type": "application/json"}
        ).text

    def delete(self, key):
        requests.delete(
            f"{self._url}/db/{key}", **self._default_kwargs
        )

    def keys(self):
        response = requests.options(
            f"{self._url}/db", **self._default_kwargs
        )
        return tuple(response.json())
