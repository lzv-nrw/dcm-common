"""
Client definition for Notification API.
"""

from typing import Optional, Mapping
from dataclasses import dataclass

import requests


@dataclass
class _Registration:
    """Registration info."""

    base_url: str
    token: str


@dataclass
class _Subscription:
    """Subscription info."""

    token: str


class NotificationAPIClient:
    """
    Client for the Notification API. A client is specific for a given
    notification service (via `api_url`), a callback url (via
    `callback_url`), and `topic`.

    Keyword arguments:
    api_url -- api base url
    topic -- subscription topic
    callback_url -- base url used to register with
                    If omitted, the client attempts to fetch their IP
                    address via the notification-service and raises
                    `ValueError` if not successful. The address is then
                    built as 'http://{ip}'.
                    (default None; uses API default)
    timeout -- timeout for notify-requests to API
               (default 1.0)
    register -- whether to register at initialization
                (default False)
    """
    _GENERAL_API_REQUEST_TIMEOUT = 1.0

    def __init__(
        self,
        api_url: str,
        topic: str,
        callback_url: Optional[str] = None,
        timeout: float = 1.0,
        register: bool = False
    ) -> None:
        self.api_url = api_url
        self._topic = topic
        if callback_url is None:
            ip = self.get_ip(
                api_url, timeout=self._GENERAL_API_REQUEST_TIMEOUT
            )
            if ip is None:
                raise ValueError(
                    f"Unable to fetch remote address of self from '{api_url}'."
                )
            self.callback_url = f"http://{ip}"
        else:
            self.callback_url = callback_url
        if self.callback_url == "":
            self._base_url_json = {}
        else:
            self._base_url_json = {"skip": self.callback_url}
        self.timeout = timeout

        if register:
            self.register()
            self.subscribe()
        else:
            self._token = None

    @staticmethod
    def get_ip(api_url: str, timeout: float = 1.0) -> Optional[str]:
        """
        Attempts to query Notification API for the client's IP address.

        Keyword arguments:
        api_url -- api base url
        timeout -- timeout for requests to API
                   (default 1.0)
        """
        try:
            return requests.get(
                f"{api_url}/ip", timeout=timeout
            ).json().get("ip")
        except (
            requests.exceptions.RequestException,
            requests.exceptions.JSONDecodeError
        ):
            return None

    @staticmethod
    def list_topics(api_url: str, timeout: float = 1.0) -> list[str]:
        """
        List available topics for Notification API at `api_url`.

        Keyword arguments:
        api_url -- api base url
        timeout -- timeout for requests to API
                   (default 1.0)
        """
        return requests.options(
            f"{api_url}/", timeout=timeout
        ).json()

    @property
    def topic(self) -> str:
        """Returns topic identifier associated with this client."""
        return self._topic

    @property
    def token(self) -> Optional[str]:
        """Returns token value associated with this client."""
        return self._token

    def get_config(self):
        """
        Returns the notification service configuration as JSON.
        """
        return requests.get(
            f"{self.api_url}/config", timeout=self._GENERAL_API_REQUEST_TIMEOUT
        ).json()

    def register(self) -> None:
        """
        Register with notification service. Raises `RuntimeError` if not
        successful.
        """
        registration = requests.post(
            f"{self.api_url}/registration",
            timeout=self._GENERAL_API_REQUEST_TIMEOUT,
            json={"baseUrl": self.callback_url}
        )
        if registration.status_code != 200:
            raise RuntimeError(
                f"Unable to make registration at '{self.api_url}': "
                + registration.text
            )
        self._token = registration.json()["token"]

    def registered(self) -> bool:
        """
        Returns `True` if currently registered at notification service.
        """
        return self._token is not None and requests.get(
            f"{self.api_url}/registration?token={self._token}",
            timeout=self._GENERAL_API_REQUEST_TIMEOUT
        ).status_code == 200

    def deregister(self) -> None:
        """Revoke registration at notification service."""
        try:
            requests.delete(
                f"{self.api_url}/registration?token={self._token}",
                timeout=self._GENERAL_API_REQUEST_TIMEOUT
            )
        except requests.exceptions.RequestException:
            pass

    def list_registered(self) -> list[_Registration]:
        """List current registrations at notification service."""
        return [
            _Registration(record["baseUrl"], record["token"])
            for record in requests.options(
                f"{self.api_url}/registration",
                timeout=self._GENERAL_API_REQUEST_TIMEOUT
            ).json()
        ]

    def subscribe(self) -> None:
        """
        Make subscription at notification service. Raises `RuntimeError`
        if not successful.
        """
        if self._token is None:
            raise RuntimeError("Not yet registered.")
        subscription = requests.post(
            f"{self.api_url}/subscription"
            + f"?token={self._token}&topic={self._topic}",
            timeout=self._GENERAL_API_REQUEST_TIMEOUT,
            json={"baseUrl": self.callback_url}
        )
        if subscription.status_code != 200:
            raise RuntimeError(
                f"Unable to make subscription at '{self.api_url}' for "
                + f"'{self._topic}': {subscription.text}"
            )

    def subscribed(self) -> bool:
        """
        Returns `True` if currently subscribed at notification service.
        """
        if self._token is None:
            raise RuntimeError("Not yet registered.")
        return requests.get(
            f"{self.api_url}/subscription"
            + f"?token={self._token}&topic={self._topic}",
            timeout=self._GENERAL_API_REQUEST_TIMEOUT
        ).status_code == 200

    def unsubscribe(self) -> None:
        """Revoke subscription at notification service."""
        if self._token is None:
            raise RuntimeError("Not yet registered.")
        requests.delete(
            f"{self.api_url}/subscription"
            + f"?token={self._token}&topic={self._topic}",
            timeout=self._GENERAL_API_REQUEST_TIMEOUT
        )

    def list_subscribed(self) -> list[_Subscription]:
        """List current subscriptions at notification service."""
        return [
            _Subscription(record)
            for record in requests.options(
                f"{self.api_url}/subscription?topic={self._topic}",
                timeout=self._GENERAL_API_REQUEST_TIMEOUT
            ).json()
        ]

    def notify(
        self,
        query: Optional[Mapping] = None,
        json: Optional[Mapping] = None,
        headers: Optional[Mapping] = None,
        skip_self: bool = True
    ) -> None:
        """
        Submit message for broadcasting.

        Keyword arguments:
        query -- mapping of query-data to be posted
                 (default None)
        json -- mapping of json-data to be posted
                (default None)
        headers -- mapping of header-data to be posted
                   (default None)
        skip_self -- whether to skip broadcasting to self; this setting
                     is ignored if client is not yet registered
                     (default True)
        """
        requests.post(
            f"{self.api_url}/notify?topic={self._topic}",
            json={
                "query": query, "json": json, "headers": headers
            } | ({"skip": self._token} if skip_self and self._token else {}),
            timeout=self.timeout
        )

    def connect(self) -> None:
        """Handles registering and subscribing in single call."""
        if not self.registered():
            self.register()
        if not self.subscribed():
            self.subscribe()
