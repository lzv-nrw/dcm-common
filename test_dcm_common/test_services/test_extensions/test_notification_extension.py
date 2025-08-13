"""Test module for notification-extension."""

from io import StringIO
from dataclasses import dataclass

from requests.exceptions import ReadTimeout

from dcm_common.services.extensions.common import PrintStatusSettings
from dcm_common.services.extensions.notification import (
    setup_connection_test_callback_with_state,
)


class PrintStatusReader:
    def __init__(self):
        self.lines = []
        self._stderr = PrintStatusSettings.file
        self._stringio = StringIO()

    def __enter__(self):
        PrintStatusSettings.file = self._stringio
        return self

    def __exit__(self, *args):
        PrintStatusSettings.file = self._stderr
        self.lines = self._stringio.getvalue().splitlines()


def test_setup_connection_test_callback_with_state():
    """Test function `setup_connection_test_callback_with_state`."""

    # create callback
    callback = setup_connection_test_callback_with_state(False)

    # mock client
    @dataclass
    class Client:
        ok: bool

        def registered(self):
            return self.ok

        def subscribed(self):
            return self.ok

    assert callback(Client(True))
    assert not callback(Client(False))


def test_setup_connection_test_callback_with_state_w_error():
    """Test function `setup_connection_test_callback_with_state`."""

    # create callback
    callback = setup_connection_test_callback_with_state(True)

    # mock client
    @dataclass
    class Client:
        error: bool

        def registered(self):
            if self.error:
                raise ReadTimeout("error")
            return True

        def subscribed(self):
            return True

    with PrintStatusReader() as output:
        callback(Client(True))  # (message)
        callback(Client(True))  # (no-message)

    # message should only be printed once
    assert len(output.lines) == 1

    with PrintStatusReader() as output:
        callback(Client(False))  # reconnect (no-message)
        callback(Client(True))  # disconnect (message)
        callback(Client(True))  # no-change (no-message)
        callback(Client(False))  # reconnect (no-message)
        callback(Client(False))  # no-change (no-message)

    # should correspond to single message again
    assert len(output.lines) == 1
