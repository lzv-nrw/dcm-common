"""
Test module for `DemoPlugin`.
"""

import pytest

from dcm_common.logger import LoggingContext as Context
from dcm_common.plugins.demo import DemoPlugin


@pytest.mark.parametrize(
    ("kwargs", "response"),
    [
        ({}, False),
        ({"success": True}, True),
        ({"success_rate": 0}, True),
        ({"success_rate": -1}, False),
        ({"success": True, "success_rate": 0}, False),
    ],
    ids=["none", "success", "rate", "rate-bad", "both"],
)
def test_validate(kwargs, response):
    """Test signature validation of `DemoPlugin`."""
    ok = DemoPlugin.validate(kwargs)

    assert ok[0] is response
    if not ok[0]:
        print(ok[1])


def test_json():
    """Test property `json` of `DemoPlugin`."""
    for key in (
        "name",
        "description",
        "signature",
        "context",
    ):
        assert key in DemoPlugin().json

    assert DemoPlugin().json["signature"] == DemoPlugin.signature.json


def test_get_success():
    """Test method `get` of `DemoPlugin` with `success`-argument."""
    plugin = DemoPlugin()
    result = plugin.get(None, success=True)
    assert result.success


def test_get_nosuccess():
    """Test method `get` of `DemoPlugin` with `success`-argument."""
    plugin = DemoPlugin()
    result = plugin.get(None, success=False)
    assert not result.success
    assert Context.ERROR in result.log
    print(result.log[Context.ERROR][0].body)


def test_get_success_rate():
    """Test method `get` of `DemoPlugin` with `success`-argument."""
    plugin = DemoPlugin()
    results = set()
    count = 0
    while len(results) < 2 and count < 100:
        count += 1
        results.add(plugin.get(None, success_rate=50).success)
    assert results == {False, True}


def test_get_with_context():
    """Test method `get` of `DemoPlugin` with pre-generated context."""
    plugin = DemoPlugin()
    # without provided context
    assert Context.USER not in plugin.get(None, success=True).log

    # with provided context
    context = plugin.create_context()
    assert context.result.log.default_origin == plugin.display_name
    context.result.log.log(Context.USER, body="test")
    assert Context.USER in plugin.get(context, success=True).log
