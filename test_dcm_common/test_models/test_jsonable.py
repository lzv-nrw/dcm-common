"""JSONable-tests."""

from collections.abc import MutableMapping

import pytest

from dcm_common.models.jsonable import (
    is_jsonable,
    is_jsonobject,
    is_jsonable_spec,
    is_jsonobject_spec,
)


@pytest.mark.parametrize(
    ("json", "expectation"),
    [
        (None, True),
        ("a", True),
        (0, True),
        (0.1, True),
        (True, True),
        ([True, "a"], True),
        ({"a": None, "b": ["a", 0.1]}, True),
        (lambda: None, False),
        ([lambda: None], False),
        ({"a": lambda: None}, False),
        ({1: "a"}, False),
    ],
)
def test_is_jsonable(json, expectation):
    """Test function `is_jsonable`."""
    assert is_jsonable(json) is expectation


@pytest.mark.parametrize(
    ("json", "expectation"),
    [
        (None, False),
        ("a", False),
        (0, False),
        (0.1, False),
        (True, False),
        ([True, "a"], False),
        ({"a": None, "b": ["a", 0.1]}, True),
        (lambda: None, False),
        ([lambda: None], False),
        ({"a": lambda: None}, False),
        ({1: "a"}, False),
    ],
)
def test_is_jsonobject(json, expectation):
    """Test function `is_jsonobject`."""
    assert is_jsonobject(json) is expectation


@pytest.mark.parametrize(
    ("spec", "expectation"),
    [
        (type(None), False),
        (str, False),
        (int, False),
        (float, False),
        (bool, False),
        (list["JSONable"], False),
        (MutableMapping[str, "JSONable"], False),
        (type(None) | str | int | float | bool, False),
        (
            str
            | int
            | float
            | bool
            | list["JSONable"]
            | MutableMapping[str, "JSONable"],
            False,
        ),
        (
            type(None)
            | int
            | float
            | bool
            | list["JSONable"]
            | MutableMapping[str, "JSONable"],
            False,
        ),
        (
            type(None)
            | str
            | float
            | bool
            | list["JSONable"]
            | MutableMapping[str, "JSONable"],
            False,
        ),
        (
            type(None)
            | str
            | int
            | bool
            | list["JSONable"]
            | MutableMapping[str, "JSONable"],
            False,
        ),
        (
            type(None)
            | str
            | int
            | float
            | list["JSONable"]
            | MutableMapping[str, "JSONable"],
            False,
        ),
        (
            type(None)
            | str
            | int
            | float
            | bool
            | MutableMapping[str, "JSONable"],
            False,
        ),
        (type(None) | str | int | float | bool | list["JSONable"], False),
        (
            type(None)
            | str
            | int
            | float
            | bool
            | list["JSONable"]
            | MutableMapping[str, "JSONable"],
            True,
        ),
        (
            str
            | int
            | float
            | bool
            | list["JSONable"]
            | MutableMapping[str, "JSONable"]
            | type(None),
            True,
        ),
        (
            type(None)
            | str
            | int
            | float
            | bool
            | list[
                type(None)
                | str
                | int
                | float
                | bool
                | list["JSONable"]
                | MutableMapping[str, "JSONable"]
            ]
            | MutableMapping[str, "JSONable"],
            True,
        ),
        (
            type(None)
            | str
            | int
            | float
            | bool
            | list["JSONable"]
            | MutableMapping[
                str,
                type(None)
                | str
                | int
                | float
                | bool
                | list["JSONable"]
                | MutableMapping[str, "JSONable"],
            ],
            True,
        ),
        ("JSONObject", False),
        ("JSONable", True),
    ],
)
def test_is_jsonable_spec(spec, expectation):
    """Test function `is_jsonable_spec`."""
    assert is_jsonable_spec(spec) is expectation


@pytest.mark.parametrize(
    ("spec", "expectation"),
    [
        (type(None), False),
        (str, False),
        (int, False),
        (float, False),
        (bool, False),
        (list["JSONable"], False),
        (MutableMapping[str, "JSONable"], True),
        (
            MutableMapping[
                str,
                type(None)
                | str
                | int
                | float
                | bool
                | list["JSONable"]
                | MutableMapping[str, "JSONable"],
            ],
            True,
        ),
        ("JSONObject", True),
        ("JSONable", False),
    ],
)
def test_is_jsonobject_spec(spec, expectation):
    """Test function `is_jsonobject_spec`."""
    assert is_jsonobject_spec(spec) is expectation
