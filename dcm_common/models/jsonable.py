"""
Definition of the JSONable-type and associated utility functions
"""

from typing import (
    Optional, TypeAlias, ForwardRef, get_args, get_origin
)
from collections.abc import MutableMapping


JSONable: TypeAlias = Optional[
    str | int | float | bool | list["JSONable"]
    | MutableMapping[str, "JSONable"]
]
JSONObject: TypeAlias = MutableMapping[str, JSONable]


def is_jsonable_spec(type_):
    """Returns `True` if `type_` conforms to the `JSONable`-spec."""
    if (
        type_ == "JSONable"
        or (
            isinstance(type_, ForwardRef)
            and type_.__forward_arg__ == "JSONable"
        )
    ):
        return True
    args = get_args(type_)
    if (
        set(get_origin(arg) or arg for arg in args)
        != {str, int, float, bool, list, MutableMapping, type(None)}
    ):
        return False
    for arg in args:
        if get_origin(arg) == list and not is_jsonable_spec(get_args(arg)[0]):
            return False
        if (
            get_origin(arg) == MutableMapping
            and (
                get_args(arg)[0] != str
                or not is_jsonable_spec(get_args(arg)[1])
            )
        ):
            return False
    return True


def is_jsonobject_spec(type_):
    """Returns `True` if `type_` conforms to the `JSONObject`-spec."""
    if (
        type_ == "JSONObject"
        or (
            isinstance(type_, ForwardRef)
            and type_.__forward_arg__ == "JSONObject"
        )
    ):
        return True
    if (
        get_origin(type_) != MutableMapping
        or get_args(type_)[0] != str
        or not is_jsonable_spec(get_args(type_)[1])
    ):
        return False
    return True


def is_jsonable(value):
    """Returns `True` if `value` conforms to the `JSONable`-spec."""
    if value is None or isinstance(value, (str,  int, float, bool)):
        return True
    if isinstance(value, list):
        return all(is_jsonable(element) for element in value)
    if isinstance(value, MutableMapping):
        return (
            all(isinstance(key, str) for key in value.keys())
            and all(is_jsonable(element) for element in value.values())
        )
    return False


def is_jsonobject(value):
    """Returns `True` if `value` conforms to the `JSONObject`-spec."""
    if not isinstance(value, MutableMapping):
        return False
    return is_jsonable(value)
