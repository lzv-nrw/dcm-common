"""Common input handlers."""

from typing import Any
from pathlib import Path
from functools import partial

from data_plumber_http import Property, Object, String, FileSystemObject
from data_plumber_http.settings import Responses

# pylint: disable=unused-import, wrong-import-position
from .plugins import PluginType


no_args_handler = Object(accept_only=[]).assemble()


report_handler = Object(
    properties={Property("token", required=True): String()},
    accept_only=["token"],
).assemble()


UUID = partial(
    String,
    enum=None,
    pattern=r"[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}",
)
"""Regular string with uuid-format."""


class TargetPath(FileSystemObject):
    """
    Generalized from `data-plumber-http`'s `FileSystemObject`.

    The constructor passes all `args` and `kwargs` aside from
    `_relative_to` to the constructor of the superclass. The new kwarg
    `_relative_to` is used to transform the generated path after the
    validation.
    """

    def __init__(self, _relative_to: Path, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.__relative_to = _relative_to

    def make(self, json, loc: str) -> tuple[Any, str, int]:
        response = super().make(json, loc)
        if response[2] != Responses().GOOD.status:
            return response
        return (
            response[0].relative_to(self.__relative_to),
            Responses().GOOD.msg,
            Responses().GOOD.status,
        )


abort_query_handler = Object(
    model=lambda token: {"token": token},
    properties={Property("token", required=True): String()},
    accept_only=["token"],
).assemble()


abort_body_handler = Object(
    properties={Property("reason"): String(), Property("origin"): String()},
    accept_only=["reason", "origin"],
).assemble()
