"""Common input handlers."""

from typing import Any
from pathlib import Path
from importlib.metadata import version

from data_plumber_http import Property, Object, String, FileSystemObject
if version("data_plumber_http").startswith("1."):
    from data_plumber_http.settings import Responses
else:  # TODO remove legacy-support
    from data_plumber_http import Responses as _R

    def Responses():  # pylint: disable=invalid-name
        "Mimic access to Responses as in v1."
        return _R


from .plugins import PluginType  # pylint: disable=unused-import, wrong-import-position


no_args_handler = Object(
    accept_only=[]
).assemble()


report_handler = Object(
    properties={
        Property("token", required=True): String()
    },
    accept_only=["token"]
).assemble()


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
            Responses().GOOD.status
        )


abort_query_handler = Object(
    model=lambda token, broadcast, re_queue: {
        "token": token,
        "broadcast": broadcast == "true",
        "re_queue": re_queue == "true"
    },
    properties={
        Property("token", required=True): String(),
        Property("broadcast", default=lambda **kwargs: "true"):
            String(enum=["true", "false"]),
        Property("re-queue", "re_queue", default=lambda **kwargs: "false"):
            String(enum=["true", "false"]),
    },
    accept_only=["token", "broadcast", "re-queue"]
).assemble()


abort_body_handler = Object(
    properties={
        Property("reason"): String(),
        Property("origin"): String()
    },
    accept_only=["reason", "origin"]
).assemble()
