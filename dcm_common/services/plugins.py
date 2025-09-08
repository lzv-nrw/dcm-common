"""Plugin-subpackage-related definitions for use service-context."""

from typing import Optional, Mapping, Any
from dataclasses import dataclass

from data_plumber_http.settings import Responses
from data_plumber_http import DPType

from dcm_common.util import qjoin
from dcm_common.models import DataModel
from dcm_common.plugins import PluginInterface


@dataclass
class PluginConfig(DataModel):
    """
    Configuration for generic plugin.

    Keyword arguments
    plugin -- plugin identifier
    args -- plugin arguments
    """

    plugin: str
    args: dict[str, Any]


class PluginType(DPType):
    """
    Generic plugin-type for use in data-plumbe-http-handlers.

    Keyword arguments:
    acceptable_plugins -- mapping of plugin-identifier to plugin-
                          instances that should be accepted
    acceptable_context -- plugin-contexts that should be accepted;
                          `None` corresponds to any context
    """

    TYPE = dict

    def __init__(
        self,
        acceptable_plugins: Mapping[str, PluginInterface],
        acceptable_context: Optional[list[str]] = None,
    ):
        self.acceptable_plugins = acceptable_plugins
        self.acceptable_context = acceptable_context

    def make(self, json, loc: str):
        for key in json:
            if key in ["plugin", "args"]:
                continue
            return (
                None,
                Responses().UNKNOWN_PROPERTY.msg.format(
                    origin=key,
                    loc=loc,
                    accepted=f"allowed fields: {qjoin(['plugin', 'args'])}",
                ),
                Responses().UNKNOWN_PROPERTY.status,
            )
        if "plugin" not in json:
            return (
                None,
                Responses().MISSING_REQUIRED.msg.format(
                    origin="plugin", loc=loc
                ),
                Responses().MISSING_REQUIRED.status,
            )
        if json["plugin"] not in self.acceptable_plugins:
            return (
                None,
                Responses().BAD_VALUE.msg.format(
                    origin=json["plugin"],
                    loc=loc,
                    expected=f"one of {qjoin(self.acceptable_plugins.keys())}",
                ),
                Responses().BAD_VALUE.status,
            )
        if (
            self.acceptable_context is not None
            and self.acceptable_plugins[json["plugin"]].context
            not in self.acceptable_context
        ):
            return (
                None,
                f"Requested plugin '{json['plugin']}' in '{loc}'.plugin has "
                + f"an invalid context (expected one of {qjoin(self.acceptable_context)}).",
                422,
            )
        if "args" not in json:
            return (
                None,
                Responses().MISSING_REQUIRED.msg.format(
                    origin="args", loc=loc
                ),
                Responses().MISSING_REQUIRED.status,
            )
        if not isinstance(json["args"], dict):
            return (
                None,
                Responses().BAD_TYPE.msg.format(
                    origin="args",
                    loc=loc,
                    xp_type="mapping",
                    fnd_type=type(json["args"]).__name__,
                ),
                Responses().BAD_TYPE.status,
            )
        valid, msg = self.acceptable_plugins[json["plugin"]].validate(
            self.acceptable_plugins[json["plugin"]].hydrate(json["args"])
        )
        if not valid:
            return (
                None,
                f"Invalid plugin-arguments given in '{loc}.args': {msg}",
                422,
            )
        return (
            PluginConfig(**json),
            Responses().GOOD.msg,
            Responses().GOOD.status,
        )
