"""Input handlers for the 'DCM Demo'-app."""

from typing import Mapping
from data_plumber_http import (
    Property,
    Object,
    Array,
    Number,
    Boolean,
    String,
    Url,
)

from dcm_common.plugins import PluginInterface
from dcm_common.services.plugins import PluginType
from dcm_common.services.handlers import UUID
from .models import DemoConfig


def get_demo_handler(demo_plugins: Mapping[str, PluginInterface]):
    """Returns handler for POST-/demo endpoint."""
    return Object(
        properties={
            Property("demo"): Object(
                model=lambda **kwargs: DemoConfig.from_json(kwargs),
                properties={
                    Property("duration", default=1.0): Number(),
                    Property("success", default=True): Boolean(),
                    Property(
                        "successPlugin", name="success_plugin"
                    ): PluginType(
                        acceptable_plugins=demo_plugins,
                        acceptable_context=["testing"],
                    ),
                    Property("children"): Array(
                        items=Object(
                            properties={
                                Property("host"): String(),
                                Property("timeout", default=10.0): Number(),
                                Property("body"): Object(free_form=True),
                            },
                            accept_only=["host", "timeout", "body"],
                        )
                    ),
                },
                accept_only=[
                    "duration",
                    "success",
                    "successPlugin",
                    "children",
                ],
            ),
            Property("token"): UUID(),
            Property("callbackUrl", name="callback_url"): Url(
                schemes=["http", "https"]
            ),
        },
        accept_only=["demo", "token", "callbackUrl"],
    ).assemble()
