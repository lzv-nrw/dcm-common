"""Input handlers for the 'DCM Demo'-app."""

from data_plumber_http import Property, Object, Array, Number, Boolean, String, Url

from .models import DemoConfig

demo_handler = Object(
    properties={
        Property("demo"): Object(
            model=lambda **kwargs: DemoConfig.from_json(kwargs),
            properties={
                Property("duration", default=1.0): Number(),
                Property("success", default=True): Boolean(),
                Property("children"): Array(
                    items=Object(
                        properties={
                            Property("host"): String(),
                            Property("timeout", default=10.0): Number(),
                            Property("body"): Object(free_form=True)
                        },
                        accept_only=["host", "timeout", "body"]
                    )
                ),
            },
            accept_only=["duration", "success", "children"]
        ),
        Property("callbackUrl", name="callback_url"):
            Url(schemes=["http", "https"])
    },
    accept_only=["demo", "callbackUrl"]
).assemble()
