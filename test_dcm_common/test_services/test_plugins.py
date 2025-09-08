"""Test suite for plugin-related utilities."""

from dataclasses import dataclass
from importlib.metadata import version

import pytest

if version("data_plumber_http").startswith("1."):
    TEST_PLUGIN_TYPE = True
else:  # TODO remove legacy-support
    TEST_PLUGIN_TYPE = False

from dcm_common.models.data_model import get_model_serialization_test
from dcm_common.services.plugins import PluginConfig


test_plugin_config_json = get_model_serialization_test(
    PluginConfig,
    (
        ((), {"plugin": "plugin-1", "args": {}}),
        ((), {"plugin": "plugin-1", "args": {"arg-1": "value-1"}}),
    ),
)


if TEST_PLUGIN_TYPE:
    # TODO remove legacy-support (pytest.mark.skip does not work as
    # decorators are evaluated and imports are missing)

    from data_plumber_http import Object, Property
    from data_plumber_http.settings import Responses

    from dcm_common.services.handlers import PluginType

    @pytest.mark.parametrize(
        ("json", "status"),
        (
            pytest_args := [
                ({"p": {}}, Responses().MISSING_REQUIRED.status),
                (
                    {"p": {"args": {"some_arg": 0}}},
                    Responses().MISSING_REQUIRED.status,
                ),
                (
                    {"p": {"plugin": "plugin-1"}},
                    Responses().MISSING_REQUIRED.status,
                ),
                (
                    {"p": {"plugin": "plugin-1", "args": None}},
                    Responses().BAD_TYPE.status,
                ),
                (
                    {"p": {"plugin": "plugin-1", "args": {}}},
                    Responses().BAD_VALUE.status,
                ),
                (
                    {
                        "p": {
                            "plugin": "plugin-1",
                            "args": {"some_arg": 0, "another_arg": 0},
                        }
                    },
                    Responses().BAD_VALUE.status,
                ),
                (
                    {"p": {"plugin": "plugin-3", "args": {"some_arg": 0}}},
                    Responses().BAD_VALUE.status,
                ),
                (
                    {"p": {"plugin": "plugin-2", "args": {"some_arg": 0}}},
                    Responses().BAD_VALUE.status,
                ),
                (
                    {"p": {"plugin": "plugin-1", "args": {"some_arg": 0}}},
                    Responses().GOOD.status,
                ),
            ]
        ),
        ids=[f"stage {i+1}" for i in range(len(pytest_args))],
    )
    def test_plugin_type(json, status):
        "Test `PluginType` in dp-handler."

        @dataclass
        class FakePlugin:
            """Fake plugin"""

            context: str

            def validate(self, kwargs):
                """Fake validation info"""
                if "some_arg" not in kwargs:
                    return False, "missing arg"
                if list(kwargs.keys()) != ["some_arg"]:
                    return False, "unknown arg"
                return True, "ok"

            def hydrate(self, kwargs):
                """Fake hydration."""
                return kwargs

        handler = Object(
            properties={
                Property("p"): PluginType(
                    acceptable_plugins={
                        "plugin-1": FakePlugin("context-1"),
                        "plugin-2": FakePlugin("context-2"),
                    },
                    acceptable_context=["context-1"],
                )
            }
        ).assemble()
        output = handler.run(json=json)

        assert output.last_status == status
        if output.last_status != Responses().GOOD.status:
            print(output.last_message)

    @pytest.mark.parametrize(
        ("json", "status"),
        (
            pytest_args := [
                (
                    {"p": {"plugin": "plugin-1", "args": {}}},
                    Responses().GOOD.status,
                ),
            ]
        ),
        ids=[f"stage {i+1}" for i in range(len(pytest_args))],
    )
    def test_plugin_type_any_context(json, status):
        "Test `PluginType` in dp-handler for any context."

        @dataclass
        class FakePlugin:
            """Fake plugin"""

            context: str

            def validate(self, kwargs):
                """Fake validation info"""
                return True, "ok"

            def hydrate(self, kwargs):
                """Fake hydration."""
                return kwargs

        handler = Object(
            properties={
                Property("p"): PluginType(
                    acceptable_plugins={
                        "plugin-1": FakePlugin("context-1"),
                    },
                    acceptable_context=None,
                )
            }
        ).assemble()
        assert handler.run(json=json).last_status == status

    @pytest.mark.parametrize(
        ("json", "status"),
        (
            pytest_args := [
                (
                    {"p": {"plugin": "plugin-1", "args": {}}},
                    Responses().BAD_VALUE.status,
                ),
            ]
        ),
        ids=[f"stage {i+1}" for i in range(len(pytest_args))],
    )
    def test_plugin_type_no_context(json, status):
        "Test `PluginType` in dp-handler for no context."

        @dataclass
        class FakePlugin:
            """Fake plugin"""

            context: str

            def validate(self, kwargs):
                """Fake validation info"""
                return True, "ok"

            def hydrate(self, kwargs):
                """Fake hydration."""
                return kwargs

        handler = Object(
            properties={
                Property("p"): PluginType(
                    acceptable_plugins={
                        "plugin-1": FakePlugin("context-1"),
                    },
                    acceptable_context=[],
                )
            }
        ).assemble()
        assert handler.run(json=json).last_status == status
