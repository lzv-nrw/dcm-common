"""
Test module for the `PluginInterface`-interface.
"""

from dataclasses import dataclass
from importlib.metadata import version

import pytest
from dcm_common.models import DataModel
from dcm_common import Logger

from dcm_common.plugins import (
    Dependency,
    PythonDependency,
    JSONType,
    Signature,
    Argument,
    PluginResult,
    PluginInterface,
    PluginExecutionContext,
)


@pytest.fixture(scope="module", name="test_plugin_class")
def _test_plugin_class():
    """Fixture for a test plugin class"""

    class TestPlugin(PluginInterface):
        """
        Implementation of a minimal plugin for testing purposes.
        """

        _NAME = "some-plugin"
        _DISPLAY_NAME = "Some Plugin"
        _DESCRIPTION = "Some plugin description"
        _CONTEXT = "testing"
        _DEPENDENCIES = [
            Dependency("some-dep", "its-version"),
            PythonDependency("pytest"),
            PythonDependency("pip"),
        ]
        _SIGNATURE = Signature(
            arg1=Argument(type_=JSONType.STRING, required=True),
            arg2=Argument(type_=JSONType.INTEGER, required=False, default=1),
        )

        def _get(self, context: PluginExecutionContext, /, **kwargs):
            return context.result

    return TestPlugin


@pytest.fixture(scope="module", name="test_plugin")
def _test_plugin(test_plugin_class):
    """Fixture for a test plugin instance"""

    return test_plugin_class()


def test_subclasshook():
    """Test method `subclasshook` of `PluginInterface`."""

    class BadPlugin:
        """Test plugin."""

        _DISPLAY_NAME = None
        _DESCRIPTION = None
        _DEPENDENCIES = None
        _SIGNATURE = None
        _INFO = None
        _CONTEXT = None

        name = None
        display_name = None
        description = None
        signature = None
        dependencies = None
        info = None

        def get(self):
            """Faked get."""
            return

        def validate(self):
            """Faked validate."""
            return

        def requirements_met(self):
            """Faked validate."""
            return True, "ok"

    assert not issubclass(BadPlugin, PluginInterface)

    class GoodPlugin(BadPlugin):
        """Test plugin."""

        _NAME = None

    assert issubclass(GoodPlugin, PluginInterface)


def test_json(test_plugin):
    """Test property `json` of an implementation of the interface."""
    json = test_plugin.json
    assert "name" in json and json["name"] == test_plugin.name
    assert (
        "description" in json
        and json["description"] == test_plugin.description
    )
    assert "context" in json and json["context"] == test_plugin.context
    assert (
        "signature" in json and json["signature"] == test_plugin.signature.json
    )
    assert (
        "dependencies" in json
        and json["dependencies"] == test_plugin.dependencies.json
    )


def test_json_no_context_dependencies():
    """
    Test property `json` of an implementation of the interface without
    context.
    """

    class TestPlugin(PluginInterface):
        """
        Implementation of a minimal plugin without context for testing
        purposes.
        """

        _NAME = "some-plugin"
        _DISPLAY_NAME = "Some Plugin"
        _DESCRIPTION = "Some plugin description"
        _SIGNATURE = Signature()

        def _get(self, context: PluginExecutionContext, /, **kwargs):
            return context.result

    assert "context" not in TestPlugin().json
    assert "dependencies" not in TestPlugin().json


def test_name(test_plugin):
    """
    Test the `name` method
    of an implementation of the interface
    """

    assert isinstance(test_plugin.name, str)
    assert test_plugin.name == "some-plugin"


def test_display_name(test_plugin):
    """
    Test the `display_name` method
    of an implementation of the interface
    """

    assert isinstance(test_plugin.display_name, str)
    assert test_plugin.display_name == "Some Plugin"


def test_description(test_plugin):
    """
    Test the `description` method
    of an implementation of the interface
    """

    assert isinstance(test_plugin.description, str)
    assert test_plugin.description == "Some plugin description"


def test_dependencies(test_plugin):
    """
    Test the `dependencies` method
    of an implementation of the interface
    """

    dependencies_json = test_plugin.dependencies.json
    assert dependencies_json == {
        "pytest": version("pytest"),
        "pip": version("pip"),
        "some-dep": "its-version",
    }


def test_signature(test_plugin):
    """
    Test the `signature` method of an implementation of the interface
    """

    assert isinstance(test_plugin.signature, Signature)
    assert isinstance(test_plugin.signature.json, dict)


def test_info(test_plugin_class):
    """
    Test the `info` method of an implementation of the interface
    """

    # without info
    assert test_plugin_class.info is None
    assert "info" not in test_plugin_class.json

    # with info
    @dataclass
    class InfoModel(DataModel):
        """info model"""

        data: str

    info = InfoModel("data")

    class TestPluginWithInfoDataModel(test_plugin_class):
        """Test plugin with info-DataModel"""

        _INFO = info

    class TestPluginWithInfoPlain(test_plugin_class):
        """Test plugin with info-JSONObject"""

        _INFO = {"data": "data"}

    assert isinstance(TestPluginWithInfoDataModel.info, dict)
    assert isinstance(TestPluginWithInfoPlain.info, dict)
    assert "info" in TestPluginWithInfoDataModel.json
    assert TestPluginWithInfoDataModel.json["info"] == info.json
    assert TestPluginWithInfoDataModel.json == TestPluginWithInfoPlain.json


def test_get_method_all_kwargs(test_plugin):
    """
    Test the `get` method
    of an implementation of the interface
    with all keyword arguments.
    """

    result = test_plugin.get(None, arg1="some path", arg2=2)

    assert isinstance(result, PluginResult)
    assert isinstance(result.log, Logger)


def test_validate_method_unknown_argument(test_plugin):
    """
    Test the `validate` method
    of an implementation of the interface
    with an unknown argument.
    """

    result = test_plugin.validate({"arg1": "some path", "arg3": 0})
    assert not result[0]
    assert "arg3" in result[1]


def test_validate_method_missing_argument(test_plugin):
    """
    Test the `validate` method
    of an implementation of the interface
    with a missing required argument.
    """

    result = test_plugin.validate({"arg2": 0})
    assert not result[0]
    assert "arg1" in result[1]


def test_validate_method_optional(test_plugin):
    """
    Test the `validate` method
    of an implementation of the interface
    with an optional argument.
    """

    result = test_plugin.validate({"arg1": "some path"})
    assert result[0]


def test_validate_method_bad_type(test_plugin):
    """
    Test the `validate` method
    of an implementation of the interface
    with an argument of bad type.
    """

    result = test_plugin.validate({"arg1": "some path", "arg2": "2"})
    assert not result[0]
    assert "arg2" in result[1]
