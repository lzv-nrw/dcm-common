"""
Test module for `PluginInterface`-extensions.
"""

from time import time

from dcm_common.logger import LoggingContext as Context, Logger
from dcm_common.plugins import (
    Signature,
    PluginInterface,
    FSPlugin,
    TimeoutAndRetryPlugin,
    PluginExecutionContext,
)


def test_fs_extension(temporary_directory):
    """Test whether inheritance from `FSPlugin` works correctly."""

    class TestFSPlugin(PluginInterface, FSPlugin):
        """
        Implementation of a minimal fs-plugin for testing purposes.
        """

        _NAME = "some-plugin"
        _DISPLAY_NAME = "Some Plugin"
        _DESCRIPTION = "Some plugin description"
        _DEPENDENCIES = []
        _SIGNATURE = Signature()

        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)

        def _get(self, context: PluginExecutionContext, /, **kwargs):
            return context.result

    fs_plugin = TestFSPlugin(working_dir=temporary_directory)

    assert hasattr(fs_plugin, "working_dir")
    assert fs_plugin.working_dir == temporary_directory
    assert hasattr(fs_plugin, "new_output")
    assert callable(fs_plugin.new_output)


def test_timeout_and_retry_extension():
    """
    Test whether inheritance from `TimeoutAndRetryPlugin` works
    correctly.
    """

    class TestTaRPlugin(PluginInterface, TimeoutAndRetryPlugin):
        """
        Implementation of a minimal timeout&retry-type-plugin for
        testing purposes.
        """

        _NAME = "some-plugin"
        _DISPLAY_NAME = "Some Plugin"
        _DESCRIPTION = "Some plugin description"
        _DEPENDENCIES = []
        _SIGNATURE = Signature()

        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)

        def _get(self, context: PluginExecutionContext, /, **kwargs):
            return context.result

    tar_plugin = TestTaRPlugin()

    assert hasattr(tar_plugin, "timeout")
    assert hasattr(tar_plugin, "retries")
    assert hasattr(tar_plugin, "retry_interval")
    assert hasattr(tar_plugin, "_retry")
    assert callable(tar_plugin._retry)  # pylint: disable=protected-access


def test_timeout_and_retry():
    """Test method `_retry` of `TimeoutAndRetryPlugin`-Extension."""

    class TestPlugin(TimeoutAndRetryPlugin):
        """Minimal test-plugin."""

        name = "Test-Plugin"

        def __init__(self, **kwargs) -> None:
            self.current_retry = 0
            self.required_retries = 0
            super().__init__(**kwargs)

        def _run(self) -> str:
            """Actually run plugin logic."""
            if self.current_retry < self.required_retries:
                self.current_retry += 1
                raise ValueError("Timeout")
            return "ok"

        def run(self, required_retries: int = 0) -> tuple[Logger, str]:
            """Run plugin logic and retry if failed."""
            self.current_retry = 0
            self.required_retries = required_retries
            return self._retry(
                self._run, description="running plugin", exceptions=ValueError
            )

    # test general mechanism and logging
    plugin = TestPlugin(retry_interval=0)
    result = plugin.run(0)
    assert Context.ERROR not in result[0]
    assert result[1] == "ok"
    result = plugin.run(1)
    assert len(result[0][Context.ERROR]) == 1
    result = plugin.run(2)
    assert len(result[0][Context.ERROR]) == 2

    # test interval
    time0 = time()
    result = plugin.run(1)
    assert time() - time0 <= 0.1

    plugin2 = TestPlugin(retry_interval=0.5)
    time0 = time()
    result = plugin2.run(1)
    assert time() - time0 >= 0.5


def test_multiple_extensions(temporary_directory):
    """
    Test whether inheritance from multiple plugin-extensions works
    correctly.
    """

    class TestPlugin(PluginInterface, FSPlugin, TimeoutAndRetryPlugin):
        """
        Implementation of a minimal multi-extension plugin.
        """

        _NAME = "some-plugin"
        _DISPLAY_NAME = "Some Plugin"
        _DESCRIPTION = "Some plugin description"
        _DEPENDENCIES = []
        _SIGNATURE = Signature()

        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)

        def _get(self, context: PluginExecutionContext, /, **kwargs):
            return context.result

    plugin = TestPlugin(working_dir=temporary_directory)

    assert hasattr(plugin, "working_dir")
    assert hasattr(plugin, "timeout")
    assert hasattr(plugin, "retries")
    assert hasattr(plugin, "retry_interval")
