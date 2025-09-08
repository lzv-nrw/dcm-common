"""
Test module for dynamic loading of external plugins.
"""

from pathlib import Path

import pytest

from dcm_common.plugins import (
    import_from_file,
    import_from_directory,
    PluginInterface,
)


@pytest.fixture(name="plugin_directory")
def _plugin_directory(fixtures):
    return fixtures / "plugins"


def test_import_from_file_minimal(plugin_directory: Path):
    """Test function `import_from_file` for simple scenario."""
    plugin = import_from_file(plugin_directory / "a.py")
    assert issubclass(type(plugin), PluginInterface)
    assert plugin.name == "a-plugin"


def test_import_from_file_wrong_name(plugin_directory: Path):
    """
    Test function `import_from_file` for scenario in which the module
    contains another plugin-definition with bad name.
    """
    plugin = import_from_file(plugin_directory / "b.py")
    assert issubclass(type(plugin), PluginInterface)
    assert plugin.name == "b-plugin"


def test_import_from_file_empty(plugin_directory: Path):
    """
    Test function `import_from_file` for scenario in which the module
    does not contain any valid plugin.
    """
    assert import_from_file(plugin_directory / "c.py") is None


def test_import_from_file_filter(plugin_directory: Path):
    """Test function `import_from_file` for simple scenario."""
    assert (
        import_from_file(
            plugin_directory / "a.py", filter_=lambda p: p.context == "unknown"
        )
        is None
    )


def test_import_from_directory_minimal(plugin_directory: Path):
    """Test function `import_from_directory`."""
    plugins = import_from_directory(plugin_directory)

    assert len(plugins) == 2
    assert "a-plugin" in plugins
    assert "b-plugin" in plugins
    assert all(p.name == identifier for identifier, p in plugins.items())
    assert all(issubclass(type(p), PluginInterface) for p in plugins.values())


def test_import_from_directory_filter(plugin_directory: Path):
    """Test function `import_from_directory`."""
    plugins = import_from_directory(
        plugin_directory, filter_=lambda p: p.context == "testing"
    )

    assert len(plugins) == 1
    assert "a-plugin" in plugins
