"""Definitions for dynamic import of external plugins."""

from typing import Optional, Callable
from pathlib import Path
import importlib
from inspect import getmembers, isclass

from dcm_common.util import list_directory_content
from .interface import PluginInterface


def import_from_file(
    path: Path,
    filter_: Callable[[type[PluginInterface]], bool] = lambda p: True,
) -> Optional[PluginInterface]:
    """
    Loads implementation `ExternalPlugin` of `PluginInterface` from file
    at `path`, applies filter, and returns instance.

    Note that plugins are instantiated without arguments. If a plugin
    needs additional information in its constructor, the recommended way
    is to read that information from the environment instead.

    Keyword arguments:
    path -- module path
    filter_ -- optional filter for implementations
               (default lambda p: True)
    """

    # load module from string
    spec = importlib.util.spec_from_loader(
        name=path.stem,
        loader=None,
        origin=path.read_text(encoding="utf-8"),
    )
    module = importlib.util.module_from_spec(spec)
    exec(spec.origin, module.__dict__)  # pylint: disable=exec-used

    # filter for valid plugins
    try:
        _, plugin = list(
            filter(
                lambda m: m[0] == "ExternalPlugin"
                and issubclass(m[1], PluginInterface)
                and filter_(m[1]),
                getmembers(module, isclass),
            )
        )[0]
    except IndexError:
        return None

    return plugin()


def import_from_directory(
    path: Path,
    filter_: Callable[[type[PluginInterface]], bool] = lambda p: True,
) -> dict[str, PluginInterface]:
    """
    Loads implementations of `PluginInterface` from directory `path`.
    Returns filtered mapping of plugin-name and -instance.

    Note that plugins are instantiated without arguments. If a plugin
    needs additional information in its constructor, the recommended way
    is to read that information from the environment instead.

    Keyword arguments:
    path -- search path for plugins
    filter_ -- optional filter for implementations
               (default lambda p: True)
    """

    return {
        plugin.name: plugin
        for plugin in map(
            import_from_file,
            list_directory_content(
                path,
                pattern="**/*",
                condition_function=lambda p: p.is_file() and p.suffix == ".py",
            ),
        )
        if plugin is not None and filter_(plugin)
    }
