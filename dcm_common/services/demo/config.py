"""Configuration module for the 'Demo'-app."""

from importlib.metadata import version

from dcm_common.plugins.demo import DemoPlugin
from dcm_common.services import FSConfig, OrchestratedAppConfig, DBConfig


class AppConfig(FSConfig, OrchestratedAppConfig, DBConfig):
    """
    Configuration for the 'Demo'-app.
    """

    AVAILABLE_PLUGINS = {DemoPlugin.name: DemoPlugin()}

    def set_identity(self) -> None:
        super().set_identity()
        self.CONTAINER_SELF_DESCRIPTION["description"] = (
            "This API is a demonstration for DCM-services."
        )

        # version
        self.CONTAINER_SELF_DESCRIPTION["version"]["api"] = "0.0.0"
        self.CONTAINER_SELF_DESCRIPTION["version"]["app"] = version(
            "dcm-common"
        )

        # plugins
        self.CONTAINER_SELF_DESCRIPTION["configuration"]["plugins"] = {
            plugin.name: plugin.json
            for plugin in self.AVAILABLE_PLUGINS.values()
        }
