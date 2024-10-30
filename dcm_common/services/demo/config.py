"""Configuration module for the 'Demo'-app."""

from importlib.metadata import version

from dcm_common.services import FSConfig, OrchestratedAppConfig


class AppConfig(FSConfig, OrchestratedAppConfig):
    """
    Configuration for the 'Demo'-app.
    """

    def set_identity(self) -> None:
        super().set_identity()
        self.CONTAINER_SELF_DESCRIPTION["description"] = (
            "This API is a demonstration for DCM-services."
        )

        # version
        self.CONTAINER_SELF_DESCRIPTION["version"]["api"] = (
            "0.0.0"
        )
        self.CONTAINER_SELF_DESCRIPTION["version"]["app"] = version(
            "dcm-common"
        )
