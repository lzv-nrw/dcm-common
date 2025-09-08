"""Test suite for the config-module."""

from dcm_common.services.config import FSConfig


def test_fsconfig_constructor():
    """Test constructor of `FSConfig`."""

    class AnotherConfig(FSConfig):
        FS_MOUNT_POINT = "some-value"

    config = AnotherConfig()
    assert (
        config.CONTAINER_SELF_DESCRIPTION["configuration"]["settings"][
            "fs_mount_point"
        ]
        == config.FS_MOUNT_POINT
    )


def test_fsconfig_set_identity_minimal():
    """Test method `set_identity` of `FSConfig` for minimal setup."""

    config = FSConfig()
    config.FS_MOUNT_POINT = "some-value"
    assert (
        config.CONTAINER_SELF_DESCRIPTION["configuration"]["settings"][
            "fs_mount_point"
        ]
        != config.FS_MOUNT_POINT
    )
    config.set_identity()
    assert (
        config.CONTAINER_SELF_DESCRIPTION["configuration"]["settings"][
            "fs_mount_point"
        ]
        == config.FS_MOUNT_POINT
    )


def test_fsconfig_set_identity_inheritance():
    """
    Test method `set_identity` of `FSConfig` when extending base-class.
    """

    class AnotherConfig(FSConfig):
        ANOTHER_KEY = "some-value"

        def set_identity(self):
            super().set_identity()
            self.CONTAINER_SELF_DESCRIPTION["another_key"] = self.ANOTHER_KEY

    base_config = FSConfig()
    config = AnotherConfig()
    assert len(base_config.CONTAINER_SELF_DESCRIPTION) + 1 == len(
        config.CONTAINER_SELF_DESCRIPTION
    )


def test_fsconfig_version_lib():
    """
    Test method generation of "version.lib" in self-description via
    `set_identity` of `FSConfig`.
    """
    assert all(
        lib in FSConfig().CONTAINER_SELF_DESCRIPTION["version"]["lib"]
        for lib in ("dcm-common", "Flask")
    )
