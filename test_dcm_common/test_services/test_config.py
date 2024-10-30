"""Test suite for the config-module."""

from uuid import uuid4
from pathlib import Path

import pytest

from dcm_common.db import (
    JSONFileStore, HTTPKeyValueStoreAdapter, PostgreSQLAdapter14
)
from dcm_common.services.config import FSConfig, OrchestratedAppConfig


def test_fsconfig_constructor():
    """Test constructor of `FSConfig`."""
    class AnotherConfig(FSConfig):
        FS_MOUNT_POINT = "some-value"

    config = AnotherConfig()
    assert (
        config.CONTAINER_SELF_DESCRIPTION["configuration"]["settings"]["fs_mount_point"] ==
        config.FS_MOUNT_POINT
    )


def test_fsconfig_set_identity_minimal():
    """Test method `set_identity` of `FSConfig` for minimal setup."""

    config = FSConfig()
    config.FS_MOUNT_POINT = "some-value"
    assert (
        config.CONTAINER_SELF_DESCRIPTION["configuration"]["settings"]["fs_mount_point"]
        != config.FS_MOUNT_POINT
    )
    config.set_identity()
    assert (
        config.CONTAINER_SELF_DESCRIPTION["configuration"]["settings"]["fs_mount_point"]
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
    assert (
        len(base_config.CONTAINER_SELF_DESCRIPTION) + 1 ==
        len(config.CONTAINER_SELF_DESCRIPTION)
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


def test_orchestratedappconfig_constructor_minimal():
    """
    Test adapter initialization in the `OrchestratedAppConfig`-
    constructor.
    """
    _ = OrchestratedAppConfig()


def test_orchestratedappconfig_constructor_unknown_adapter():
    """
    Test adapter initialization in the `OrchestratedAppConfig`-
    constructor with an unknown adapter-type.
    """

    class Config(OrchestratedAppConfig):
        ORCHESTRATION_QUEUE_ADAPTER = "unknown"

    with pytest.raises(ValueError):
        _ = Config()


def test_orchestratedappconfig_constructor_unknown_backend():
    """
    Test adapter initialization in the `OrchestratedAppConfig`-
    constructor with an unknown backend-type.
    """

    class Config(OrchestratedAppConfig):
        ORCHESTRATION_QUEUE_SETTINGS = {"backend": "unknown"}

    with pytest.raises(ValueError):
        _ = Config()


def test_orchestratedappconfig_constructor_disk_backend(temporary_directory):
    """
    Test adapter initialization in the `OrchestratedAppConfig`-
    constructor with a disk-backend.
    """

    class Config(OrchestratedAppConfig):
        ORCHESTRATION_QUEUE_SETTINGS = {
            "backend": "disk", "dir": str(temporary_directory / str(uuid4()))
        }

    config = Config()
    token = config.queue.push(None)
    assert JSONFileStore(
        dir_=Path(config.ORCHESTRATION_QUEUE_SETTINGS["dir"])
    ).read(token) is None


def test_orchestratedappconfig_constructor_disk_backend_missing_dir():
    """
    Test adapter initialization in the `OrchestratedAppConfig`-
    constructor with a disk-backend and missing directory in settings.
    """

    class Config(OrchestratedAppConfig):
        ORCHESTRATION_QUEUE_SETTINGS = {
            "backend": "disk"
        }

    with pytest.raises(KeyError):
        _ = Config()


def test_orchestratedappconfig_constructor_http_adapter():
    """
    Test adapter initialization in the `OrchestratedAppConfig`-
    constructor with an http-adapter.
    """

    class Config(OrchestratedAppConfig):
        ORCHESTRATION_QUEUE_ADAPTER = "http"
        ORCHESTRATION_QUEUE_SETTINGS = {"url": "-"}

    assert isinstance(Config().queue, HTTPKeyValueStoreAdapter)


def test_orchestratedappconfig_constructor_http_adapter_missing_url():
    """
    Test adapter initialization in the `OrchestratedAppConfig`-
    constructor with an http-adapter and missing url in settings.
    """

    class Config(OrchestratedAppConfig):
        ORCHESTRATION_QUEUE_ADAPTER = "http"
        ORCHESTRATION_QUEUE_SETTINGS = {}

    with pytest.raises(TypeError):
        _ = Config()


def test_orchestratedappconfig_constructor_postgres_adapter():
    """
    Test adapter initialization in the `OrchestratedAppConfig`-
    constructor with a postgres-adapter.
    """

    class Config(OrchestratedAppConfig):
        ORCHESTRATION_QUEUE_ADAPTER = "postgres14"
        ORCHESTRATION_QUEUE_SETTINGS = {
            "key_name": "key", "value_name": "value", "table": "queue"
        }

    assert isinstance(Config().queue, PostgreSQLAdapter14)


def test_orchestratedappconfig_constructor_postgres_adapter_missing_args():
    """
    Test adapter initialization in the `OrchestratedAppConfig`-
    constructor with a postgres-adapter and missing arg in settings.
    """

    class Config(OrchestratedAppConfig):
        ORCHESTRATION_QUEUE_ADAPTER = "postgres14"
        ORCHESTRATION_QUEUE_SETTINGS = {}

    with pytest.raises(TypeError):
        _ = Config()
