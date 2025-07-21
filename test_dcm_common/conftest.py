""" Configure the tests """

from pathlib import Path
from shutil import rmtree

import pytest
from dcm_common.services.tests import external_service, run_service


TESTING_DIR = Path("test_dcm_common/tmp")
BAGIT_PROFILE_TEST = Path("test_dcm_common/fixtures/test_profile.json")


def pytest_sessionstart():
    """
    Create the temporary directory to store the test results
    before running the tests.
    """
    if TESTING_DIR.is_dir():
        rmtree(TESTING_DIR)
    TESTING_DIR.mkdir(exist_ok=True)


def pytest_sessionfinish():
    """
    Remove the temporary directory after whole test run finished.
    """
    if TESTING_DIR.is_dir():
        rmtree(TESTING_DIR)


@pytest.fixture()
def temporary_directory():
    """
    Return the path for the temporary directory.
    """
    return TESTING_DIR


@pytest.fixture()
def bagit_profile_test():
    """
    Return the path to a local test bagit profile.
    """
    return BAGIT_PROFILE_TEST


@pytest.fixture(name="fixtures")
def _fixtures():
    """
    Return the path for the temporary directory.
    """
    return Path("test_dcm_common/fixtures")
