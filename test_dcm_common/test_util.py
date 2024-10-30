"""
This module contains pytest-tests for testing the util-module.

All tests use the test data located in fixtures.
The tests generate data in a temporary directory.
"""

import os
from pathlib import Path
import shutil
import json
from unittest import mock
from http.server import HTTPServer, BaseHTTPRequestHandler
from multiprocessing import Process

import pytest

from dcm_common import util


@pytest.fixture(name="get_simple_http_server")
def _get_simple_http_server():
    # setup fake server
    def _(data):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(data)
        return HTTPServer(("localhost", 8080), Handler)
    return _


@pytest.fixture(name="run_simple_http_server")
def _run_simple_http_server():
    # setup fake server
    def _(http, request):
        # run fake server
        p = Process(
            target=http.serve_forever,
            daemon=True
        )
        def __():  # kill server after this test
            if p.is_alive():
                p.kill()
                p.join()
        request.addfinalizer(__)
        p.start()
    return _


def test_get_profile_remote(
    bagit_profile_test, run_simple_http_server, get_simple_http_server, request
):
    """ Test loading a valid remote profile """

    run_simple_http_server(
        get_simple_http_server(
            bagit_profile_test.read_bytes()
        ),
        request
    )

    some_remote_test_profile = util.get_profile("http://localhost:8080")

    profile_identifier = (some_remote_test_profile["BagIt-Profile-Info"]
                                                  ["BagIt-Profile-Identifier"])
    assert profile_identifier == str(bagit_profile_test)


def test_get_profile_remote_txt(
    run_simple_http_server, get_simple_http_server, request
):
    """ Test loading a url with a txt """
    run_simple_http_server(
        get_simple_http_server(
            b"Some text\nSecond line"
        ),
        request
    )
    with pytest.raises(json.JSONDecodeError):
        util.get_profile("http://localhost:8080")


def test_get_profile_local(bagit_profile_test):
    """  Test loading a valid local profile """
    some_local_test_profile = util.get_profile(bagit_profile_test)
    profile_identifier = (some_local_test_profile["BagIt-Profile-Info"]
                                                 ["BagIt-Profile-Identifier"])
    assert profile_identifier == str(bagit_profile_test)


def test_get_profile_local_empty_json(temporary_directory):
    # Create an empty json file
    filename = temporary_directory / "empty_profile.json"
    with filename.open("w", encoding="utf-8") as my_file:
        json.dump({}, my_file)
    # Load the empty json as profile
    a_local_profile = util.get_profile(filename)
    # Assert the profile is empty
    assert not a_local_profile
    # Delete the json file
    filename.unlink()


def test_get_profile_local_some_json(temporary_directory):
    # Create a json file
    filename = temporary_directory / "some_profile.json"
    some_dict = {"key1": "value1", "key2": "value2"}
    some_json_string = json.dumps(some_dict, indent=4)
    filename.write_text(some_json_string, encoding="utf-8")
    # Load the json as profile
    some_local_profile = util.get_profile(filename, encoding="utf-8")
    # Assert the profile is not empty
    assert bool(some_local_profile)
    # Delete the json file
    filename.unlink()


def test_get_profile_local_txt_json_syntax(temporary_directory):
    # Create a txt file with some text
    filename = temporary_directory / "some_profile.txt"
    filename.write_text("""{"key1": "value1","key2": "value2"}""", encoding="utf-8")
    # Load the txt as profile
    some_local_profile = util.get_profile(filename, encoding="utf-8")
    # Assert the profile is not empty
    assert bool(some_local_profile)
    # Delete the txt file
    filename.unlink()


def test_get_profile_local_txt_freetext(temporary_directory):
    # Create a txt file with some text
    filename = temporary_directory / "some_profile.txt"
    filename.write_text("some text", encoding="utf-8")
    # Try to load the txt as profile
    with pytest.raises(json.JSONDecodeError):
        util.get_profile(filename, encoding="utf-8")
    # Delete the txt file
    filename.unlink()


def test_list_directory_content(temporary_directory):
    # list of test-directories
    dirs = [
        Path("A"),
        Path("B"),
        Path("B/C")
    ]
    # create dirs and test files
    for this_dir in dirs:
        (temporary_directory / "ldc" / this_dir).mkdir(parents=True)
        util.write_test_file(
            temporary_directory / "ldc" / this_dir / "test.txt"
        )

    # check
    list_of_files = util.list_directory_content(
        temporary_directory / "ldc",
        pattern="**/*",
        condition_function=lambda p: p.is_file()
    )
    list_of_dirs = util.list_directory_content(
        temporary_directory / "ldc",
        pattern="**/*",
        condition_function=lambda p: p.is_dir()
    )
    list_of_files_in_subdir = util.list_directory_content(
        temporary_directory / "ldc",
        pattern="*",
        condition_function=lambda p: p.is_file()
    )

    # cleanup
    shutil.rmtree(temporary_directory / "ldc")

    assert len(list_of_files) == len(dirs)
    assert len(list_of_dirs) == len(dirs)
    assert len(list_of_files_in_subdir) == 0


def test_make_path():
    path_from_str = util.make_path("A/")
    path_from_path = util.make_path(Path("A/"))

    assert path_from_str == path_from_path


@pytest.mark.parametrize(
    ("test_input", "expected_output"),
    [
        (
            ({"A": {"B": 1}},  ["A", "B"]),
            1
        ), # nested_value_exists
        (
            ({"A": "B"},  ["A", "B"]),
            None
        ), # key_exists_but_no_value
        (
            ({"A": {"B": 1}},  ["A", "C"]),
            None
        ), # key_not_exist
    ],
    ids=[
        "nested_value_exists",
        "key_exists_but_no_value",
        "key_not_exist",
    ]
)
def test_value_from_dict_path(
    test_input,
    expected_output
):
    """
    Test function value_from_dict_path
    """

    value = util.value_from_dict_path(*test_input)
    assert value == expected_output


def test_write_test_file(temporary_directory):
    # create file and generate directories as needed
    util.write_test_file(
        temporary_directory / "write_file_test" / "test.txt", mkdir=True
    )
    # does the file exist?
    does_exist = \
        (temporary_directory / "write_file_test" / "test.txt").is_file()
    # clean up
    (temporary_directory / "write_file_test" / "test.txt").unlink()
    (temporary_directory / "write_file_test").rmdir()

    assert does_exist


def test_write_test_file_bad_directory(temporary_directory):
    # create file but do not create directories
    with pytest.raises(FileNotFoundError):
        util.write_test_file(
            temporary_directory / "write_file_test" / "test.txt", mkdir=False
        )


def test_now():
    """Test function `now` with utcdelta-parameter."""

    time0 = util.now(False, -1)
    time1 = util.now(False, 0)
    time2 = util.now(False, 1)

    assert time0 <= time1
    assert time1 <= time2
    assert time0.isoformat().endswith("-01:00")
    assert time1.isoformat().endswith("+00:00")
    assert time2.isoformat().endswith("+01:00")


def test_now_env():
    """
    Test function `now` with utcdelta-parameter set via environment variable.
    """

    time0 = util.now(False)
    os.environ["UTC_TIMEZONE_OFFSET"] = "1"
    time1 = util.now(False)
    assert time0.isoformat().endswith("+00:00")
    assert time1.isoformat().endswith("+01:00")
    del os.environ["UTC_TIMEZONE_OFFSET"]


def test_get_output_path_simple(temporary_directory):
    """Test function `get_output_path`."""

    result = util.get_output_path(temporary_directory)
    assert result.is_dir()


def test_get_output_path_no_mkdir(temporary_directory):
    """Test function `get_output_path`."""

    result = util.get_output_path(temporary_directory, mkdir=False)
    assert isinstance(result, Path)
    assert not result.exists()


def test_get_output_path_fail(temporary_directory):
    """Test function `get_output_path`."""

    no_uuid = "no-uuid"
    (temporary_directory / no_uuid).mkdir(exist_ok=True, parents=True)
    with mock.patch(
        "dcm_common.util.uuid4", return_value=no_uuid
    ):
        result = util.get_output_path(
            temporary_directory, max_retries=2
        )
        assert result is None
