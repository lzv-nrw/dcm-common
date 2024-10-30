"""Module providing helper functions for the project LZV.nrw."""

from typing import Callable, TypeAlias, Mapping, Optional
import os
from pathlib import Path
from urllib import request
from json import loads as json_loads
from datetime import datetime, timezone, timedelta
from uuid import uuid4


NestedDict: TypeAlias = Mapping[str, "str | list[str] | NestedDict"]


def get_profile(
    url: str | Path,
    is_local_file: bool = False,
    is_remote_file: bool = False,
    encoding: str = "utf-8",
    remote_timeout: int = 10
) -> NestedDict:
    """
    Returns a dictionary generated from a JSON-format (remote or local)
    file.

    Raises a FileNotFoundError if neither remote or local file can be
    read.

    Keyword argument:
    url -- the path to the json document,
           either as a url or as a local path
    is_local_file -- skip checking for remote
                     (default False)
    is_remote_file -- skip checking for local
                      (default False)
    encoding -- file encoding
                (default utf-8)
    remote_timeout -- a timeout in seconds for blocking operations
                      like the connection attempt, see request.urlopen
                      (default 10)
    """

    # handle bad input
    if is_local_file and is_remote_file:
        raise ValueError(
            "Either 'is_local_file' or 'is_remote_file' needs to be False."
        )

    # initialize working variable
    json_str = None
    if not isinstance(url, Path) and not is_local_file:
        # try to read file contents from url and catch ValueError
        try:
            with request.urlopen(url, timeout=remote_timeout) as remote_file:
                json_str = remote_file.read()
            json_str = json_str.decode(encoding)
        except ValueError:
            json_str = None

    # check if result from url usable
    if json_str is None and not is_remote_file:
        # try to read from a local file instead
        path = make_path(url)
        if path.is_file():
            json_str = path.read_text(encoding=encoding)

    # finish up
    if json_str is None:
        raise FileNotFoundError(
            f"File or url '{url}' did not yield any useful input."
        )
    return json_loads(json_str)


def list_directory_content(
    path: str | Path,
    pattern: str = "*",
    condition_function: Callable[[Path], bool] = lambda p: True
) -> list[Path]:
    """
    Function for collecting the contents of a directory using
    Path.glob. Can be customized by providing a glob-pattern and a
    condition_function.

    Keyword argument:
    path -- path to the directory

    Optional arguments:
    pattern -- glob pattern
               (default "*"; only collects the immediate contents of
               the directory)
    condition_function -- the condition function that every entry is
                          required to pass, e.g. lambda p: p.is_dir()
                          (default lambda p: True)
    """

    return [
        x for x in make_path(path).glob(pattern) if condition_function(x)
    ]


def make_path(path: str | Path) -> Path:
    """
    A convenience-function returning a `Path`-object created from path.

    Keyword arguments:
    path -- filesystem path either as str or Path
    """

    if isinstance(path, str):
        return Path(path)
    return path


def value_from_dict_path(
    nesteddict: NestedDict,
    path: list[str]
) -> Optional[str | list[str]]:
    """
    Returns value in nested dict from path.

    Keyword arguments:
    nesteddict -- (nested) dictionary
    path -- list containing the keys to the (nested) dictionary
    """

    # start in outer dict
    current_dict = nesteddict  # type: Any
    # iterate keys in path
    for key in path:
        # replace working dict if key available
        if isinstance(current_dict, dict)\
                and key in current_dict:
            current_dict = current_dict[key]
        else:
            return None
    return current_dict


def write_test_file(path: str | Path, mkdir: bool = False) -> None:
    """
    Write a dummy file at path.

    Keyword arguments:
    path -- path to file
    mkdir -- (optional) create required directories on the fly
             (default False)
    """

    _path = make_path(path)

    if mkdir and not _path.parent.is_dir():
        _path.parent.mkdir(parents=True, exist_ok=True)

    _path.write_text("", encoding="utf-8")


def now(keep_micro: bool = False, utcdelta: Optional[int] = None) -> datetime:
    """
    Helper for getting datetime.now() in specific format for UTC + utcdelta.

    Keyword arguments:
    keep_micro -- if `False`, set datetime microseconds to zero
                  (default False)
    utcdelta -- optional timedelta for UTC-timezone in hours
                (default None: if `None`, either the environment
                variable `UTC_TIMEZONE_OFFSET` (if set) or a fallback of
                0 is used)
    """

    if utcdelta is None:
        _utcdelta = int(os.environ.get("UTC_TIMEZONE_OFFSET") or 0)
    else:
        _utcdelta = utcdelta

    # mypy - hint
    assert _utcdelta is not None

    if keep_micro:
        return datetime.now(tz=timezone(timedelta(hours=_utcdelta)))
    return datetime.now(
        tz=timezone(timedelta(hours=_utcdelta))
    ).replace(microsecond=0)


def get_output_path(
    base_path: Path,
    max_retries: int = 10,
    mkdir: bool = True
) -> Optional[Path]:
    """
    On success, it returns a `Path` that did not previously exist
    in `base_path`. Returns `None` if `max_retries` is exceeded.

    Keyword arguments:
    base_path -- base path in which to create a new directory
    max_retries -- maximum number of retries before exiting
                   (default 10)
    mkdir -- if `True`, call mkdir for directory
             (default True)
    """

    for _ in range(max_retries):
        try:
            (output := base_path / str(uuid4())).mkdir(
                exist_ok=False, parents=True
            )
        except FileExistsError:
            pass
        else:
            if not mkdir:
                output.rmdir()
            return output
    return None
