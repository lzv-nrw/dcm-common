"""Tests for the `dilled`-module."""

from pathlib import Path
from uuid import uuid4
from dataclasses import dataclass
from multiprocessing import Process, Pipe
import sqlite3

import pytest
import dill

from dcm_common.orchestra import (
    DillIgnore,
    DilledProcess,
    DilledPipe,
    dillignore,
)


def test_dilled_process_w_locals(temporary_directory: Path):
    """
    Test compatibility of `DilledProcess` with locals for target and
    args.
    """

    @dataclass
    class File:
        """Local class"""

        path: Path
        content: str

    def run(file1: File, *, file2: File):
        file1.path.write_text(file1.content, encoding="utf-8")
        file2.path.write_text(file2.content, encoding="utf-8")

    file1 = File(temporary_directory / str(uuid4()), "file1")
    file2 = File(temporary_directory / str(uuid4()), "file2")

    # works fine with DilledProcess
    p = DilledProcess(
        target=run,
        args=(file1,),
        kwargs={"file2": file2},
    )
    p.start()
    p.join()
    assert file1.path.is_file()
    assert file1.path.read_text(encoding="utf-8") == file1.content
    assert file2.path.is_file()
    assert file2.path.read_text(encoding="utf-8") == file2.content

    # but breaks for regular multiprocessing
    p = Process(
        target=run,
        args=(file1,),
        kwargs={"file2": file2},
    )
    with pytest.raises(AttributeError):
        p.start()


def test_dilled_pipe(temporary_directory: Path):
    """
    Test compatibility of `DilledPipe` with locals.
    """

    @dataclass
    class File:
        """Local class"""

        path: Path
        content: str

    file1 = File(temporary_directory / str(uuid4()), "file1")
    file2 = File(temporary_directory / str(uuid4()), "file2")

    def run(pipe):
        try:  # non-DilledPipe case
            recv_file = pipe.recv()
        except EOFError:
            return
        recv_file.path.write_text(recv_file.content, encoding="utf-8")
        pipe.send(file2)

    # works fine with DilledPipe
    pipe_parent, pipe_child = DilledPipe()

    p = DilledProcess(
        target=run,
        args=(
            # dill otherwise breaks file-descriptor
            DillIgnore(pipe_child),
        ),
    )
    p.start()
    pipe_parent.send(file1)
    recv_file: File = pipe_parent.recv()

    assert file1.path.is_file()
    assert file1.path.read_text(encoding="utf-8") == file1.content
    assert recv_file.path == file2.path
    assert recv_file.content == file2.content

    # but breaks for regular multiprocessing
    pipe_parent, pipe_child = Pipe()
    p = DilledProcess(  # still needs DilledProcess to support local target
        target=run,
        args=(DillIgnore(pipe_child),),
    )
    p.start()
    with pytest.raises(AttributeError):
        pipe_parent.send(file1)


def test_dillignore_decorator():
    """
    Test decorator `dillignore`. Uses sqlite3-connection as unpicklable
    by dill.
    """

    # check base
    class Unpicklable:
        """Test case that cannot be pickled as is."""

        def __init__(self, p):
            self.db = sqlite3.connect(":memory:")
            self.other_property = p

    with pytest.raises(TypeError):
        dill.loads(dill.dumps(Unpicklable(0)))

    # using dillignore
    @dillignore("db")
    class Picklable:
        """Test case that can be pickled."""

        def __init__(self, p):
            self.db = sqlite3.connect(":memory:")
            self.other_property = p

    obj = Picklable(0)
    pickled_and_unpickled = dill.loads(dill.dumps(obj))
    assert obj.other_property == pickled_and_unpickled.other_property

    # accessing ignored property raises error
    obj.db.close()
    with pytest.raises(RuntimeError):
        pickled_and_unpickled.db.close()


def test_dillignore_decorator_inheritance():
    """
    Test decorator `dillignore`'s behavior for inheritance. Uses
    sqlite3-connection as unpicklable by dill.
    """

    @dillignore("db")
    class Picklable:
        """Test case that can be pickled."""

        def __init__(self, p):
            self.db = sqlite3.connect(":memory:")
            self.other_property = p

    # * simple inheritance does work
    class SubPicklable(Picklable):
        """Test case that can be pickled."""

    dill.loads(dill.dumps(SubPicklable(0)))

    # * inheriting form a class with dillignore and adding another
    #   property does not work -> needs to provide an extensive list on
    #   last application of decorator
    @dillignore("db2")
    class SubPicklable2(Picklable):
        """Test case that cannot be pickled."""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.db2 = sqlite3.connect(":memory:")

    with pytest.raises(TypeError):
        dill.loads(dill.dumps(SubPicklable2(0)))

    @dillignore("db", "db2")
    class SubPicklable3(Picklable):
        """Test case that can be pickled."""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.db2 = sqlite3.connect(":memory:")

    dill.loads(dill.dumps(SubPicklable3(0)))
