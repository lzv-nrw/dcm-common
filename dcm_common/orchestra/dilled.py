"""
Definition of an altered multiprocessing-module with support for dill-
pickles.
"""

from typing import Callable, Optional, Iterable, Mapping, Any
from dataclasses import dataclass
import multiprocessing
from multiprocessing.connection import Connection

import dill


@dataclass
class DillIgnore:
    """
    When used to wrap an argument that is passed to `DilledProcess`'s
    args or send via a `DilledPipe`, dill-pickling is skipped. This only
    works on top level, nesting within an argument is not supported.
    """

    value: Any


class DilledProcess(multiprocessing.Process):
    """
    Variant of a `multiprocessing.Process` that uses `dill` for pickling
    of target and arguments.

    This adds support for locals in target and arguments.
    """

    def __init__(
        self,
        *,
        target: Callable,
        args: Optional[Iterable] = None,
        kwargs: Optional[Mapping] = None,
        **other,
    ):
        super().__init__(target=lambda: None, args=(), kwargs={}, **other)
        # pickle data using dill but skip values wrapped in ProtectedArg
        self._target = dill.dumps(target)
        self._args = tuple(
            map(
                lambda arg: (
                    arg if isinstance(arg, DillIgnore) else dill.dumps(arg)
                ),
                args or (),
            )
        )
        self._kwargs = dict(
            map(
                lambda kwarg: (
                    kwarg[0],
                    (
                        kwarg[1]
                        if isinstance(kwarg[1], DillIgnore)
                        else dill.dumps(kwarg[1])
                    ),
                ),
                (kwargs or {}).items(),
            )
        )

    def run(self):
        # unpickle but unpack values wrapped in DillIgnore and run
        target = dill.loads(self._target)
        if target:
            target(
                *map(
                    lambda arg: (
                        arg.value
                        if isinstance(arg, DillIgnore)
                        else dill.loads(arg)
                    ),
                    self._args,
                ),
                **dict(
                    map(
                        lambda kwarg: (
                            kwarg[0],
                            (
                                kwarg[1].value
                                if isinstance(kwarg[1], DillIgnore)
                                else dill.loads(kwarg[1])
                            ),
                        ),
                        self._kwargs.items(),
                    )
                ),
            )


@dataclass
class DilledConnection:
    """
    Wrapper for `dill`-pickled `multiprocessing.connection.Connection`.
    """

    conn: Connection

    def send(self, obj: Any) -> None:
        """`dill`-wrapped send."""
        if isinstance(obj, DillIgnore):
            self.conn.send(obj)
        self.conn.send(dill.dumps(obj))

    def recv(self) -> Any:
        """`dill`-wrapped recv."""
        obj = self.conn.recv()
        if isinstance(obj, DillIgnore):
            return obj.value
        return dill.loads(obj)

    def close(self) -> None:
        """Close connection."""
        self.conn.close()

    def poll(self, timeout: Optional[float] = None) -> None:
        """Poll connection."""
        return self.conn.poll(timeout)


class DilledPipe:
    """
    Wrapper for `multiprocessing.Pipe` with support for `dill`-pickling.
    """

    # pylint: disable=too-few-public-methods

    def __init__(self, *args, **kwargs) -> None:
        parent, child = multiprocessing.Pipe(*args, **kwargs)
        self.parent = DilledConnection(parent)
        self.child = DilledConnection(child)

    def __iter__(self):
        return iter((self.parent, self.child))


def dillignore(*attr_names):
    """
    Class decorator to exclude given instance attributes from the
    pickled state. After unpickling, accessing those attributes raises
    a `RuntimeError`.

    Prerequisites/Caveats:
    * class must not define custom implementations of __getattribute__,
      __getstate__, and __setstate__
    * as a consequence of the first point:
      * inheriting from a class decorated with a dillignore and applying
        another dillignore for different attributes requires
        re-application for the previous attributes with that same
        decorator
      * this decorator cannot be chained with itself
    """

    def decorator(cls):
        if not attr_names:
            return cls

        class _DillIgnoreDiscriminator:
            pass

        # raise error if attribute is used after being picked+unpickled
        def __getattribute__(self, attr):
            original = object.__getattribute__(self, attr)
            if attr in attr_names and isinstance(
                original, _DillIgnoreDiscriminator
            ):
                raise RuntimeError(
                    f"Tried to access member '{attr}' which is lost during "
                    + "pickling. Renew manually before using it."
                )
            return original

        # remove given attributes from state to avoid pickling-errors
        def __getstate__(self):
            state = self.__dict__.copy()
            for attr in attr_names:
                del state[attr]
            return state

        # replace removed attributes by discriminator in state
        def __setstate__(self, state):
            self.__dict__.update(state)
            for attr in attr_names:
                self.__dict__[attr] = _DillIgnoreDiscriminator()

        cls.__getattribute__ = __getattribute__
        cls.__getstate__ = __getstate__
        cls.__setstate__ = __setstate__

        return cls

    return decorator
