"""
This module contains an interface for the definition of adapters to
different key-value store-type database implementations.
"""

from typing import Optional, Any
import abc


class KeyValueStoreAdapter(metaclass=abc.ABCMeta):
    """
    Interface for adapters to key-value store-type databases.

    # Implementation guide
    A new `KeyValueStoreAdapter`-type can inherit most requirements directly
    from this class. Below are the requirements imposed by the
    interface.

    ## Required definitions
    * `write` writes value for given key
    * `read` returns value for given key
    * `next` returns the next record as tuple of key and value
    * `delete` delete given key
    * `keys` list existing keys
    """
    @classmethod
    def __subclasshook__(cls, subclass):
        return (
            hasattr(subclass, "write")
            and hasattr(subclass, "read")
            and hasattr(subclass, "next")
            and hasattr(subclass, "delete")
            and hasattr(subclass, "keys")
            and callable(subclass.write)
            and callable(subclass.read)
            and callable(subclass.next)
            and callable(subclass.delete)
            and callable(subclass.keys)
            or NotImplemented
        )

    @abc.abstractmethod
    def write(self, key: str, value: Any) -> None:
        """
        Writes `value` for a given `key`.
        """
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method 'write'."
        )

    @abc.abstractmethod
    def push(self, value: Any) -> str:
        """
        Generate unused key and writes `value`. Returns that key.
        """
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method 'push'."
        )

    @abc.abstractmethod
    def read(self, key: str, pop: bool = False) -> Any:
        """
        Returns stored value for a given `key` or `None`. If `pop`, that
        record is removed from the store.
        """
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method 'read'."
        )

    @abc.abstractmethod
    def next(self, pop: bool = False) -> Optional[tuple[str, Any]]:
        """
        Returns next stored record as tuple of key and value or `None`.

        If `pop` is True, the returned record is deleted.
        """
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method 'next'."
        )

    @abc.abstractmethod
    def delete(self, key: str) -> None:
        """Deletes the record for `key`."""
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method 'delete'."
        )

    @abc.abstractmethod
    def keys(self) -> tuple[str]:
        """
        Returns a tuple of `key`s in the store.
        """
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method 'keys'."
        )
