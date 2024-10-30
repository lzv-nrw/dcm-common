"""
This module contains an interface for the definition of key-value store-
type databases.
"""

from typing import Any
import abc


class KeyValueStore(metaclass=abc.ABCMeta):
    """
    Interface for key-value store-type databases.

    # Implementation guide
    A new `KeyValueStore`-type can inherit most requirements directly
    from this class. Below are the requirements imposed by the
    interface.

    ## Required definitions
    * `_write` specifies how pre-encoded data is to be written into the
      store
      (should accept encoded data; see also methods `_encode` &
      `_decode`)
    * `_read` specifies how data is to be retrieved from the store
      (should return undecoded data; see also methods `_encode` &
      `_decode`)
    * `_delete` specifies how key-value pairs are removed from the store
    * `keys` specifies how information regarding available keys can be
      generated

    ## Optional definitions
    * `_encode` defines how data is encoded for storing in the database
    * `_decode` defines how data that has been previously encoded with
      `_encode` can be decoded
    * `_SUPPORTED_TYPES` specifies what value types are supported
      (can be used, for example, when only certain types meet the
      en-/decoding requirements)
    """
    @classmethod
    def __subclasshook__(cls, subclass):
        return (
            hasattr(subclass, "_write")
            and hasattr(subclass, "_read")
            and hasattr(subclass, "_delete")
            and hasattr(subclass, "keys")
            and callable(subclass._write)
            and callable(subclass._read)
            and callable(subclass._delete)
            and callable(subclass.keys)
            or NotImplemented
        )

    _SUPPORTED_TYPES: tuple[type, ...] = (object, )

    def _encode(self, value: Any) -> Any:
        """
        Returns the encoded representation of `value`.
        """
        return value

    def _decode(self, value: Any) -> Any:
        """
        Returns the decoded representation of `value`.
        """
        return value

    @abc.abstractmethod
    def _write(self, key: str, value: Any) -> None:
        """
        Writes `value` for a given `key` without encoding.
        """
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method '_write'."
        )

    @abc.abstractmethod
    def _read(self, key: str) -> Any:
        """
        Returns the undecoded value for a given `key`.
        """
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method '_read'."
        )

    @abc.abstractmethod
    def _delete(self, key: str) -> None:
        """Deletes the record for `key`."""
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method '_delete'."
        )

    @abc.abstractmethod
    def keys(self) -> tuple[str]:
        """
        Returns a tuple of `key`s in the store.
        """
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method 'keys'."
        )

    def write(self, key: str, value: Any) -> None:
        """
        Write `value` for a given `key`.

        Raises `TypeError` if the type of `value` is not supported.
        """
        if not isinstance(value, self._SUPPORTED_TYPES):
            raise TypeError(
                f"{self.__class__.__name__} does not support "
                + f"{value.__class__.__name__} but only "
                + f"{', '.join(map(lambda x: x.__name__, self._SUPPORTED_TYPES))}."
            )
        return self._write(key, self._encode(value))

    def read(self, key: str) -> Any:
        """
        Retrieve the value for a given `key`.
        """
        return self._decode(self._read(key))

    def delete(self, key: str) -> None:
        """Deletes the record for `key`."""
        self._delete(key)
