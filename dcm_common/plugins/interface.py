"""
This module contains a generic interface for the definition of a
plugin system.
"""

from typing import Optional, Callable
import abc

from dcm_common.logger import Logger
from dcm_common.models import DataModel, JSONObject
from .types import (
    Dependency,
    PythonDependency,
    _Dependencies,
    Signature,
    PluginResult,
    PluginExecutionContext,
)


class classproperty(property):  # pylint: disable=invalid-name
    """
    Can be used as a decorator for defining class-properties see
    https://stackoverflow.com/questions/1697501/staticmethod-with-property
    for reference.
    """

    def __get__(self, cls, owner):
        return classmethod(self.fget).__get__(None, owner)()


class PluginInterface(metaclass=abc.ABCMeta):
    """
    Generic plugin-interface.

    # General Notes

    The plugin system is designed to support inhomogenous call
    signatures for plugin implementations. This is achieved by
    introducing a call-signature which is required per plugin. The
    definition of a signature is based on the types `types.Signature`
    and `types.Argument`. Arguments can be primitive or complex
    (nested). This signature is used to
    * validate basic structure and types of input arguments
      (`validate`),
    * add default values to a plugin-call (`hydrate`), and
    * generate parts of the plugin-self description in JSON-format
      (`json`).

    # How to add a Plugin

    See the `demo`-module for a simple example.

    A new plugin can inherit most requirements directly from the
    `PluginInterface` metaclass (see also the `extensions`-module).
    Below are the requirements imposed by the plugin-system.

    Requirements for an implementation:
    _DISPLAY_NAME -- name for use in reports etc.
    _NAME -- plugin name identifier
    _DESCRIPTION -- brief self description of plugin properties/use
                    cases
    _SIGNATURE -- argument signature; used for
                  * validation of input args
                  * hydration of input args with default values
                  * generation of info regarding input args
                  see `_validate_more` for additional validation
    _get -- business logic of the plugin; the return type should inherit
            from `PluginResult` (if another type is used, `_RESULT_TYPE`
            should be set accordingly)

    Optional definitions:
    _RESULT_TYPE -- this attribute corresponds to the return type of the
                    `_get`-method; it is then used when an execution
                    context is created
    _CONTEXT -- this field is used to group plugins by their usage-
                context
    _DEPENDENCIES -- dependencies on other software/packages; this is
                     used to provide information on dependency versions
                     in an identify-request (used in method
                     `requirements_met`)
    _INFO -- free-form data field which can be used to communicate
             plugin-specific information in a plugin-specific format
             (`DataModel`, `JSONObject`, or `None`)
    _validate_more -- additional validation steps can be defined here
                      (e.g., mutually exclusive arguments)
    """

    # setup requirements for an object to be regarded
    # as implementing the PluginInterface
    @classmethod
    def __subclasshook__(cls, subclass):
        return (
            hasattr(subclass, "_NAME")
            and hasattr(subclass, "_DISPLAY_NAME")
            and hasattr(subclass, "_DESCRIPTION")
            and hasattr(subclass, "_SIGNATURE")
            and hasattr(subclass, "_DEPENDENCIES")
            and hasattr(subclass, "_CONTEXT")
            and hasattr(subclass, "_INFO")
            and hasattr(subclass, "get")
            and hasattr(subclass, "name")
            and hasattr(subclass, "display_name")
            and hasattr(subclass, "description")
            and hasattr(subclass, "signature")
            and hasattr(subclass, "dependencies")
            and hasattr(subclass, "info")
            and hasattr(subclass, "validate")
            and hasattr(subclass, "requirements_met")
            and callable(subclass.get)
            and callable(subclass.validate)
            and callable(subclass.requirements_met)
            or NotImplemented
        )

    _RESULT_TYPE = PluginResult
    """`PluginResult`-subtype used to create execution context."""

    # setup checks for missing implementation/definition of properties
    @property
    @abc.abstractmethod
    def _NAME(self) -> str:  # pylint: disable=invalid-name
        """Plugin's name identifier."""

        raise NotImplementedError(
            f"Class '{self.__class__.__name__}' does not define property "
            + "'_NAME'."
        )

    @property
    @abc.abstractmethod
    def _DISPLAY_NAME(self) -> str:  # pylint: disable=invalid-name
        """Plugin's display name."""

        raise NotImplementedError(
            f"Class '{self.__class__.__name__}' does not define property "
            + "'_DISPLAY_NAME'."
        )

    @property
    @abc.abstractmethod
    def _DESCRIPTION(self) -> str:  # pylint: disable=invalid-name
        """Plugin's self-description."""

        raise NotImplementedError(
            f"Class '{self.__class__.__name__}' does not define property "
            + "'_DESCRIPTION'."
        )

    _CONTEXT: Optional[str] = None
    """Plugin's context."""

    @property
    @abc.abstractmethod
    def _SIGNATURE(self) -> Signature:  # pylint: disable=invalid-name
        """Plugin's call-signature."""

        raise NotImplementedError(
            f"Class '{self.__class__.__name__}' does not define property "
            + "'_SIGNATURE'."
        )

    _DEPENDENCIES: Optional[list[Dependency | PythonDependency]] = None
    """Plugin dependencies."""

    _INFO: Optional[DataModel | JSONObject] = None
    """Additional information for plugin."""

    @abc.abstractmethod
    def _get(
        self, context: PluginExecutionContext, /, **kwargs
    ) -> PluginResult:
        """
        Run plugin-logic and return `PluginResult`.

        Positional arguments:
        context -- additional context for execution (see also method
                   `create_context`)

        Keyword arguments:
        kwargs -- for expected keyword arguments see property
                  `signature`
        """
        raise NotImplementedError(
            f"Class '{self.__class__.__name__}' does not define method '_get'."
        )

    def get(
        self, context: Optional[PluginExecutionContext], /, **kwargs
    ) -> PluginResult:
        """
        Hydrate args, run plugin-logic, and return `PluginResult`.

        Positional arguments:
        context -- additional context for execution (see also method
                   `create_context`)

        Keyword arguments:
        kwargs -- for expected keyword arguments see property
                  `signature`
        """
        return self._get(
            context or self.create_context(), **self.hydrate(kwargs)
        )

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    # define get methods for each property
    # pylint: disable=no-self-argument
    @classproperty
    def name(cls) -> str:
        """Returns plugin's name identifier."""
        return cls._NAME

    @classproperty
    def display_name(cls) -> str:
        """Returns plugin's display name."""
        return cls._DISPLAY_NAME

    @classproperty
    def description(cls) -> str:
        """Returns plugin's self-description."""
        return cls._DESCRIPTION

    @classproperty
    def context(cls) -> str:
        """Returns plugin's context."""
        return cls._CONTEXT

    @classproperty
    def signature(cls) -> Signature:
        """Returns plugin's call-signature."""
        return cls._SIGNATURE

    @classproperty
    def dependencies(cls) -> _Dependencies:
        """
        Returns plugin's dependencies as mapping of name and version.
        """
        return _Dependencies(cls._DEPENDENCIES or [])

    @classproperty
    def info(cls) -> Optional[JSONObject]:
        """Returns additional plugin information."""
        return (
            cls._INFO.json if isinstance(cls._INFO, DataModel) else cls._INFO
        )

    @classproperty
    def json(cls) -> JSONObject:
        """Returns dictionary that can be jsonified."""
        json = {
            "name": cls.name,
            "description": cls.description,
            "signature": cls.signature.json,
        }
        if cls._DEPENDENCIES is not None:
            json["dependencies"] = cls.dependencies.json
        if cls._CONTEXT is not None:
            json["context"] = cls.context
        if cls._INFO is not None:
            json["info"] = cls.info
        return json

    def create_context(
        self,
        set_progress: Optional[Callable[[str], None]] = None,
        push: Optional[Callable[[], None]] = None,
    ) -> PluginExecutionContext:
        """
        Returns plugin-execution context.

        Keyword arguments:
        set_progress -- callback to set a verbose status update
                        (default None)
        push -- function that is called by `plugin.get` after progress
                has been made
                (default None)
        """
        kwargs = {
            "result": self._RESULT_TYPE(
                log=Logger(default_origin=self.display_name)
            )
        }
        if set_progress:
            kwargs["_set_progress"] = set_progress
        if push:
            kwargs["_push"] = push
        return PluginExecutionContext(**kwargs)

    def hydrate(self, kwargs: JSONObject) -> JSONObject:
        """
        Returns a 'hydrated' dictionary of keyword arguments (based on
        the plugin's `signature`).

        Keyword arguments:
        kwargs -- dict of keyword arguments where default-values are
                  added to
        """

        return self._SIGNATURE.hydrate(kwargs)  # type: ignore[return-value]

    # pylint: disable=unused-argument
    @classmethod
    def _validate_more(cls, kwargs) -> tuple[bool, str]:
        """
        Returns tuple of boolean for validity and string-reasoning.

        Additional validation of arguments which is not captured by a
        `Signature`.
        """

        return True, ""

    @classmethod
    def validate(cls, kwargs) -> tuple[bool, str]:
        """
        Returns tuple of boolean for validity and string-reasoning
        (based on the plugin's `signature`).

        Keyword arguments:
        kwargs -- the kwargs to be validated
        """

        response = cls._SIGNATURE.validate(kwargs)  # type: ignore[attr-defined]
        if not response[0]:
            return response

        return cls._validate_more(kwargs)

    @classmethod
    def requirements_met(cls) -> tuple[bool, str]:
        """
        Returns a tuple containing a boolean (`True` if requirements are
        met) and a message.
        """
        return True, "ok"
