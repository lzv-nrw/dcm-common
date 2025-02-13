"""
Plugin-system types and data-model definitions.
"""

from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from copy import deepcopy
from importlib.metadata import version, PackageNotFoundError

from dcm_common.models import JSONable, JSONObject, DataModel
from dcm_common.models.report import Progress
from dcm_common import Logger


@dataclass
class Dependency:
    """
    Represents a dependency.

    Keyword arguments:
    name -- name of the dependency
    version -- available version
    """

    name: str
    version: str

    @property
    def json(self) -> dict[str, str]:
        """
        Returns serialized dependency as {<name>: <version>}.
        """
        return {self.name: self.version}


class PythonDependency(Dependency):
    """
    Represents a python package dependency.

    Keyword arguments:
    name -- package name
    """

    UNKNOWN_VERSION = "-not installed-"

    def __init__(self, name: str) -> None:
        try:
            _version = version(name)
        except PackageNotFoundError:
            _version = self.UNKNOWN_VERSION
        super().__init__(name, _version)


@dataclass
class _Dependencies:
    dependencies: list[Dependency | PythonDependency]

    @property
    def json(self) -> dict[str, str]:
        """
        Returns serialized dependencies as {<name>: <version>, ...}.
        """
        return {
            package.name: package.version for package in self.dependencies
        }


class JSONType:
    """Enum for JSON-data types."""

    STRING = "string"
    INTEGER = "integer"
    NUMBER = "number"
    BOOLEAN = "boolean"
    ARRAY = "array"
    OBJECT = "object"
    PRIMITIVE = [STRING, INTEGER, NUMBER, BOOLEAN]
    ANY = [*PRIMITIVE, ARRAY, OBJECT]
    MAP = {
        STRING: str,
        INTEGER: int,
        NUMBER: int | float,
        BOOLEAN: bool,
        ARRAY: list,
        OBJECT: dict,
    }
    PAM = {
        str: STRING,
        int: INTEGER,
        float: NUMBER,
        bool: BOOLEAN,
        list: ARRAY,
        dict: OBJECT,
    }


class Argument:
    """
    Class to represent a function argument.

    Required attributes:
    type_ -- type-name of the argument (given via `JSONType`)
    required -- if `True` this argument is mandatory in an import
                request
    description -- brief argument description
    example -- example value for argument (only `JSONType.PRIMITIVE` or
               `JSONType.ARRAY`)
    item_type -- (required only if type_ is `JSONType.ARRAY`) type of
                 array elements (only `JSONType.PRIMITIVE`)
    properties -- (required only if type_ is `JSONType.OBJECT`)
                  dictionary with `Arguments` as values

    Optional attribute:
    default -- default value of the argument
    additional_properties -- (used only if type_ is `JSONType.OBJECT`)
                             allow non-explicitly defined fields
                             (default False)
    """

    def __init__(
        self,
        type_: str,
        required: bool,
        description: Optional[str] = None,
        example: Optional[str | int | float | bool | list] = None,
        item_type: Optional[str] = None,
        properties: Optional[dict[str, "Argument"]] = None,
        additional_properties: bool = False,
        default: Optional[Any] = None,
    ) -> None:
        if type_ not in JSONType.ANY:
            raise ValueError(
                f"Bad type: 'type_' has to be one of '{JSONType.ANY}' instead "
                + f"of '{type_}'."
            )
        self.type_ = type_
        self.required = required
        self.description = description
        self.example = example
        # validate input: Argument is a list
        if type_ == JSONType.ARRAY:
            if item_type is None:
                raise ValueError(
                    "Missing type for list items ('type_' is "
                    + f"'{JSONType.ARRAY}' but no 'item_type' given)."
                )
            if item_type not in JSONType.PRIMITIVE:
                raise ValueError(
                    "Bad type for list items ('item_type' has to be one of "
                    + f"'{JSONType.PRIMITIVE}' not '{item_type}')."
                )
            self.item_type: Optional[str] = item_type
        else:
            self.item_type = None
        # validate input: Argument is a dict
        if type_ == JSONType.OBJECT:
            if not additional_properties and properties is None:
                raise ValueError(
                    f"Missing child-Arguments ('type_' is '{JSONType.OBJECT}' "
                    + "but no 'properties' given)."
                )
            if default is not None:
                raise ValueError(
                    "Illegal default value for a 'type_' of "
                    + f"'{JSONType.OBJECT}'."
                )
            self.properties: Optional[dict[str, Argument]] = properties
            self.additional_properties = additional_properties
        else:
            self.properties = None
        if default is not None and not isinstance(
            default, JSONType.MAP[type_]
        ):  # type: ignore[arg-type]
            raise ValueError(
                "Type of 'default' has to match 'type_': "
                + f"found '{type(default).__name__}' and "
                + f"'{JSONType.MAP[type_].__name__}'."  # type: ignore[attr-defined]
            )
        self.default = default

    @property
    def json(self) -> JSONObject:
        """Format as json"""

        json: JSONObject = {
            "type": self.type_,
            "required": self.required,
        }
        if self.description is not None:
            json["description"] = self.description
        if self.example is not None:
            json["example"] = self.example
        if self.default is not None:
            json["default"] = self.default
        if self.item_type is not None:
            json["itemType"] = self.item_type
        if self.properties is not None:
            json["properties"] = {}
            for name, p in self.properties.items():
                json["properties"][name] = p.json  # type: ignore[index, call-overload]
        if self.type_ == JSONType.OBJECT:
            json["additional_properties"] = self.additional_properties
        return json

    def hydrate(self, arg: JSONable) -> JSONable:
        """
        Returns with `arg` but has missing items replaced by their
        defaults values.

        Requires a valid call signature.
        """

        # primitive or array can have explicit defaults
        if self.type_ in JSONType.PRIMITIVE:
            # this construct is a safeguard to not overwrite existing
            # data
            if arg is None:
                return self.default
            return arg
        if self.type_ == JSONType.ARRAY:
            # make a deep copy to prevent unexpected changes in default
            # value
            if arg is None:
                return deepcopy(self.default)
            return arg

        # object without explicit properties
        if self.properties is None:
            return arg

        # mypy-hint
        assert isinstance(arg, dict)
        assert isinstance(self.properties, dict)

        # objects have defaults defined only implicitly
        result: JSONObject = {}
        for name, p in self.properties.items():
            if p.type_ == JSONType.OBJECT:
                if not p.required and name not in arg:
                    continue
                result[name] = p.hydrate(arg.get(name, {}))
            else:
                if not p.required and p.default is None and name not in arg:
                    continue
                result[name] = p.hydrate(arg.get(name, None))

        return arg | result

    def validate(self, arg: JSONable) -> tuple[bool, str]:
        """
        Validate `arg` against `self`. Returns a tuple of `bool`
        (`True` if input is valid) and `str` (reason for result).
        """

        # validate type
        if not isinstance(arg, JSONType.MAP[self.type_]):  # type: ignore[arg-type]
            return (
                False,
                f"Argument has bad type, expected '{self.type_}' but found"
                + f" '{JSONType.PAM.get(type(arg), type(arg).__name__)}'.",
            )

        # validate itemType if necessary
        if self.type_ == JSONType.ARRAY:
            # mypy-hint
            assert isinstance(arg, list)
            for i in arg:
                if not isinstance(i, JSONType.MAP[self.item_type]):  # type: ignore[index, arg-type]
                    return (
                        False,
                        "Array element has bad type, expected "
                        + f"'{self.item_type}' but found "
                        + f"'{JSONType.PAM.get(type(i), type(i).__name__)}'.",
                    )

        # validate properties if necessary
        if self.type_ == JSONType.OBJECT:
            # mypy-hint
            properties = self.properties or {}
            assert isinstance(properties, dict)
            assert isinstance(arg, dict)

            # validate required args are present
            for p in properties:
                if properties[p].required and p not in arg:
                    return (False, f"Missing required property '{p}'.")
            # validate unknown args
            if not self.additional_properties:
                for p in arg:
                    if p not in properties:
                        return (False, f"Unknown property '{p}'.")

            for name, p in arg.items():
                # skip validation of free-form entries
                if name not in properties:
                    continue
                # validate nested args
                response = properties[name].validate(p)
                if not response[0]:
                    return (False, f"Bad value in '{name}': {response[1]}")
        return True, "Argument is valid."


class Signature(Argument):
    """
    Class to represent the full argument signature of a plugin call.

    Use as
    >>> Signature(arg1=Argument(...), arg2=Argument(...), ...)
    """

    def __init__(self, **kwargs: Argument) -> None:
        super().__init__(
            type_=JSONType.OBJECT, required=True, properties=kwargs
        )


class FreeFormSignature(Argument):
    """
    Class to represent the full argument signature of a plugin call
    which accepts any data.

    Use as
    >>> FreeFormSignature(arg1=Argument(...), arg2=Argument(...), ...)
    """

    def __init__(self, **kwargs: Argument) -> None:
        super().__init__(
            type_=JSONType.OBJECT,
            required=True,
            properties=kwargs,
            additional_properties=True,
        )


@dataclass(kw_only=True)
class PluginResult(DataModel):
    """
    Generic plugin result `DataModel`.

    Define child models based on `PluginResult` by, for example, writing
     >>> @dataclass
     ... class PluginResultB(PluginResult):
     ...    data: str

    Keyword arguments:
    log -- `Logger` object
    """

    log: Logger = field(default_factory=Logger)


@dataclass(kw_only=True)
class PluginExecutionContext:
    """
    Plugin execution context.

    Keyword arguments:
    result -- `PluginResult` associated with the plugin execution
    _set_progress -- callback to set a verbose status update
    _push -- function that is called by `plugin.get` after progress has
             been made
    """
    result: PluginResult = field(default_factory=PluginResult)
    _set_progress: Callable[[str], None] = lambda value: None
    _push: Callable[[], None] = lambda: None

    # these wrappers servers only to provide proper auto-completion etc.
    def set_progress(self, value: str) -> None:
        """Updates verbose progress."""
        return self._set_progress(value)

    def push(self) -> None:
        """Updates result- and progress-data in host process."""
        return self._push()
