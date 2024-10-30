"""
Base data-model definition
"""

from typing import (
    Optional, Callable, Any, TypeVar, Union, get_type_hints, get_args,
    get_origin
)
from collections.abc import Mapping, MutableMapping

from .jsonable import (
    JSONable, JSONObject, is_jsonable_spec, is_jsonobject_spec
)


class _DataModelDeSerializationSkipSignal(Exception):
    pass


def _handler(category: str, name: str, json_name: Optional[str] = None):
    class SerializationDescriptor:
        """
        Can be used to decorate `DataModel`-methods to append to class-
        attribute (de-)serialization handlers.
        """
        def __init__(self, handler):
            if not isinstance(handler, classmethod):
                raise TypeError(
                    f"Bad (de-)serialization handler '{handler}' for "
                    + "'DataModel'-class (expected 'classmethod'). This "
                    + "is likely due to wrong decorator-order. Apply "
                    + "'@dataclass' first."
                )
            self.handler = handler

        def __set_name__(self, owner, name_):
            # https://stackoverflow.com/a/54316392
            # regular behavior
            self.handler.class_name = owner.__name__

            # modify class by appending given method to handlers
            # if handlers are inherited, make a copy first
            if not hasattr(owner, category):
                setattr(owner, category, {})
            else:
                setattr(owner, category, getattr(owner, category).copy())
            getattr(owner, category)[name] = (
                name if json_name is None else json_name,
                self.handler.__func__
            )
    return SerializationDescriptor


T = TypeVar("T", bound="DataModel")


class DataModel:
    """
    The `DataModel` class serves as a base for the definition of data
    models that support serialization to and deserialization from JSON.
    It provides default methods for both procedures in most standard-
    cases. Unsupported attribute types can be patched with a decorator-
    based handler system.

    A `DataModel` requires proper type annotations for all relevant
    attributes, e.g.,
     >>> class Model1(DataModel):
     ...     attrib: str
     ...     ...
     >>> @dataclass
     ... class Model2(DataModel):
     ...     attrib: Model1
     ...     ...

    Private attributes can be denoted with a leading underscore (will
    not be included in serialized form). The same applies to a None-
    value in an `Optional`-field
     >>> class Model(DataModel):
     ...     attrib1: Optional[str]
     ...     _attrib2: str
     >>> Model(None, "string").json
     {}

    Deserialization of deeply nested type-annotations is typically not
    supported and will cause a `ValueError` to be raised. Basic examples
    for this behavior include lists of lists, dictionaries of
    dictionaries, and similar constructs. Similarly, parameterized types
    need their full spec to work, e.g., `dict` is insufficient.

    In order to still use the default-(de-)serialzation methods,
    attribute-specific handlers can be defined. These can override
    default behavior or patch problematic types. A simple example for
    a serialization handler that enables use of nested lists is
     >>> @dataclass
     ... class Model(DataModel):
     ...     p: list[list[int]]
     ...     @DataModel.deserialization_handler("p")
     ...     @classmethod
     ...     def p_handler(cls, value):
     ...         return value
    The handler-system can also be used to change attribute names
    between the JSON and native representation (in this example combined
    with nested `DataModel`s):
     >>> @dataclass
     ... class InnerModel(DataModel):
     ...     p: int
     >>> @dataclass
     ... class Model(DataModel):
     ...     _id: InnerModel
     ...     @DataModel.serialization_handler("_id", "id")
     ...     @classmethod
     ...     def id_serialization_handler(cls, value):
     ...         return value.json
     ...     @DataModel.deserialization_handler("_id", "id")
     ...     @classmethod
     ...     def id_deserialization_handler(cls, value):
     ...         return cls.from_json(value)
     Also note that the deserialization-handlers get the attribute's
     type passed as 'cls' (instead of the parent type).

    Handlers support a basic form of conditional behavior by signal to
    skip a field from within a handler. This may be useful if an
    attribute that has to be processed by a handler should also be
    omitted under certain conditions. To use this, call the
    `DataModel.skip`-method in a handler:
     >>> @dataclass
     ... class Model(DataModel):
     ...     path: Optional[Path] = None
     ...     @DataModel.serialization_handler("path")
     ...     @classmethod
     ...     def path_serialization_handler(cls, value):
     ...         if value is None:
     ...             DataModel.skip()
     ...         return str(value)
    """

    _SERIALIZATION_ERR_MSG = (
        "{msg} while serializing attribute '{key}' in DataModel "
        + "'{model}'. Please define a custom handler to resolve this "
        + "issue."
    )
    _DESERIALIZATION_ERR_MSG = (
        "{msg} while deserializing attribute '{key}' in DataModel "
        + "'{model}'. Please define a custom handler to resolve this "
        + "issue."
    )

    _serialization_handlers = {}
    _deserialization_handlers = {}

    @classmethod
    def skip(cls):
        """
        Skip given field during a (de-)serialization.

        Only usable in a DataModel-handler.
        """
        raise _DataModelDeSerializationSkipSignal()

    @staticmethod
    def serialization_handler(
        name: str, json_name: Optional[str] = None
    ) -> Callable[[Callable[[type, Any], JSONable]], None]:
        """
        Registers decorated method to be used as a handler during
        serialization for the given attribute `name`. `json_name`
        denotes the desired name for the key in the resulting JSON.

        Use as
         >>> @DataModel.serialization_handler("_attrib", "attrib")
         ... @classmethod
         ... def attrib_handler(cls, value) -> JSONable:
         ...    ...
        """
        return _handler("_serialization_handlers", name, json_name)

    @classmethod
    def deserialization_handler(
        cls, key: str, json_key: Optional[str] = None
    ) -> Callable[[Callable[[type, JSONable], Any]], None]:
        """
        Registers decorated method to be used as a handler during
        de-serialization for the given attribute `key`. `json_key`
        denotes the expected name for the key in the resulting JSON.

        Note that the method's `cls`-parameter is the child type
        (instead of parent type).

        Use as
         >>> @DataModel.deserialization_handler("_attrib", "attrib")
         ... @classmethod
         ... def attrib_handler(cls, value) -> Any:
         ...    ...
        """
        return _handler("_deserialization_handlers", key, json_key)

    @property
    def json(self) -> JSONObject:
        """Returns dictionary that can be jsonified."""
        return self._dict_to_json(self.__dict__, True)

    @classmethod
    def _dict_to_json(
        cls, json: dict, use_handlers: bool = False, keep_none: bool = False
    ) -> JSONObject:
        """Convert dictionary values into JSONable."""
        _json = {}
        for key, value in json.items():
            if (
                use_handlers and key in cls._serialization_handlers
            ):
                try:
                    _json[
                        cls._serialization_handlers[key][0]
                    ] = cls._serialization_handlers[key][1](cls, value)
                except _DataModelDeSerializationSkipSignal:
                    pass
            elif isinstance(key, str) and key.startswith("_"):
                pass
            elif (
                isinstance(value, DataModel)
                or (
                    hasattr(value, "json") and not callable(value.json)
                )
            ):
                _json[key] = value.json
            elif isinstance(value, MutableMapping):
                _json[key] = cls._dict_to_json(value, keep_none=True)
            elif isinstance(value, (list, tuple)):
                _json[key] = cls._list_to_json(key, value)
            elif isinstance(value, (str, int, float, bool)):
                _json[key] = value
            elif value is None:
                if keep_none:
                    _json[key] = value
                else:
                    pass
            else:
                raise ValueError(
                    cls._SERIALIZATION_ERR_MSG.format(
                        msg=f"Encountered non-supported attribute '{value}' "
                        + f"(type '{type(value).__name__}')",
                        key=key,
                        model=cls.__name__
                    )
                )
        return _json

    @classmethod
    def _list_to_json(cls, key: str, json: list) -> list[JSONable]:
        """Convert list elements into JSONable."""
        _json = []
        for value in json:
            if (
                isinstance(value, DataModel)
                or (
                    hasattr(value, "json") and not callable(value.json)
                )
            ):
                _json.append(value.json)
            elif isinstance(value, MutableMapping):
                _json.append(cls._dict_to_json(value))
            elif isinstance(value, (list, tuple)):
                _json.append(cls._list_to_json(key, value))
            elif isinstance(value, (str, int, float, bool)) or value is None:
                _json.append(value)
            else:
                raise ValueError(
                    cls._SERIALIZATION_ERR_MSG.format(
                        msg=f"Encountered non-supported object '{value}' (type"
                        + f" {type(value).__name__})",
                        key=key,
                        model=cls.__name__
                    )
                )
        return _json

    @classmethod
    def from_json(cls: type[T], json: JSONObject) -> T:
        """
        Instantiate this class `T` based on the given `json`.
        """
        if not isinstance(json, MutableMapping):
            raise ValueError(
                cls._DESERIALIZATION_ERR_MSG.format(
                    msg=f"Encountered bad input value '{json}' "
                    + f"(got type '{type(json).__name__}' but "
                    + f"expected type '{MutableMapping.__name__}')",
                    key="<root>",
                    model=cls.__name__
                )
            )

        _json = {}
        for key, type_ in get_type_hints(
            cls,
            localns=locals() | {"JSONable": JSONable, "JSONObject": JSONObject}
        ).items():
            if key in cls._deserialization_handlers:
                try:
                    _json[key] = (
                        cls._deserialization_handlers[key][1](
                            type_,
                            json.get(
                                cls._deserialization_handlers[key][0],
                                None
                            )
                        )
                    )
                except _DataModelDeSerializationSkipSignal:
                    pass
                continue

            if key not in json:
                continue

            if is_jsonable_spec(type_) or is_jsonobject_spec(type_):
                _json[key] = json[key]
            elif isinstance(json[key], MutableMapping):
                _json[key] = cls._from_json_object(key, json[key])
            elif isinstance(json[key], list):
                _json[key] = cls._from_json_array(key, json[key])
            elif (
                json[key] is None
                or isinstance(json[key], (str | int | float | bool))
            ):
                _json[key] = json[key]
            else:
                if type_ != Any and not isinstance(json[key], type_):
                    raise ValueError(
                        cls._DESERIALIZATION_ERR_MSG.format(
                            msg=f"Encountered bad input value '{json[key]}' "
                            + f"(got type '{type(json[key]).__name__}' but "
                            + f"expected type '{type_.__name__}')",
                            key=key,
                            model=cls.__name__
                        )
                    )
                _json[key] = json[key]
        try:
            return cls(**_json)
        except TypeError as e:
            raise TypeError(
                f"Unable to instantiate class '{cls.__name__}' with kwargs "
                + f"'{_json}'. Maybe class is missing proper attribute "
                + "annotation?"
            ) from e

    @classmethod
    def _from_json_object(cls: type[T], key: str, json: JSONObject) -> Any:
        """Process single (object-)argument for deserialization."""
        # get type annotations with extended namespace
        type_ = get_type_hints(
            cls,
            localns=locals() | {"JSONable": JSONable, "JSONObject": JSONObject}
        )[key]

        # plain DataModel annotation
        if hasattr(type_, "from_json"):
            return type_.from_json(json)

        # collect more info
        type_origin = get_origin(type_)
        type_args = get_args(type_)

        # try-except because type_origin may not be a class (e.g. Union)
        try:
            is_mapping = issubclass(type_origin, Mapping)
        except TypeError:
            is_mapping = False
        if is_mapping:
            # handle a mapping-type
            try:
                type__ = type_args[1]
                if hasattr(type__, "from_json"):
                    return {
                        key_: type__.from_json(json_)
                        for key_, json_ in json.items()
                    }
            except (IndexError, TypeError):
                type__ = None
        elif type_origin is Union:
            # handle union-types; try deserialization for all types in
            # union that have from_json method
            for type__ in type_args:
                if is_jsonobject_spec(type__):
                    return json
                if not hasattr(type__, "from_json"):
                    continue
                try:
                    return type__.from_json(json)
                except TypeError:
                    pass
            type__ = None
        else:
            type__ = None

        # check if the type is a primitive
        if (
            type__ is not None
            and (type__ == Any or type__ in (str, int, float, bool))
        ):
            return json
        # no match > raise error
        raise ValueError(
            cls._DESERIALIZATION_ERR_MSG.format(
                msg=f"Encountered incompatible typehint '{type_}'",
                key=key,
                model=cls.__name__
            )
        )

    @classmethod
    def _from_json_array(cls: type[T], key: str, json: list[JSONable]) -> Any:
        """Process single (array-)argument for deserialization."""
        try:
            type_ = get_args(
                get_type_hints(
                    cls,
                    localns=locals() | {
                        "JSONable": JSONable, "JSONObject": JSONObject
                    }
                )[key]
            )[0]
        except IndexError:
            return json
        if type_ == Any:
            return json
        if len(get_args(type_)) > 1:
            raise ValueError(
                cls._DESERIALIZATION_ERR_MSG.format(
                    msg="Encountered a parameterized type (e.g., union) in a "
                    + "list",
                    key=key,
                    model=cls.__name__
                )
            )
        result = []
        for item in json:
            if isinstance(item, list):
                raise ValueError(
                    cls._DESERIALIZATION_ERR_MSG.format(
                        msg="Encountered a list of lists",
                        key=key,
                        model=cls.__name__
                    )
                )
            if isinstance(item, MutableMapping):
                if not hasattr(type_, "from_json"):
                    raise ValueError(
                        cls._DESERIALIZATION_ERR_MSG.format(
                            msg="Encountered an unexpected object in list",
                            key=key,
                            model=cls.__name__
                        )
                    )
                result.append(type_.from_json(item))
            else:
                result.append(item)
        return result


def get_model_serialization_test(
    model: type[DataModel],
    param_sets: Optional[tuple[tuple[tuple, Mapping], ...]] = None,
    instances: Optional[tuple[DataModel, ...]] = None,
) -> Callable:
    """
    Returns a pytest-test method for one iteration of a serialization-
    deserialization sequence.

    Use by inserting a definition like
     >>> @dataclass
     ... class _Model(DataModel):
     ...    p: Optional[str] = None
     >>> test_get_model_serialization_test = get_model_serialization_test(
     ...    _Model,
     ...    param_sets(
     ...        (("a",), {}),
     ...        ...
     ...    ),
     ...    instances=(
     ...        _Model("a"),
     ...        ...
     ...    )
     ... )
    into your pytest-compatible file.

    Keyword arguments:
    model -- DataModel-type
    param_sets -- sets of parameters for model instantiation that should
                  be tested; a tuple of tuples containing the args and
                  kwargs
                  (default None)
    instances -- tuple of initialized instances to be tested
                 (default None)
    """
    # pylint: disable=import-outside-toplevel
    import json
    import pickle

    def _format_problems(problems):
        result = f"Failed serialization-test for '{model.__name__}':"
        for problem in problems:
            result += "\n" + f"* arg#{problem[0]} ({problem[1]}): {problem[2]}"
        return result

    def _():
        problems = []
        for i, target in enumerate((param_sets or ()) + (instances or ())):
            if isinstance(target, tuple):
                _instance = model(*target[0], **target[1])
            else:
                _instance = target
            try:
                _json = _instance.json
            except ValueError as exc_info:
                problems.append(
                    (
                        i,
                        target,
                        f"Unable to serialize ({exc_info})."
                    )
                )
                continue
            try:
                json.dumps(_json)
            except TypeError as exc_info:
                problems.append(
                    (
                        i,
                        target,
                        f"Serialization incomplete ({exc_info})."
                    )
                )
                continue
            try:
                __instance = model.from_json(_json)
            except ValueError as exc_info:
                problems.append(
                    (
                        i,
                        target,
                        f"Unable to deserialize ({exc_info})."
                    )
                )
                continue
            try:
                if pickle.dumps(__instance) != pickle.dumps(_instance):
                    problems.append(
                        (
                            i,
                            target,
                            "Lost information during serialization-"
                            + "deserialization-cycle."
                        )
                    )
                    continue
            except TypeError as exc_info:
                problems.append(
                    (
                        i,
                        target,
                        f"Model incompatible with this test ({exc_info})"
                    )
                )
                continue
            if _json != __instance.json:
                problems.append(
                    (
                        i,
                        target,
                        "Lost information during serialization-"
                        + "deserialization-cycle."
                    )
                )
        assert not problems, _format_problems(problems)

    return _
