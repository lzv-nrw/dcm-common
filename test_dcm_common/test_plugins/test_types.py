"""
Test module for the plugin-system's types.
"""

from importlib.metadata import version

import pytest

from dcm_common.plugins import JSONType, Argument, PythonDependency


@pytest.mark.parametrize(
    "arguments",
    [
        {"type_": "unknown"},
        {"type_": JSONType.STRING, "default": 0},
        {"type_": JSONType.ARRAY},
        {"type_": JSONType.ARRAY, "item_type": JSONType.OBJECT},
        {"type_": JSONType.OBJECT},
        {"type_": JSONType.OBJECT, "properties": {}, "default": {}},
    ],
    ids=[
        "bad_type",
        "bad_default_type",
        "missing_itemType",
        "bad_itemType",
        "missing_properties",
        "obj_illegal_default",
    ],
)
def test_argument_constructor(arguments):
    """Test exception-behavior for constructor of class `Argument`."""

    with pytest.raises(ValueError):
        Argument(required=False, **arguments)


@pytest.mark.parametrize(
    ("argument", "data", "expected"),
    [
        (  # primitive_ok
            Argument(JSONType.STRING, False),
            "some string",
            True,
        ),
        (Argument(JSONType.STRING, False), 0, False),  # primitive_bad
        (  # array_ok
            Argument(JSONType.ARRAY, False, item_type=JSONType.STRING),
            ["string1", "string2"],
            True,
        ),
        (  # array_bad
            Argument(JSONType.ARRAY, False, item_type=JSONType.STRING),
            [0, 1],
            False,
        ),
        (  # object_ok
            Argument(
                JSONType.OBJECT,
                False,
                properties={"p1": Argument(JSONType.STRING, False)},
            ),
            {"p1": "some string"},
            True,
        ),
        (  # object_bad
            Argument(
                JSONType.OBJECT,
                False,
                properties={"p1": Argument(JSONType.STRING, False)},
            ),
            {"p1": 0},
            False,
        ),
        (  # missing_required
            Argument(
                JSONType.OBJECT,
                False,
                properties={
                    "p1": Argument(JSONType.STRING, False),
                    "p2": Argument(JSONType.STRING, True),
                },
            ),
            {"p1": "some string"},
            False,
        ),
        (  # unknown
            Argument(
                JSONType.OBJECT,
                False,
                properties={
                    "p1": Argument(JSONType.STRING, False),
                },
            ),
            {"p2": 0},
            False,
        ),
        (  # free-form object
            Argument(JSONType.OBJECT, False, additional_properties=True),
            {"p1": "some string", "p2": "additional property"},
            True,
        ),
        (  # mixed-form object
            Argument(
                JSONType.OBJECT,
                False,
                properties={"p1": Argument(JSONType.STRING, False)},
                additional_properties=True,
            ),
            {"p1": "some string", "p2": "additional property"},
            True,
        ),
        (  # mixed-form object bad type
            Argument(
                JSONType.OBJECT,
                False,
                properties={"p1": Argument(JSONType.STRING, False)},
                additional_properties=True,
            ),
            {"p1": 0, "p2": "additional property"},
            False,
        ),
    ],
    ids=[
        "primitive_ok",
        "primitive_bad",
        "array_ok",
        "array_bad",
        "object_ok",
        "object_bad",
        "missing_required",
        "unknown",
        "free-form_object",
        "mixed-form_object",
        "mixed-form_object_bad",
    ],
)
def test_argument_validation(argument, data, expected):
    """Test `Argument`-validation."""
    result = argument.validate(data)

    print(result[1], end="")
    assert result[0] == expected


@pytest.mark.parametrize(
    ("argument", "json"),
    [
        (  # primitive
            Argument(JSONType.STRING, False),
            {"type": JSONType.STRING, "required": False},
        ),
        (  # array
            Argument(JSONType.ARRAY, False, item_type=JSONType.STRING),
            {
                "type": JSONType.ARRAY,
                "required": False,
                "itemType": JSONType.STRING,
            },
        ),
        (  # object
            Argument(
                JSONType.OBJECT,
                False,
                properties={"p1": Argument(JSONType.STRING, False)},
            ),
            {
                "type": JSONType.OBJECT,
                "required": False,
                "properties": {
                    "p1": {"type": JSONType.STRING, "required": False}
                },
                "additional_properties": False,
            },
        ),
        (  # default_primitive
            Argument(JSONType.STRING, False, default="some string"),
            {
                "type": JSONType.STRING,
                "required": False,
                "default": "some string",
            },
        ),
        (  # default_array
            Argument(
                JSONType.ARRAY,
                False,
                item_type=JSONType.INTEGER,
                default=[0, 1],
            ),
            {
                "type": JSONType.ARRAY,
                "required": False,
                "itemType": JSONType.INTEGER,
                "default": [0, 1],
            },
        ),
    ],
    ids=[
        "primitive",
        "array",
        "object",
        "default_primitive",
        "default_array",
    ],
)
def test_argument_json(argument, json):
    """Test `Argument`'s `json` property."""

    assert argument.json == json


@pytest.mark.parametrize(
    ("arg", "in_", "out"),
    [
        (
            Argument(JSONType.INTEGER, True, default=1),
            -1,
            -1,
        ),
        (
            Argument(JSONType.INTEGER, True, default=2),
            None,
            2,
        ),
        (
            Argument(JSONType.INTEGER, False),
            None,
            None,
        ),
        (
            Argument(JSONType.INTEGER, False),
            4,
            4,
        ),
        (
            Argument(JSONType.INTEGER, False, default=5),
            None,
            5,
        ),
        (
            Argument(
                JSONType.OBJECT,
                False,
                properties={"q1": Argument(JSONType.INTEGER, True)},
            ),
            None,
            None,
        ),
        (
            Argument(
                JSONType.OBJECT,
                True,
                properties={
                    "q1": Argument(JSONType.INTEGER, False, default=71)
                },
            ),
            {},
            {"q1": 71},
        ),
        (
            Argument(
                JSONType.OBJECT,
                False,
                properties={"q1": Argument(JSONType.INTEGER, False)},
            ),
            {},
            {},
        ),
        (
            Argument(JSONType.INTEGER, True, default=1),
            0,
            0,
        ),
        (
            Argument(JSONType.OBJECT, True, additional_properties=True),
            None,
            {},
        ),
        (
            Argument(JSONType.OBJECT, True, additional_properties=True),
            {"q1": 0},
            {"q1": 0},
        ),
        (  # additional properties (general)
            Argument(
                JSONType.OBJECT,
                True,
                properties={
                    "q1": Argument(JSONType.INTEGER, False, default=0)
                },
                additional_properties=True,
            ),
            {"q1": 1, "q2": 0},
            {"q1": 1, "q2": 0},
        ),
        (  # defaults + additional properties
            Argument(
                JSONType.OBJECT,
                True,
                properties={
                    "q1": Argument(JSONType.INTEGER, False, default=0)
                },
                additional_properties=True,
            ),
            {"q2": 0},
            {"q1": 0, "q2": 0},
        ),
        (  # defaults + no additional properties
            Argument(
                JSONType.OBJECT,
                True,
                properties={
                    "q1": Argument(JSONType.INTEGER, False, default=0)
                },
                additional_properties=False,
            ),
            {"q2": 0},
            {"q1": 0, "q2": 0},
        ),
        (  # nested object with default
            Argument(
                JSONType.OBJECT,
                True,
                properties={
                    "q1": Argument(
                        JSONType.OBJECT,
                        True,
                        properties={
                            "p1": Argument(JSONType.INTEGER, False, default=0)
                        },
                        additional_properties=True,
                    )
                },
            ),
            {},
            {"q1": {"p1": 0}},
        ),
        (  # nested object with defaults and additional properties
            Argument(
                JSONType.OBJECT,
                True,
                properties={
                    "q1": Argument(
                        JSONType.OBJECT,
                        True,
                        properties={
                            "p1": Argument(JSONType.INTEGER, False, default=0)
                        },
                        additional_properties=True,
                    )
                },
            ),
            {"q1": {"p2": 0}},
            {"q1": {"p1": 0, "p2": 0}},
        ),
    ],
)
def test_argument_hydrate(arg, in_, out):
    """Test `Argument`'s `hydrate`-method."""

    argument = Argument(JSONType.OBJECT, False, properties={"p": arg})
    if in_ is None:
        in__ = {}
    else:
        in__ = {"p": in_}
    if out is None:
        out_ = {}
    else:
        out_ = {"p": out}
    assert argument.hydrate(in__) == out_


def test_python_dependency_constructor():
    """Test behavior of `PythonDependency`'s constructor."""
    assert PythonDependency("pytest").version == version("pytest")
    assert (
        PythonDependency("unknown-package").version
        == PythonDependency.UNKNOWN_VERSION
    )
