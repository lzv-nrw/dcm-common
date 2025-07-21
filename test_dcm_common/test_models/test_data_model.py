"""DataModel-tests."""

from typing import Any, Optional
from dataclasses import dataclass, field

import pytest

from dcm_common.models import JSONable, JSONObject, DataModel
from dcm_common.models.data_model import get_model_serialization_test

# pylint: disable=missing-class-docstring, missing-function-docstring


def test_json_minimal():
    """Test property `json` of class `DataModel`."""
    class Model(DataModel):
        def __init__(self, p1: str, p2: int):
            self.p1 = p1
            self.p2 = p2

    assert Model("a", 1).json == {"p1": "a", "p2": 1}


def test_json_minimal_dataclass():
    """Test property `json` of class `DataModel`."""
    @dataclass
    class Model(DataModel):
        p1: str
        p2: int

    assert Model("a", 1).json == {"p1": "a", "p2": 1}


def test_json_private_attributes():
    """
    Test property `json` of class `DataModel` for private attributes.
    """
    class Model(DataModel):
        def __init__(self, p1):
            self.p1 = p1
            self._p2 = p1

    assert Model("a").json == {"p1": "a"}


def test_json_private_attributes_dataclass():
    """
    Test property `json` of class `DataModel` for private attributes.
    """
    @dataclass
    class Model(DataModel):
        p1: str
        _p2: int

    assert Model("a", 1).json == {"p1": "a"}


def test_json_missing_attributes():
    """Test property `json` of class `DataModel`."""
    class Model(DataModel):
        def __init__(self, p1: str, p2: int):
            self.p1 = p1
            self.p2 = p2

    assert Model("a", 1).json == {"p1": "a", "p2": 1}


def test_json_inheritance():
    """Test property `json` of class `DataModel` with inheritance."""
    class BaseModel(DataModel):
        def __init__(self, p1: str, p2: int):
            self.p1 = p1
            self.p2 = p2

    class Model(BaseModel):
        def __init__(self, p1: str, p2: int, p3: bool):
            super().__init__(p1, p2)
            self.p3 = p3

    assert Model("a", 1, True).json == {"p1": "a", "p2": 1, "p3": True}


def test_json_bad_handler():
    """
    Test error behavior for bad definition of a (de-)serialization
    handler for a `DataModel`-class.
    """
    with pytest.raises(TypeError):
        @dataclass
        class Model(DataModel):
            p: int

            @classmethod
            @DataModel.serialization_handler("p")
            def p_handler(cls, value):
                return value


def test_json_override():
    """
    Test overriding serialization of field in property `json` of class
    `DataModel`.
    """
    @dataclass
    class Model1(DataModel):
        """Base - None will be omitted in serialization"""
        p: Optional[int]

    @dataclass
    class Model2(DataModel):
        """Override default behavior for None via handler"""
        p: Optional[int]

        @DataModel.serialization_handler("p")
        @classmethod
        def p_handler(cls, value):
            return value

    @dataclass
    class Model3(DataModel):
        """Change serialized name via handler"""
        p: int

        @DataModel.serialization_handler("p", "_p")
        @classmethod
        def p_handler(cls, value):
            return "a"

    @dataclass
    class Model4(DataModel):
        """Override default behavior for private attribute via handler"""
        _p: int

        @DataModel.serialization_handler("_p", "p")
        @classmethod
        def p_handler(cls, value):
            return value

    assert not Model1(None).json
    assert Model2(None).json == {"p": None}
    assert Model3(3).json == {"_p": "a"}
    assert Model4(3).json == {"p": 3}


def test_json_error():
    """
    Test serialization via property `json` with non-serializable field
    of class `DataModel`.
    """
    class Model(DataModel):
        def __init__(self, p):
            self.p = p

    with pytest.raises(ValueError):
        _ = Model(lambda: None).json


def test_json_error_patch():
    """
    Test serialization via property `json` with non-serializable field
    of class `DataModel`; patched via handler.
    """
    class Model(DataModel):
        def __init__(self, p):
            self.p = p

        @DataModel.serialization_handler("p")
        @classmethod
        def handler(cls, value):
            return value()

    assert Model(lambda: None).json == {"p": None}


def test_json_nested():
    """
    Test property `json` of class `DataModel` with nested structure.
    """
    class InnerModel(DataModel):
        def __init__(self, q: int):
            self.q = q

    class Model(DataModel):
        def __init__(self, p1: str, p2: InnerModel):
            self.p1 = p1
            self.p2 = p2

    assert Model("a", InnerModel(1)).json == {"p1": "a", "p2": {"q": 1}}


def test_json_nested_handler_conflicting_names():
    """
    Test property `json` of class `DataModel` with nested structure and
    name conflict with Mapping and outer model-handler.
    """
    @dataclass
    class Model(DataModel):
        p: dict[str, int]

        @DataModel.serialization_handler("q")
        @classmethod
        def p_serialization_handler(cls, value):
            if isinstance(value, int):
                return None
            return value.json

    assert Model({"q": -1}).json == {"p": {"q": -1}}
    assert Model({"q": 1}).json == {"p": {"q": 1}}


def test_json_nested_dataclass():
    """
    Test property `json` of class `DataModel` with nested structure and
    dataclasses.
    """
    @dataclass
    class InnerModel(DataModel):
        q: int

    @dataclass
    class Model(DataModel):
        p1: str
        p2: InnerModel = field(default_factory=lambda: InnerModel(2))

    assert Model("a", InnerModel(1)).json == {"p1": "a", "p2": {"q": 1}}
    assert Model("a").json == {"p1": "a", "p2": {"q": 2}}


def test_json_skip_via_handler():
    """
    Test property `json` of class `DataModel` with handler and
    `DataModel.skip`-signal.
    """
    @dataclass
    class Model(DataModel):
        p: int

        @DataModel.serialization_handler("p")
        @classmethod
        def p_serialization_handler(cls, value):
            if value < 0:
                DataModel.skip()
            return value

    assert Model(-1).json == {}
    assert Model(1).json == {"p": 1}


def test_json_dictionary_with_none_value():
    """
    Test property `json` of class `DataModel` with a dictionary
    containing `None`-values.
    """
    @dataclass
    class Model(DataModel):
        p: dict

    assert Model({"a": None}).json == {"p": {"a": None}}


def test_json_dictionary_with_underscore_in_key():
    """
    Test property `json` of class `DataModel` with a dictionary
    containing keys with leading underscore.
    """
    @dataclass
    class Model(DataModel):
        p: dict

    assert Model({"_a": "a", "a": "b"}).json == {"p": {"_a": "a", "a": "b"}}


def test_json_dictionary_with_non_string_key():
    """
    Test property `json` of class `DataModel` with a dictionary
    containing non-string keys.
    """
    @dataclass
    class Model(DataModel):
        p: dict

    assert Model({None: "a", 1: "b"}).json == {"p": {None: "a", 1: "b"}}


def test_from_json_minimal():
    """Test method `from_json` of class `DataModel`."""
    class Model(DataModel):
        p1: str
        p2: int
        def __init__(self, p1, p2):
            self.p1 = p1
            self.p2 = p2

    assert Model("a", 1).json == Model.from_json(Model("a", 1).json).json


def test_from_json_minimal_dataclass():
    """Test method `from_json` of class `DataModel`."""
    @dataclass
    class Model(DataModel):
        p1: str
        p2: int

    assert Model("a", 1).json == Model.from_json(Model("a", 1).json).json


def test_from_json_bad_value():
    """Test method `from_json` of class `DataModel` for bad input."""
    @dataclass
    class Model(DataModel):
        p: str

    with pytest.raises(ValueError):
        _ = Model.from_json("a")


def test_from_json_with_default():
    """
    Test method `from_json` of class `DataModel` with default value.
    """
    class Model(DataModel):
        p: int = 5
        def __init__(self, p):
            self.p = p

    assert Model(1).json == Model.from_json(Model(1).json).json


def test_from_json_with_default_dataclass():
    """
    Test method `from_json` of class `DataModel` with default value.
    """
    @dataclass
    class Model(DataModel):
        p: int = 5

    assert Model(1).json == Model.from_json(Model(1).json).json


def test_from_json_incomplete_annotation():
    """
    Test method `from_json` of class `DataModel` for model without
    attribute annotation.
    """
    class Model(DataModel):
        p = None
        def __init__(self, p):
            self.p = p

    with pytest.raises(TypeError) as e:
        _ = Model.from_json(Model("a").json).json
    assert "Maybe class is missing proper attribute annotation?" in str(e)


def test_from_json_missing_annotation():
    """
    Test method `from_json` of class `DataModel` for model without
    attribute annotation.
    """
    class Model(DataModel):
        def __init__(self, p):
            self.p = p

    with pytest.raises(TypeError) as e:
        _ = Model.from_json(Model("a").json).json
    assert "Maybe class is missing proper attribute annotation?" in str(e)


def test_from_json_optional():
    """
    Test method `from_json` of class `DataModel` with optional
    attribute.
    """
    class Model(DataModel):
        p: Optional[str]
        def __init__(self, p):
            self.p = p

    assert Model("a").json == Model.from_json(Model("a").json).json


def test_from_json_optional_dataclass():
    """
    Test method `from_json` of class `DataModel` with optional
    attribute.
    """
    @dataclass
    class Model(DataModel):
        p: Optional[str]

    assert Model("a").json == Model.from_json(Model("a").json).json


def test_from_json_any():
    """
    Test method `from_json` of class `DataModel` with any attribute.
    """
    class Model(DataModel):
        p: Any
        def __init__(self, p):
            self.p = p

    assert Model(1).json == Model.from_json(Model(1).json).json
    assert Model("a").json == Model.from_json(Model("a").json).json
    assert Model(["a"]).json == Model.from_json(Model(["a"]).json).json


def test_from_json_any_dataclass():
    """
    Test method `from_json` of class `DataModel` with any attribute.
    """
    @dataclass
    class Model(DataModel):
        p: Any

    assert Model(1).json == Model.from_json(Model(1).json).json
    assert Model("a").json == Model.from_json(Model("a").json).json
    assert Model(["a"]).json == Model.from_json(Model(["a"]).json).json


def test_from_json_list():
    """
    Test method `from_json` of class `DataModel` with list attribute.
    """
    class Model(DataModel):
        p1: list
        p2: list[str]
        p3: list[Any]
        p4: Optional[list]
        def __init__(self, p1, p2, p3, p4):
            self.p1 = p1
            self.p2 = p2
            self.p3 = p3
            self.p4 = p4

    assert (
        Model(["a"], ["b"], ["c"], ["d"]).json
        == Model.from_json(Model(["a"], ["b"], ["c"], ["d"]).json).json
    )


def test_from_json_list_error():
    """
    Test method `from_json` of class `DataModel` with list attribute.
    """
    class Model(DataModel):
        p: list[list]
        def __init__(self, p):
            self.p = p

    with pytest.raises(ValueError):
        _ = Model.from_json([[]])


def test_from_json_dict():
    """
    Test method `from_json` of class `DataModel` with list attribute.
    """
    class Model(DataModel):
        p1: dict[str, Any]
        p2: dict[str, int]
        def __init__(self, p1, p2):
            self.p1 = p1
            self.p2 = p2

    assert (
        Model({"a": "b", "c": {"d": "e"}}, {"f": "g"}).json
        == Model.from_json(
            Model({"a": "b", "c": {"d": "e"}}, {"f": "g"}).json
        ).json
    )


def test_from_json_dict_error():
    """
    Test method `from_json` of class `DataModel` with list attribute.
    """
    class Model1(DataModel):
        p: dict
        def __init__(self, p):
            self.p = p
    class Model2(DataModel):
        p: dict[str, dict]
        def __init__(self, p):
            self.p = p

    with pytest.raises(ValueError):
        Model1.from_json({"p": {"a": "b"}})
    with pytest.raises(ValueError):
        Model2.from_json({"p": {"a": "b"}})


def test_from_json_nested_object_basic():
    """
    Test method `from_json` of class `DataModel` for nested models.
    """
    class InnerModel(DataModel):
        q: int
        def __init__(self, q):
            self.q = q

    class Model(DataModel):
        p: InnerModel
        def __init__(self, p: InnerModel):
            self.p = p

    assert (
        Model(InnerModel(1)).json
        == Model.from_json(Model(InnerModel(1)).json).json
    )


def test_from_json_nested_object_list():
    """
    Test method `from_json` of class `DataModel` for nested models.
    """
    class InnerModel(DataModel):
        q: int
        def __init__(self, q):
            self.q = q

    class Model(DataModel):
        p: list[InnerModel]
        def __init__(self, p):
            self.p = p

    assert (
        Model([InnerModel(1), InnerModel(2)]).json
        == Model.from_json(Model([InnerModel(1), InnerModel(2)]).json).json
    )


def test_from_json_nested_object_list_error():
    """
    Test method `from_json` of class `DataModel` for nested models.
    """
    class InnerModel(DataModel):
        q: int
        def __init__(self, q):
            self.q = q

    class Model(DataModel):
        p: list[dict[str, InnerModel]]
        def __init__(self, p):
            self.p = p

    with pytest.raises(ValueError):
        _ = Model.from_json({"p": [{"a": {"q": 0}}]})


def test_from_json_nested_object_dict():
    """
    Test method `from_json` of class `DataModel` for nested models.
    """
    class InnerModel(DataModel):
        q: int
        def __init__(self, q):
            self.q = q

    class Model(DataModel):
        p: dict[str, InnerModel]
        def __init__(self, p):
            self.p = p

    assert (
        Model({"a": InnerModel(1), "b": InnerModel(2)}).json
        == Model.from_json(
            Model({"a": InnerModel(1), "b": InnerModel(2)}).json
        ).json
    )


def test_from_json_nested_optional_datamodel():
    """
    Test method `from_json` of class `DataModel` for nested models.
    """
    @dataclass
    class InnerModel(DataModel):
        q: int

    @dataclass
    class Model(DataModel):
        p: Optional[InnerModel] = None

    assert (
        Model(InnerModel(1)).json
        == Model.from_json(
            Model(InnerModel(1)).json
        ).json
    )
    assert (
        Model().json
        == Model.from_json(
            Model().json
        ).json
    )


def test_from_json_nested_union_datamodel():
    """
    Test method `from_json` of class `DataModel` for union of models.
    """
    @dataclass
    class InnerModel1(DataModel):
        q: int

    @dataclass
    class InnerModel2(DataModel):
        pass

    @dataclass
    class Model(DataModel):
        p: Optional[InnerModel1 | InnerModel2] = None

    assert isinstance(Model.from_json({"p": {"q": 1}}).p, InnerModel1)
    assert isinstance(Model.from_json({"p": {}}).p, InnerModel2)
    assert Model.from_json({"p": None}).p is None


def test_from_json_optional_jsonobject():
    """
    Test method `from_json` of class `DataModel` for optional JSONObject.
    """
    @dataclass
    class Model(DataModel):
        p: Optional[JSONObject] = None

    assert (
        Model().json == Model.from_json(Model().json).json
    )
    assert (
        Model({"p": {}}).json == Model.from_json(Model({"p": {}}).json).json
    )


def test_from_json_nested_object_dict_error():
    """
    Test method `from_json` of class `DataModel` for nested models.
    """
    class InnerModel(DataModel):
        q: int
        def __init__(self, q):
            self.q = q

    class Model(DataModel):
        p: dict[str, list[InnerModel]]
        def __init__(self, p):
            self.p = p

    with pytest.raises(ValueError):
        _ = Model.from_json({"p": [{"a": {"q": 0}}]})


def test_from_json_nested_handler_conflicting_names():
    """
    Test method `from_json` of class `DataModel` with nested structure
    and name conflict with Mapping and outer model-handler.
    """
    @dataclass
    class Model(DataModel):
        p: dict[str, int]

        @DataModel.deserialization_handler("q")
        @classmethod
        def p_deserialization_handler(cls, value):
            if isinstance(value, int):
                return None
            return value.json

    assert Model.from_json({"p": {"q": -1}}).p == {"q": -1}
    assert Model.from_json({"p": {"q": 1}}).p == {"q": 1}


def test_from_json_inheritance():
    """Test method `from_json` of class `DataModel` with inheritance."""
    class BaseModel(DataModel):
        p1: str
        p2: int
        def __init__(self, p1, p2):
            self.p1 = p1
            self.p2 = p2

    class Model(BaseModel):
        p3: bool
        def __init__(self, p1, p2, p3):
            super().__init__(p1, p2)
            self.p3 = p3

    assert (
        Model("a", 1, True).json
        == Model.from_json(Model("a", 1, True).json).json
    )


def test_from_json_handler():
    """
    Test overriding deserialization of field in method `from_json` of
    class `DataModel`.
    """
    @dataclass
    class Model1(DataModel):
        p: int

    @dataclass
    class Model2(DataModel):
        p: int

        @DataModel.deserialization_handler("p", "_p")
        @classmethod
        def p_handler(cls, value):
            return 1

    assert Model1.from_json({"p": 0}).json == {"p": 0}
    assert Model2.from_json({"_p": 1}).json == {"p": 1}


def test_from_json_error_handler_patch():
    """
    Test overriding deserialization of field in method `from_json` of
    class `DataModel` to patch problematic property.
    """
    @dataclass
    class Model1(DataModel):
        p: list[list[int]]

    @dataclass
    class Model2(DataModel):
        p: list[list[int]]

        @DataModel.deserialization_handler("p")
        @classmethod
        def p_handler(cls, value):
            return value

    with pytest.raises(ValueError):
        _ = Model1.from_json({"p": [[0, 1], [2, 3]]})
    assert (
        Model2.from_json({"p": [[0, 1], [2, 3]]}).json
        == {"p": [[0, 1], [2, 3]]}
    )


def test_from_json_skip_via_handler():
    """
    Test skipping value in method `from_json` of class `DataModel` with
    handler and `DataModel.skip`-signal.
    """
    @dataclass
    class Model(DataModel):
        p: int = 0

        @DataModel.deserialization_handler("p")
        @classmethod
        def p_deserialization_handler(cls, value):
            if value < 0:
                DataModel.skip()
            return value

    assert Model.from_json({"p": -1}).p == 0
    assert Model.from_json({"p": 1}).p == 1


def test_from_json_omitted_field_with_handler():
    """
    Test KeyError when deserializing omitted field via handler.
    """
    @dataclass
    class Model(DataModel):
        p: Optional[int] = None

        @DataModel.deserialization_handler("p")
        @classmethod
        def p_deserialization_handler(cls, value):
            if value is None:
                DataModel.skip()
            return value

    assert Model.from_json({"p": 1}).p == 1
    assert Model.from_json({"p": None}).p is None
    assert Model.from_json({}).p is None


def test_json_from_json_attribute_renaming_datamodel_type():
    """
    Test renaming `DataModel`-type attribute via handler.
    """
    @dataclass
    class InnerModel(DataModel):
        p: int

        @DataModel.serialization_handler("p", "_p")
        @classmethod
        def p_serialization_handler(cls, value):
            return value

        @DataModel.deserialization_handler("p", "_p")
        @classmethod
        def p_deserialization_handler(cls, value):
            return value

    @dataclass
    class Model(DataModel):
        _id: InnerModel

        @DataModel.serialization_handler("_id", "id")
        @classmethod
        def id_serialization_handler(cls, value):
            return value.json

        @DataModel.deserialization_handler("_id", "id")
        @classmethod
        def id_deserialization_handler(cls, value):
            return cls.from_json(value)

        @property
        def id(self):
            return self._id

    assert (
        Model(InnerModel(1)).json ==
        Model.from_json(Model(InnerModel(1)).json).json
    )
    assert isinstance(
        Model.from_json(Model(InnerModel(1)).json).id, InnerModel
    )


def test_json_from_json_parameterized_generic():
    """Test support for `DataModel`-type with parameterized generic."""
    @dataclass
    class Model(DataModel):
        p: str | dict[str, str]

    assert (
        Model("a").json ==
        Model.from_json(Model("a").json).json
    )


def test_json_from_json_jsonable_jsonobject():
    """
    Test error-handling for `DataModel`-type with parameterized generics
    JSONable and JSONObject.
    """
    @dataclass
    class Model(DataModel):
        p1: JSONable
        p2: JSONObject

    assert (
        Model("a", {"b": 0}).json ==
        Model.from_json(Model("a", {"b": 0}).json).json
    )


def test_json_from_json_handlers_inheritance_same_name():
    """
    Test inheritance of (de-)serialization handlers of class
    `DataModel` for identical class scope+name.
    """
    class Model(DataModel):
        p: int
        def __init__(self, p):
            self.p = p

        @DataModel.serialization_handler("p")
        @classmethod
        def p_serialization_handler(cls, value):
            return value + 1

        @DataModel.deserialization_handler("p")
        @classmethod
        def p_deserialization_handler(cls, value):
            return value - 1

    BaseModel = Model

    class Model(BaseModel):
        pass

    assert BaseModel(p=1).json == {"p": 2}
    assert Model(p=1).json == {"p": 2}
    assert id(Model) != id(BaseModel)


def test_json_from_json_handlers_inheritance():
    """
    Test inheritance of (de-)serialization handlers of class
    `DataModel`.
    """

    class BaseModel(DataModel):
        p: int
        def __init__(self, p):
            self.p = p

        @DataModel.serialization_handler("p")
        @classmethod
        def p_serialization_handler(cls, value):
            return value + 1

        @DataModel.deserialization_handler("p")
        @classmethod
        def p_deserialization_handler(cls, value):
            return value - 1

    class Model(BaseModel):
        @DataModel.serialization_handler("p")
        @classmethod
        def p_serialization_handler(cls, value):
            return value + 1

    assert BaseModel(p=1).json == {"p": 2}
    assert Model(p=1).json == {"p": 2}


def test_json_from_json_handlers_inheritance_dataclass():
    """
    Test inheritance of (de-)serialization handlers of class
    `DataModel` with dataclasses.
    """
    @dataclass
    class BaseModel(DataModel):
        p: int

        @DataModel.serialization_handler("p")
        @classmethod
        def p_serialization_handler(cls, value):
            return value + 1

        @DataModel.deserialization_handler("p")
        @classmethod
        def p_deserialization_handler(cls, value):
            return value - 1

    @dataclass
    class Model(BaseModel):
        pass

    assert BaseModel(p=1).json == {"p": 2}
    assert Model(p=1).json == {"p": 2}


@dataclass
class _Model(DataModel):
    p: Optional[str] = None


test_get_model_serialization_test_param_sets = get_model_serialization_test(
    _Model,
    param_sets=(
        (("a",), {}),
        ((None,), {}),
        ((), {"p": "a"}),
        ((), {"p": None}),
        ((), {}),
    )
)


test_get_model_serialization_test_instances = get_model_serialization_test(
    _Model,
    instances=(
        _Model("a"),
        _Model(None),
        _Model(p="a"),
        _Model(p=None),
        _Model(),
    )
)
