"""Test suite for the xml-module."""

import pytest
import xmlschema
from dcm_common import LoggingContext as Context

from dcm_common.xml import XMLValidator


@pytest.fixture(name="minimal_xsd")
def _minimal_xsd():
    return """<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <xsd:element name="person">
    <xsd:complexType>
      <xsd:sequence>
        <xsd:element name="name" type="xsd:string"/>
        <xsd:element name="age" type="xsd:int"/>
      </xsd:sequence>
    </xsd:complexType>
  </xsd:element>
</xsd:schema>
"""


@pytest.fixture(name="minimal_xml")
def _minimal_xml():
    return """<?xml version="1.0" encoding="UTF-8"?>
<person>
  <name>John Doe</name>
  <age>30</age>
</person>
"""


def test_xmlvalidator_constructor(minimal_xsd):
    """Test the `version` argument of the `XMLValidator constructor."""
    assert isinstance(XMLValidator(minimal_xsd).schema, xmlschema.XMLSchema10)
    assert isinstance(
        XMLValidator(minimal_xsd, "1.0").schema, xmlschema.XMLSchema10
    )
    assert isinstance(
        XMLValidator(minimal_xsd, "1.1").schema, xmlschema.XMLSchema11
    )
    with pytest.raises(ValueError):
        XMLValidator(minimal_xsd, "unknown")


@pytest.mark.parametrize(
    ("mutate", "expected_result"),
    [
        (lambda x: x, True),
        (lambda x: x.replace("<name>", "<nme>"), False),
        (lambda x: x.replace("age", "height"), False),
    ],
    ids=["ok", "bad-xml", "invalid-xml"],
)
def test_is_valid(mutate, expected_result, minimal_xsd, minimal_xml):
    """Test method `is_valid` of `XMLValidator`."""

    validator = XMLValidator(minimal_xsd)
    assert validator.is_valid(mutate(minimal_xml)) == expected_result


@pytest.mark.parametrize(
    ("mutate", "expected_success", "expected_errors"),
    [
        (lambda x: x, True, 0),  # ok
        (lambda x: x.replace("<name>", "<nme>"), False, 1),  # bad-xml
        (lambda x: x.replace("age", "height"), True, 1),  # invalid-xml-single
        (
            # invalid-xml-multiple
            lambda x: x.replace("age", "height").replace(
                "</name>", "</name>\n  <sex>male</sex>"
            ),
            True,
            2,
        ),
        (
            # invalid-xml-attribute
            lambda x: x.replace(
                "<age>30</age>", """<age key="string">30</age>"""
            ),
            True,
            1,
        ),
    ],
    ids=[
        "ok",
        "bad-xml",
        "invalid-xml-single",
        "invalid-xml-multiple",
        "invalid-xml-attribute",
    ],
)
def test_validate(
    mutate, expected_success, expected_errors, minimal_xsd, minimal_xml
):
    """Test method `validate` of `XMLValidator`."""

    validator = XMLValidator(minimal_xsd)
    result = validator.validate(mutate(minimal_xml))
    assert result.success == expected_success
    if expected_success:
        assert result.valid == (expected_errors == 0)
    if expected_errors == 0:
        assert Context.ERROR not in result.log
    else:
        print(result.log.fancy())
        assert len(result.log[Context.ERROR]) == expected_errors


@pytest.mark.parametrize(
    "source",
    [
        "string",
        "file",
        "url",
    ],
)
def test_schema_sources_validate(
    source, minimal_xsd, minimal_xml, temporary_directory, run_service
):
    """
    Test method `validate` of `XMLValidator` instantiated with an xsd schema
    via different sources.
    """

    if source == "file":
        file_path = temporary_directory / "minimal.xsd"
        file_path.write_text(minimal_xsd, encoding="utf-8")
        validator = XMLValidator(file_path)
    elif source == "url":
        run_service(
            routes=[("/minimal.xsd", lambda: (minimal_xsd, 200), ["GET"])],
            port=5555,
        )
        validator = XMLValidator("http://localhost:5555/minimal.xsd")
    else:  # string
        validator = XMLValidator(minimal_xsd)

    result = validator.validate(minimal_xml)
    assert result.success
    assert result.valid
    assert Context.ERROR not in result.log
