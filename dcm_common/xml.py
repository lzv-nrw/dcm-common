"""
This module defines an `XMLValidator` component.
"""

from typing import TypeAlias, Optional, Any
from pathlib import Path
from dataclasses import dataclass, field
from xml.etree.ElementTree import ParseError, tostring, Element

import xmlschema
from dcm_common import LoggingContext as Context, Logger
from dcm_common.models import DataModel


XML: TypeAlias = str | Path


@dataclass(kw_only=True)
class XMLValidatorResult(DataModel):
    """
    Data model for the result of `XMLValidator.validate`-invocations.
    """

    success: Optional[bool] = None
    valid: Optional[bool] = None
    log: Logger = field(default_factory=Logger)


class XMLValidator:
    """
    An `XMLValidator` can be used to validate XML-documents based on an
    XML schema provided at instantiation.

    Keyword arguments:
    schema -- xsd schema as either string, url, or Path
    schema_name -- name identifier that is used in the log
                   (default None)
    version -- optionally request specific XML schema version ('1.0' or
               '1.1')
               (default None; uses `xmlschema`s default, 1.0)
    """

    _ERROR_FORMAT = (
        "{reason} ({name}: {message}; XPath: {xpath}; XSD: {xsd}; XML: {xml})."
    )
    _INFO_FORMAT = \
        "Validation of '{xml}' with schema '{schema}' returns {result}."

    def __init__(
        self,
        schema: XML,
        version: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> None:
        if version is None:
            self.schema, exc_info = self._load_xml_schema(
                xmlschema.XMLSchema, schema
            )
        else:
            if version == "1.0":
                self.schema, exc_info = self._load_xml_schema(
                    xmlschema.XMLSchema10, schema
                )
            elif version == "1.1":
                self.schema, exc_info = self._load_xml_schema(
                    xmlschema.XMLSchema11, schema
                )
            else:
                raise ValueError(f"Unknown XML schema version '{version}'.")
        if exc_info is not None:
            raise ValueError(
                f"Unable to load schema '{schema_name or schema}'"
            ) from exc_info
        self.name = schema_name or self._generate_name(schema)

    @staticmethod
    def _load_xml_schema(schema_type, schema):
        try:
            return schema_type(schema), None
        except Exception as exc_info:  # pylint: disable=broad-exception-caught
            return None, exc_info

    @classmethod
    def _generate_name(cls, base: Optional[Any]) -> str:
        """
        Returns a name identifier based on input `base`.
        """
        N = 50
        _base = cls._flatten_multiline(str(base))
        return str(_base)[:N] + ("" if len(str(_base)) < N else "..")

    @staticmethod
    def _flatten_multiline(string: str) -> str:
        """
        Returns new string where
        * beginning and ending whitespace is removed (per line), and
        * newlines have been removed.
        """
        return "".join(
            x.strip() for x in string.split("\n")
        )

    def is_valid(self, xml: XML) -> bool:
        """
        Returns `True` if the given xml is valid according to the
        validator's schema.

        Keyword arguments:
        xml -- xml as either string, url, or Path
        """
        try:
            return self.schema.is_valid(xml)
        except ParseError:
            return False

    def validate(
        self, xml: XML, xml_name: Optional[str] = None
    ) -> XMLValidatorResult:
        """
        Returns a `XMLValidatorResult`, whose log attribute contains
        a description of the validation result
        (according to the validator's schema).

        Keyword arguments:
        xml -- xml as either string, url, or Path
        xml_name -- name identifier that is used in the log
                    (default None)
        """
        log = Logger(default_origin="XML Schema Validator")
        _name = xml_name or self._generate_name(xml)
        try:
            for error in self.schema.iter_errors(xml):
                log.log(
                    Context.ERROR,
                    body=self._ERROR_FORMAT.format(
                        reason=error.reason,
                        name=type(error).__name__,
                        message=error.message,
                        xpath=error.path if error.path is not None else "-",
                        xsd=self._flatten_multiline(
                            error.validator.tostring("", 20)
                        ) if hasattr(error.validator, "tostring") else "-",
                        xml=(
                            self._flatten_multiline(
                                tostring(error.obj).decode()
                            ) if isinstance(error.obj, (Element))
                            else str(error.obj)
                        )
                    )
                )
        except ParseError as exc_info:
            log.log(
                Context.ERROR,
                body=f"Malformed XML, unable to continue ({exc_info})."
            )
            return XMLValidatorResult(success=False, log=log)
        log.log(
            Context.INFO,
            body=self._INFO_FORMAT.format(
                xml=_name,
                schema=self.name,
                result="INVALID" if Context.ERROR in log else "VALID",
            ),
        )
        return XMLValidatorResult(
            success=True, valid=(Context.ERROR not in log), log=log
        )
