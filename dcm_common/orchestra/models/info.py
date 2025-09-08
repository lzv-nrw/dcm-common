"""Definition of the `JobInfo`- and related models."""

from typing import Optional
from dataclasses import dataclass, field

from flask import request

from dcm_common.util import now
from dcm_common.models import DataModel, JSONObject
from .token import Token
from .report import Report


class JobConfig(DataModel):
    """
    Datamodel for the configuration details of a `Job`.

    Keyword arguments:
    type_ -- job type
    original_body -- original, i.e., unaltered JSON-request body
                     (default None; attempts to get json via
                     `flask.request.json`; uses `None` instead if
                     that is not successful)
    request_body -- request body filled with additional information like
                    defaults
    properties -- free field that can be used to store additional
                  information for the job-factory (e.g., specific
                  endpoint)
                  (default None)
    """

    type_: str
    original_body: JSONObject
    request_body: JSONObject
    properties: Optional[JSONObject]

    def __init__(
        self,
        type_: str,
        original_body: Optional[JSONObject],
        request_body: Optional[JSONObject],
        properties: Optional[JSONObject] = None,
    ) -> None:
        self.type_ = type_
        self.request_body = request_body
        if original_body is None:
            try:
                self.original_body = request.json
            except RuntimeError:
                self.original_body = None
        else:
            self.original_body = original_body
        self.properties = properties

    @DataModel.serialization_handler("type_", "type")
    @classmethod
    def type__serialization_handler(cls, value):
        """Handle serialization of `type_`."""
        return value

    @DataModel.deserialization_handler("type_", "type")
    @classmethod
    def type__deserialization_handler(cls, value):
        """Handle deserialization of `type_`."""
        return value

    @DataModel.serialization_handler("original_body")
    @classmethod
    def original_body_serialization_handler(cls, value):
        """Handle by always adding to json."""
        return value


@dataclass
class MetadataRecord(DataModel):
    """Datamodel for a single record in a `Job`'s metadata."""

    by: Optional[str] = None
    datetime: Optional[str] = field(
        default_factory=lambda: now(True).isoformat()
    )


class JobMetadata(DataModel):
    """Datamodel for `Job`-metadata."""

    produced: Optional[MetadataRecord]
    consumed: Optional[MetadataRecord]
    aborted: Optional[MetadataRecord]
    completed: Optional[MetadataRecord]

    def __init__(
        self,
        produced: Optional[MetadataRecord] = None,
        consumed: Optional[MetadataRecord] = None,
        aborted: Optional[MetadataRecord] = None,
        completed: Optional[MetadataRecord] = None,
    ):
        self.produced = produced
        self.consumed = consumed
        self.aborted = aborted
        self.completed = completed

    def produce(self, by: Optional[str]) -> None:
        """Sets produced-record if not already set."""
        if self.produced is not None:
            return
        self.produced = MetadataRecord(by)

    def consume(self, by: Optional[str]) -> None:
        """Sets consumed-record if not already set."""
        if self.consumed is not None:
            return
        self.consumed = MetadataRecord(by)

    def abort(self, by: Optional[str]) -> None:
        """Sets aborted-record if not already set."""
        if self.aborted is not None:
            return
        self.aborted = MetadataRecord(by)

    def complete(self, by: Optional[str]) -> None:
        """Sets completed-record if not already set."""
        if self.completed is not None:
            return
        self.completed = MetadataRecord(by)


@dataclass
class JobInfo(DataModel):
    """
    Datamodel aggregating `Job`-related information (stored in
    registry).
    """

    config: JobConfig
    token: Optional[Token] = None
    metadata: JobMetadata = field(default_factory=JobMetadata)
    report: Optional[Report | JSONObject] = None

    @DataModel.deserialization_handler("report")
    @classmethod
    def report_deserialization_handler(cls, value):
        """Manually handle 'report'; only supported as JSONObject."""
        if value is None:
            DataModel.skip()
        return value
