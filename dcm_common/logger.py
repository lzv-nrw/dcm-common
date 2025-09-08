"""
This module contains the class definition for a basic logger.
"""

from typing import Optional
from enum import Enum
from datetime import datetime as datetime_

from dcm_common.util import now


class LogMessage:
    """
    Class describing a log entry.

    Keyword arguments:
    body -- message body
    origin -- origin of message creation
    datetime -- isoformat string of record's datetime
    """

    def __init__(
        self,
        body: str,
        origin: Optional[str] = None,
        datetime: Optional[datetime_] = None,
    ) -> None:
        self.body = body
        self.origin = origin
        if datetime is None:
            self.datetime = now()
        else:
            self.datetime = datetime

    def __repr__(self) -> str:
        return str(self.json)

    @property
    def json(self):
        """Convert to `JSONObject`."""
        return {
            "datetime": self.datetime.isoformat(),
            "origin": self.origin,
            "body": self.body,
        }

    @classmethod
    def from_json(cls, json) -> "LogMessage":
        """Initialize from `JSONObject`."""
        _json = json.copy()
        if _json.get("datetime") is not None:
            _json["datetime"] = datetime_.fromisoformat(_json["datetime"])
        return cls(**_json)

    def keys(self):
        return ["origin", "body"]

    def __getitem__(self, key):
        return self.json[key]

    def claim(self, origin: Optional[str]) -> None:
        """
        Claim this `LogMessage` by setting new value for origin.

        Keyword arguments:
        origin -- new origin
        """
        self.origin = origin

    def format(
        self, *args, origin: Optional[str] = None, **kwargs
    ) -> "LogMessage":
        """
        Returns new `LogMessage` with `body` formatted using `args` and
        `kwargs`.

        Positional and keyword arguments are passed into `str.format`.
        Note that the `origin` keyword is reserved (see below).

        Keyword arguments:
        origin -- optional origin-override
                  (default None)
        """
        return LogMessage(
            body=self.body.format(*args, **kwargs),
            origin=origin or self.origin,
        )


class LoggingContext(Enum):
    """
    Enum-class for different types of Logger-keys.
    """

    ERROR = "ERRORS"
    WARNING = "WARNINGS"
    INFO = "INFO"
    EVENT = "EVENTS"
    NETWORK = "NETWORK"
    FILE_SYSTEM = "FILE_SYSTEM"
    STARTUP = "STARTUP"
    SHUTDOWN = "SHUTDOWN"
    USER = "USER"
    AUTHENTICATION = "AUTHENTICATION"
    SECURITY = "SECURITY"

    @property
    def fancy(self) -> str:
        """
        Get stringified `LoggingContext` decorated with ANSI-color.
        """

        # fancy colors
        class FancyColors:
            ERROR = "\033[31m"
            WARNING = "\033[33m"
            INFO = "\033[34m"
            EVENT = "\033[97m"
            NETWORK = "\033[35m"
            FILE_SYSTEM = "\033[95m"
            STARTUP = "\033[36m"
            SHUTDOWN = "\033[96m"
            USER = "\033[90m"
            AUTHENTICATION = "\033[93m"
            SECURITY = "\033[91m"
            RESTORE = "\033[0m"

        return (
            getattr(FancyColors, self.name, FancyColors.RESTORE)
            + self.value
            + FancyColors.RESTORE
        )


class Logger:
    """
    Objects of this class can be used to log messages by context (see
    also `LoggingContext`).

    Keyword arguments:
    default_origin -- optional default origin for logged messages
                      (default None)
    fmt -- format for message stringification; can contain any keys
           present in LogMessage.json
           (default None; corresponds to "[{datetime}] {origin}: {body}")
    json -- serialized `Logger` to use for initialization
            (default None)
    """

    def __init__(
        self,
        default_origin: Optional[str] = None,
        fmt: Optional[str] = None,
        json: Optional[dict[str, list[dict[str, Optional[str]]]]] = None,
    ) -> None:
        self.report: dict[LoggingContext, list[LogMessage]] = {}
        self._origin = default_origin
        self._fmt = fmt or "[{datetime}] {origin}: {body}"
        if json is not None:
            for context, msgs in json.items():
                _context = getattr(LoggingContext, context)
                self.log(
                    _context, *[LogMessage.from_json(msg) for msg in msgs]
                )

    @property
    def default_origin(self) -> Optional[str]:
        """Returns `Logger`'s default-origin."""
        return self._origin

    def set_default_origin(self, origin: str) -> None:
        """Set `Logger`'s default-origin."""
        self._origin = origin

    @property
    def json(self) -> dict[str, list[dict[str, Optional[str]]]]:
        """
        Format as json

        `LogMessages` within a `Logger` necessarily have an origin
        (hence the given return signature)
        """
        return {k.name: [m.json for m in v] for k, v in self.report.items()}

    @classmethod
    def from_json(cls, json) -> "Logger":
        """Initialize from `JSONObject`."""
        return cls(json=json)

    def log(
        self,
        context: LoggingContext,
        *args: LogMessage,
        body: Optional[str | list[str]] = None,
        origin: Optional[str] = None,
    ) -> None:
        """
        Add message(s) to log.

        This method can be called with either
        * args (a reference to a `LogMessage`s is logged as is) or
        * body and origin (new `LogMessage`(s) are generated).

        Keyword arguments:
        context -- log message context
        *args -- `LogMessage`(s) to be logged
        body -- message(s) to be logged
        origin -- origin of message
        """

        if context not in self.report:
            self.report[context] = []

        for msg in args:
            if not isinstance(msg, LogMessage):
                raise TypeError(
                    "Logger.log args expected type 'LogMessage' "
                    + f"but found '{type(msg).__name__}'."
                )
            self.report[context].append(msg)

        if body is not None:
            _origin = origin or self.default_origin or "unknown"

            if isinstance(body, list):
                _body = body
            else:
                _body = [body]

            for b in _body:
                self.report[context].append(LogMessage(body=b, origin=_origin))

    def pick(
        self,
        *args: LoggingContext,
        complement: bool = False,
        default_origin: Optional[str] = None,
    ) -> "Logger":
        """
        Returns a new `Logger` that contains only the given contexts or
        all but the given contexts.

        Keyword arguments:
        *args -- contexts
        complement -- if `True` the returned `Logger` contains only the
                      contexts given in *args and their complement
                      otherwise
                      (default False)
        default_origin -- argument is passed to the `Logger` constructor
        """

        _logger = Logger(default_origin=default_origin)

        if complement:
            contexts = [c for c in self.report if c not in args]
        else:
            contexts = [c for c in args if c in self.report]
        for context in contexts:
            _logger.log(context, *self.report[context])

        return _logger

    @staticmethod
    def octopus(
        *args: "Logger", default_origin: Optional[str] = None
    ) -> "Logger":
        """
        Combines all `Logger`s given in positional arguments and returns
        the result.

        Keyword arguments:
        default_origin -- argument is passed to the `Logger` constructor
        """
        logger = Logger(default_origin=default_origin)
        for a in args:
            logger.merge(a)
        return logger

    def merge(
        self, logger: "Logger", contexts: Optional[list[LoggingContext]] = None
    ) -> None:
        """
        Merge logger-report contents (report_source) into this report.

        Keyword arguments:
        logger -- `Logger` to be copied from
        contexts -- list of contexts to copy
                    (default: None; default copies entire report_source)
        """

        if contexts is None:
            _contexts = logger.keys()
        else:
            _contexts = contexts

        for context in _contexts:
            self.log(context, *logger[context])

    def fancy(
        self,
        fancy: bool = True,
        fmt: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_by_reverse: bool = False,
        flatten: bool = False,
    ):
        """
        Get stringified `Logger` where contexts are (optionally)
        decorated with ANSI-color.

        Keyword arguments:
        fancy -- if `True`, decorate contexts with ANSI-color
        fmt -- optional `LogMessage`-format override
               (default None; corresponds to `Logger`'s default)
        sort_by -- `LogMessage`-field name to sort by, e.g., 'datetime'
                   or 'origin'
                   (default None; no sorting)
        sort_by_reverse -- if `True`, reverse sorting order
                           (default False)
        flatten -- whether to flatten contexts
                   (default False)
        """
        _fmt = fmt or self._fmt
        # in order to support flattened reports that are also sorted,
        # generate a new log where all messages are dumped into a single
        # context along with a context-msg-map
        if flatten:
            _logger = Logger()
            _context_map: dict[LogMessage, LoggingContext] = {}
            for context, partial_report in self.report.items():
                for msg in partial_report:
                    # safeguard for identical messages
                    _msg = LogMessage(msg.body, msg.origin, msg.datetime)
                    _logger.log(LoggingContext.INFO, _msg)
                    _context_map[_msg] = context
        else:
            _logger = self

        lines = []
        for context, partial_report in _logger.report.items():
            if len(partial_report) == 0:
                continue
            # context-headline
            if not flatten:
                if fancy:
                    lines.append(context.fancy)
                else:
                    lines.append(context.value)
            # messages
            lines += list(
                map(
                    lambda x: (
                        (  # prepend context if flatten
                            _context_map.get(x).fancy
                            if fancy
                            else _context_map.get(x).value
                        )
                        if flatten
                        else "*"
                    )
                    + " "
                    + _fmt.format(**x.json),
                    (
                        sorted(
                            partial_report,
                            key=lambda x: getattr(x, sort_by),
                            reverse=sort_by_reverse,
                        )
                        if sort_by is not None
                        else partial_report
                    ),
                )
            )
        return "\n".join(lines)

    def __len__(self):
        return len(self.report)

    def keys(self):
        return list(self.report.keys())

    def __getitem__(self, key):
        return self.report[key]

    def __contains__(self, key):
        return key in self.report

    def __str__(self) -> str:
        return self.fancy(False)

    def __bool__(self) -> bool:
        return len(self.report) != 0
