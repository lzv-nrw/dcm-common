"""Test suite for logger-module."""

from datetime import datetime, timedelta
import re

import pytest

from dcm_common.models.data_model import get_model_serialization_test
from dcm_common import LoggingContext as Context, Logger, LogMessage


@pytest.fixture(name="contexts")
def _contexts():
    return [
        Context.INFO,
        Context.WARNING,
        Context.ERROR
    ]


@pytest.fixture(name="some_logger")
def init_logger():
    return Logger(default_origin="Some service")


def test_logger_init(some_logger):
    assert isinstance(some_logger.report, dict)
    assert some_logger.default_origin == "Some service"


def test_logger_log(some_logger, contexts):
    """Test method `log` of `Logger` for basic example."""
    some_logger.log(contexts[0], body="Example.")

    assert contexts[0] in some_logger
    assert len(some_logger[contexts[0]]) == 1
    assert some_logger[contexts[0]][0].origin == "Some service"
    assert some_logger[contexts[0]][0].body == "Example."

    some_logger.log(contexts[1], body="Example.", origin="Another service")
    assert some_logger[contexts[1]][0].origin == "Another service"


def test_logger_log_from_logmessage(some_logger, contexts):
    """Test method `log` of `Logger` by logging `LogMessage`."""
    msg = LogMessage("Example2.", "Some other service")
    some_logger.log(contexts[0], msg)

    assert contexts[0] in some_logger
    assert len(some_logger[contexts[0]]) == 1
    assert msg in some_logger[contexts[0]]
    assert some_logger[contexts[0]][0].origin == "Some other service"
    assert some_logger[contexts[0]][0].body == "Example2."


def test_logger_log_multiple(some_logger, contexts):
    """Test method `log` of `Logger` by various items."""
    msg1 = LogMessage("Example.", "Some other service")
    msg2 = LogMessage("Example2.", "Some other service")
    some_logger.log(
        contexts[0],
        msg1, msg2,
        body=["Example3.", "Example4."],
        origin="Yet another service"
    )

    assert contexts[0] in some_logger
    assert len(some_logger[contexts[0]]) == 4
    assert msg1 in some_logger[contexts[0]]
    assert msg2 in some_logger[contexts[0]]


def test_logger_json(contexts):
    """Test `json`-property of `Logger`."""
    # prepare simple examples
    logger0 = Logger()
    logger1 = Logger()
    logger1.log(contexts[0], body="Example1", origin="Service1")
    logger1.log(contexts[0], body="Example2", origin="Service2")

    # run tests
    get_model_serialization_test(
        Logger, instances=(
            logger0, logger1,
        )
    )()

    logger2 = Logger(default_origin="Service0")
    logger2.log(contexts[0], body="Example1")
    logger3 = Logger(fmt="{origin}: {body}")
    logger3.log(contexts[0], body="Example1", origin="Service1")
    # test properties that are not included in the serialization
    for logger in (logger2, logger3,):
        with pytest.raises(AssertionError):
            get_model_serialization_test(
                Logger, instances=(logger,)
            )()
        assert logger.json == Logger.from_json(logger.json).json


def test_logger_no_origin(contexts):
    """Test exception-behavior of `Logger` when missing `origin`."""
    some_logger = Logger()

    some_origin = "Service1"
    some_logger.log(contexts[0], body="Example1.", origin=some_origin)
    assert some_logger[contexts[0]][0].origin == some_origin

    with pytest.raises(TypeError):
        some_logger.log(contexts[0], body="Example2.")


def test_logger_mapping(some_logger, contexts):
    some_logger.log(contexts[0], body="Example.")

    assert contexts[0] in some_logger
    assert contexts[1] not in some_logger
    assert len(some_logger[contexts[0]]) == 1
    assert isinstance(some_logger[contexts[0]][0], LogMessage)

    some_logger.log(contexts[1], body="Example2.")
    assert len(some_logger) == 2
    assert len(some_logger[contexts[1]]) == 1
    assert isinstance(some_logger[contexts[1]][0], LogMessage)


def test_logger___str__(some_logger, contexts):
    some_logger.log(contexts[0], body="Example.")
    some_logger.log(contexts[1], body="Example2.")

    logger_str = str(some_logger).split("\n")
    assert logger_str[0] == contexts[0].value
    assert logger_str[1].endswith("Example.")
    assert logger_str[2] == contexts[1].value
    assert logger_str[3].endswith("Example2.")


def test_logger___str___empty(some_logger):
    assert str(some_logger) == ""


def test_logger___bool__(contexts):
    some_logger = Logger(default_origin="Service1")
    another_logger = Logger(default_origin="Service2")

    some_logger.log(contexts[0], body="Example.")

    assert some_logger and not another_logger


def test_logger_pick(some_logger, contexts):
    """Test method `pick` of `Logger`."""

    msg1 = LogMessage("Example.")
    msg2 = LogMessage("Example2.")
    msg3 = LogMessage("Example3.")
    some_logger.log(contexts[0], msg1)
    some_logger.log(contexts[1], msg2)
    some_logger.log(contexts[2], msg3)

    # basic
    another_logger = some_logger.pick(
        contexts[0]
    )
    assert another_logger.default_origin is None
    assert contexts[0] in another_logger
    assert msg1 in another_logger[contexts[0]]
    assert len(another_logger) == 1

    # multiple
    another_logger = some_logger.pick(
        contexts[0], contexts[1]
    )
    assert len(another_logger) == 2
    assert msg1 in another_logger[contexts[0]]
    assert msg2 in another_logger[contexts[1]]

    # complement
    another_logger = some_logger.pick(
        contexts[1],
        complement=True
    )
    assert len(another_logger) == 2
    assert msg1 in another_logger[contexts[0]]
    assert msg3 in another_logger[contexts[2]]

    # new default origin
    another_logger = some_logger.pick(
        default_origin="New Logger"
    )
    assert another_logger.default_origin == "New Logger"


def test_logger_merge(contexts):
    """Test merge-method"""

    logger_src = Logger(default_origin="Service1")
    logger_target = Logger(default_origin="Service2")

    # prepare source-logger
    logger_src.log(contexts[0], body="Example.")
    logger_src.log(contexts[1], body="Example 2.")

    # merge report
    logger_target.merge(logger_src)

    # check if copied successfully
    assert str(logger_target) == str(logger_src)


def test_logger_merge_selective(contexts):
    """Test merge-method with selected contexts"""

    logger_src = Logger(default_origin="Service1")
    logger_target = Logger(default_origin="Service2")

    # prepare source-logger
    logger_src.log(contexts[0], body="Example.")
    logger_src.log(contexts[1], body="Example 2.")

    # copy report
    logger_target.merge(logger_src, [contexts[0]])

    # check whether copied category1
    assert \
        str(logger_target[contexts[0]]) == str(logger_src[contexts[0]])
    # check whether copied nothing else
    assert str(logger_target) != str(logger_src)


def test_logger_octopus(contexts):
    """Test octopus-method"""
    logger0 = Logger(default_origin="Service1")
    logger1 = Logger(default_origin="Service2")
    logger0.log(contexts[0], body="Example1.")
    logger1.log(contexts[1], body="Example2.")
    logger2 = Logger.octopus(logger0, logger1)

    assert len(logger2[contexts[0]]) == 1
    assert len(logger2[contexts[1]]) == 1
    assert "Service1" in str(logger2)
    assert "Service2" in str(logger2)
    assert "Example1" in str(logger2)
    assert "Example2" in str(logger2)


def test_logger_octopus_empty():
    """Test octopus-method"""
    logger0 = Logger.octopus()
    assert isinstance(logger0, Logger)


def test_logger_octopus_origin(contexts):
    """Test octopus-method"""
    logger0 = Logger.octopus(default_origin="Service1")
    logger0.log(contexts[0], body="test")
    assert "Service1" in str(logger0)


def test_Logger_fancy(some_logger, contexts):
    """Test method `fancy` of `Logger`."""

    some_logger.log(contexts[0], body="Example.")

    assert contexts[0].fancy not in str(some_logger)
    assert contexts[0].fancy in some_logger.fancy()


@pytest.mark.parametrize(
    "sort_by_reverse",
    [True, False],
    ids=["reverse", "non-reverse"],
)
@pytest.mark.parametrize(
    "sort_by",
    ["datetime", None],
    ids=["sorted", "unsorted"],
)
def test_Logger_fancy_sorted_by_date(
    sort_by, sort_by_reverse, some_logger, contexts
):
    """
    Test method `fancy` of `Logger` with setting `sort_by` and
    `sort_by_reverse`.
    """

    msg_old = LogMessage(
        "msg 1",
        "Service 1",
        datetime=datetime.now() + timedelta(days=-1)
    )
    msg_new = LogMessage(
        "msg 2", "Service 2", datetime=datetime.now()
    )

    some_logger.log(contexts[0], msg_new)
    some_logger.log(contexts[0], msg_old)

    assert len(re.findall(
        fr".*({msg_old.body}).*({msg_new.body}).*",
        some_logger.fancy(
            sort_by=sort_by, sort_by_reverse=sort_by_reverse
        ).replace("\n", "")
    )) == (1 if sort_by is not None and not sort_by_reverse else 0)


@pytest.mark.parametrize(
    "sort_by",
    ["origin", None],
    ids=["sorted", "unsorted"],
)
def test_Logger_fancy_sorted_by_origin(
    sort_by, some_logger, contexts
):
    """
    Test method `fancy` of `Logger` with setting `sort_by`.
    """

    msg_a = LogMessage("msg 1", "Service-A")
    msg_z = LogMessage("msg 2", "Service-Z")

    some_logger.log(contexts[0], msg_z)
    some_logger.log(contexts[0], msg_a)

    assert len(re.findall(
        fr".*({msg_a.body}).*({msg_z.body}).*",
        some_logger.fancy(sort_by=sort_by).replace("\n", "")
    )) == (1 if sort_by is not None else 0)


def test_Logger_fancy_flatten(some_logger, contexts):
    """
    Test method `fancy` of `Logger` with setting `flatten`.
    """

    msg_a = LogMessage("msg 1", "Service-A")
    msg_z = LogMessage("msg 2", "Service-Z")

    some_logger.log(contexts[0], msg_z)
    some_logger.log(contexts[1], msg_a)
    some_logger.log(contexts[0], msg_a)

    assert len(some_logger.fancy(flatten=False).split("\n")) == 5

    flattened = some_logger.fancy(flatten=True)
    assert len(flattened.split("\n")) == 3
    assert len(re.findall(
        fr".*{contexts[0].value}.*({msg_z.body})"
        + fr".*{contexts[0].value}.*({msg_a.body})"
        + fr".*{contexts[1].value}.*({msg_a.body}).*",
        flattened.replace("\n", "")
    )) == 1


def test_Logger_fancy_flatten_with_sorted_by_date(some_logger, contexts):
    """
    Test method `fancy` of `Logger` with settings `sort_by` and
    `flatten`.
    """

    msg_old = LogMessage(
        "msg 1", "Service 1", datetime=datetime.now() + timedelta(days=-1)
    )
    msg_current = LogMessage("msg 2", "Service 2", datetime=datetime.now())
    msg_future = LogMessage(
        "msg 3", "Service 3", datetime=datetime.now() + timedelta(days=1)
    )

    some_logger.log(contexts[0], msg_future)
    some_logger.log(contexts[0], msg_old)
    some_logger.log(contexts[1], msg_current)

    assert len(re.findall(
        fr".*{contexts[0].value}.*({msg_old.body})"
        + fr".*{contexts[1].value}.*({msg_current.body})"
        + fr".*{contexts[0].value}.*({msg_future.body}).*",
        some_logger.fancy(sort_by="datetime", flatten=True).replace("\n", "")
    )) == 1
    assert len(re.findall(
        fr".*({msg_old.body}).*({msg_current.body}).*({msg_future.body}).*",
        some_logger.fancy(sort_by="datetime", flatten=False).replace("\n", "")
    )) == 0


def test_Logger_fancy_from_json(contexts):
    """Test method `fancy` of `Logger` that has been created from json."""

    # prepare logger for serialization
    some_logger = Logger(default_origin="Service1")
    some_logger.log(
        contexts[0],
        body="Message1"
    )

    # make copy from serialized logger
    some_other_logger = Logger(json=some_logger.json)

    assert some_other_logger.fancy() == some_logger.fancy()


def test_LoggingContext_fancy():
    """Test method `fancy` for member of `LoggingContext`."""

    fancy_key = Context.ERROR.fancy
    assert Context.ERROR.value in fancy_key
    assert Context.ERROR.value != fancy_key


test_log_message_json = get_model_serialization_test(
    LogMessage, (
        ((), {"body": "a", "origin": "b"}),
        ((), {"datetime": datetime.now(), "body": "a", "origin": "b"}),
    )
)


def test_LogMessage_unpacking(contexts):
    """Test unpacking `LogMessage` as mapping."""

    body = "Job accepted."
    origin = "Some container"
    msg = LogMessage(body=body, origin=origin)

    logger1 = Logger()
    logger1.log(
        contexts[0],
        **msg
    )
    logger2 = Logger()
    logger2.log(
        contexts[0],
        body=body, origin=origin
    )

    assert str(logger1) == str(logger2)


def test_LogMessage_format():
    """Test formatting `LogMessage` from template."""

    template = LogMessage(body="{kwarg} and {}", origin="")

    result = template.format("positional", kwarg="keyword")

    assert isinstance(result, LogMessage)
    assert result["origin"] == ""
    assert result["body"] == "keyword and positional"


def test_LogMessage_format_origin():
    """Test formatting `LogMessage` from template; origin override."""

    template = LogMessage(body="", origin="")

    result = template.format(origin="Service1")

    assert result["origin"] == "Service1"


def test_LogMessage_claim():
    """Test method `claim` of model `LogMessage`."""

    msg = LogMessage(body="Job accepted.", origin="Some container")
    msg.claim("Some other container")

    assert msg.origin == "Some other container"
