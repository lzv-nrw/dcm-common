"""
Test module for the class `TransactionResult` of the `db`-subpackage.
"""

import pytest

from dcm_common.db import TransactionResult


@pytest.mark.parametrize(
    ("kwargs", "context"),
    pytest_args := [
        [{"success": True}, ""],
        [{"success": True, "data": None}, ""],
        [{"success": True, "data": []}, ""],
        [{"success": False}, "some context:"],
        [{"success": False, "msg": "some error message"}, "some context:"],
    ],
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_eval(kwargs, context):
    """Test `eval` method of `TransactionResult` class."""

    response = TransactionResult(**kwargs)

    if kwargs["success"]:
        assert response.eval() == (
            kwargs["data"] if "data" in kwargs else None
        )
    else:
        with pytest.raises(ValueError) as exc_info:
            response.eval(context)
        assert context in str(exc_info)
        if "msg" in kwargs and kwargs["msg"] is not None:
            assert kwargs["msg"] in str(exc_info)
