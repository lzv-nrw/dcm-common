"""Tests for the `Token`-data model."""

from datetime import datetime

from dcm_common.models.data_model import get_model_serialization_test
from dcm_common.orchestra.models import Token


test_token_json = get_model_serialization_test(
    Token,
    (
        (("a",), {}),
        (("a", True, datetime.now()), {}),
    ),
)
