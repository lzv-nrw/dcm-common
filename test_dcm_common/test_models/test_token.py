"""Token-data model test-module."""

from time import sleep

from dcm_common.models import Token
from dcm_common.models.data_model import get_model_serialization_test


def test_token_constructor():
    """Test constructor for `Token`-objects."""

    token = Token()

    assert hasattr(token, "value")
    assert hasattr(token, "expires")
    assert hasattr(token, "expires_at")


def test_token_value():
    """
    Test optional argument `value` in constructor for `Token`-objects.
    """

    token = Token(value="test")
    assert token.value == "test"


def test_token_expires_at():
    """
    Test optional argument `expires_at` in constructor for `Token`-objects.
    """

    token = Token()
    assert Token(expires_at=token.expires_at).expires_at == token.expires_at
    assert (
        Token(expires_at=token.expires_at.isoformat()).expires_at
        == token.expires_at
    )


def test_token_expired():
    """Test method `Token.expired`."""

    # Token expiration date needs to be at least 1 sec into the future
    sleep_duration = 1

    token = Token(expires=False)
    token2 = Token(expires=True, duration=sleep_duration)

    assert not token.expired()
    assert not token2.expired()
    sleep(sleep_duration + 0.1)
    assert not token.expired()
    assert token2.expired()


test_token_json = get_model_serialization_test(
    Token, (
        ((), {}),
        ((), {"expires": True}),
        ((), {"value": "token-value"}),
    )
)
