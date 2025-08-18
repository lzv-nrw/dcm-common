"""
Token data-model definition
"""

from typing import Optional
from datetime import datetime, timedelta
from uuid import uuid4

from dcm_common.util import now
from .data_model import DataModel


class Token(DataModel):
    """
    Record-class that stores scheduling-related meta-information on
    `Job`s.

    Keyword arguments:
    expires -- control whether the generated `Token` expires
               (default True)
    duration -- duration until `Token` expires in seconds
                (default 3600)
    expires_at -- alternative to `duration`; datetime or ISO-formatted
                  string for expiration date
                  (default None)
    value -- token value/identifier
             (default None; automatically generate token)
    """
    value: str
    expires_at: Optional[datetime | str]
    expires: bool

    def __init__(
        self,
        expires: bool = True,
        duration: int = 3600,
        expires_at: Optional[datetime | str] = None,
        value: Optional[str] = None,
    ) -> None:
        self.value: str = str(uuid4()) if value is None else value
        self.expires: bool = expires
        if expires_at is not None:
            self.expires_at = (
                expires_at if isinstance(expires_at, datetime)
                else datetime.fromisoformat(expires_at)
            )
        else:
            if self.expires:
                self.expires_at = now() + timedelta(seconds=duration)
            else:
                self.expires_at = None

    @DataModel.serialization_handler("expires_at")
    @classmethod
    def expires_at_serialization(cls, value):
        """Performs `expires_at`-serialization."""
        if value is None:
            DataModel.skip()
        return value.isoformat()

    @DataModel.deserialization_handler("expires_at")
    @classmethod
    def expires_at_deserialization(cls, value):
        """Performs `expires_at`-deserialization."""
        if value is None:
            DataModel.skip()
        return datetime.fromisoformat(value)

    def expired(self) -> bool:
        """
        Returns `True` if the `Token` is expired, otherwise `False`.
        """

        if not self.expires:
            return False
        return now(True) > self.expires_at
