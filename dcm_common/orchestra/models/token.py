"""Definition of the `Token`-model."""

from typing import Optional
from dataclasses import dataclass
from datetime import datetime

from dcm_common.models import DataModel


@dataclass
class Token(DataModel):
    """Token datamodel."""

    value: str
    expires: bool = False
    expires_at: Optional[datetime] = None

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
