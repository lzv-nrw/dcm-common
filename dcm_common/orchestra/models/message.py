"""Definition of the `Message`-model."""

from typing import Optional
from dataclasses import dataclass
from enum import Enum
from datetime import datetime


class Instruction(Enum):
    """Instruction-type enum."""

    ABORT = "abort"


@dataclass
class Message:
    """Record class for an orchestra-message."""

    token: str
    instruction: Instruction
    origin: str
    content: str
    received_at: datetime
    expires_at: Optional[datetime]

    @property
    def json(self):
        """Returns message as JSONable."""
        return {
            "token": self.token,
            "instruction": self.instruction.value,
            "origin": self.origin,
            "content": self.content,
            "receivedAt": self.received_at.isoformat(),
            "expiresAt": (
                None
                if self.expires_at is None
                else self.expires_at.isoformat()
            ),
        }

    @classmethod
    def from_json(cls, kwargs) -> "Message":
        """Returns instance created from given JSON."""
        return cls(
            token=kwargs["token"],
            instruction=Instruction(kwargs["instruction"]),
            origin=kwargs["origin"],
            content=kwargs["content"],
            received_at=datetime.fromisoformat(kwargs["receivedAt"]),
            expires_at=(
                datetime.fromisoformat(kwargs["expiresAt"])
                if "expiresAt" in kwargs
                else None
            ),
        )
