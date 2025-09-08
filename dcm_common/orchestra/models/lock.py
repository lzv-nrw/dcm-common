"""Definition of the `Lock`-model."""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class Lock:
    """Lock on job token in registry."""

    id: str
    name: str
    token: str
    expires_at: datetime

    @property
    def json(self):
        """Returns lock as JSONable."""
        return {
            "id": self.id,
            "name": self.name,
            "token": self.token,
            "expiresAt": self.expires_at.isoformat(),
        }

    @classmethod
    def from_json(cls, kwargs) -> "Lock":
        """Returns instance created from given JSON."""
        return cls(
            id=kwargs["id"],
            name=kwargs["name"],
            token=kwargs["token"],
            expires_at=datetime.fromisoformat(kwargs["expiresAt"]),
        )
