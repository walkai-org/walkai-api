from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(slots=True)
class Invitation:
    email: str
    token_hash: str
    expires_at: datetime
    invited_by: Optional[str] = None
    used_at: Optional[datetime] = None

