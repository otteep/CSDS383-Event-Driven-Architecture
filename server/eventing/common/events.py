from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional
from uuid import uuid4
from datetime import datetime, timezone

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

@dataclass
class Event:
    event_type: str               # supplier.init | supplier.valid | product.init
    event_version: int
    event_id: str
    correlation_id: str
    occurred_at: str
    payload: Dict[str, Any]

    @staticmethod
    def new(event_type: str, payload: Dict[str, Any], correlation_id: Optional[str] = None, version: int = 1) -> "Event":
        return Event(
            event_type=event_type,
            event_version=version,
            event_id=str(uuid4()),
            correlation_id=correlation_id or str(uuid4()),
            occurred_at=iso_now(),
            payload=payload,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_type": self.event_type,
            "event_version": self.event_version,
            "event_id": self.event_id,
            "correlation_id": self.correlation_id,
            "occurred_at": self.occurred_at,
            "payload": self.payload,
        }
