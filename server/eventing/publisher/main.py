import os
import json
import logging
from pathlib import Path
from typing import List, Dict, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import re 
from common.broker import publish
from common.events import Event

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("event.publisher")

BROKER_URL = os.getenv("BROKER_URL", "amqp://admin:admin@rabbitmq:5672/")
EXCHANGE = os.getenv("EXCHANGE", "events")
EVENTS_DIR = os.getenv("EVENTS_DIR", "/app/events")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def publish_supplier_event(event_data: Dict[str, Any]) -> None:
    """Publish a supplier initialization event"""
    evt = Event.new(
        event_type="supplier.init",
        payload=event_data
    )
    publish(BROKER_URL, "supplier.init", evt.to_dict(), exchange=EXCHANGE)
    log.info(f"Published supplier.init event: {evt.event_id}")

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
def is_email(s: str) -> bool:
    return bool(EMAIL_RE.match(s or ""))

def validate_supplier_init(msg: Dict[str, Any]) -> Dict[str, Any]:
    payload = msg.get("payload") or {}

    supplier = payload.get("supplier") or {}

    # Fix naming
    name = supplier.get("supplier_name")
    contact = supplier.get("supplier_contact")
    products = supplier.get("products")

    if not name:
        raise ValueError("supplier_name missing")

    if not is_email(contact):
        raise ValueError("supplier_contact invalid")

    if not isinstance(products, list) or not products:
        raise ValueError("products must be non-empty list")

    for i, p in enumerate(products):
        if not p.get("product_name"):
            raise ValueError(f"products[{i}].product_name missing")
        if p.get("product_quantity") is None or int(p["product_quantity"]) < 0:
            raise ValueError(f"products[{i}].product_quantity invalid")
        if p.get("product_price") is None or float(p["product_price"]) <= 0:
            raise ValueError(f"products[{i}].product_price invalid")

    return payload


@app.post("/publish")
async def publish_events(events: List[Dict[str, Any]]):
    """Publish events from the web UI"""

    count = 0
    failed_events = []

    for event in events:
        try:
            validate_supplier_init({"payload": event})
            publish_supplier_event(event)
            count += 1
        except Exception as e:
            log.error(f"Error publishing event {event.get('supplier', {}).get('supplier_name', '<unknown>')}: {e}")
            failed_events.append({
                "event": event,
                "error": str(e)
            })

    return {
        "message": "Events published (partial success if errors occurred).",
        "count": count,
        "failed_count": len(failed_events),
        "failed_events": failed_events
    }


@app.get("/health")
async def health():
    return {"status": "ok"}

def main():
    """Batch publish all events from the events directory"""
    events_dir = Path(EVENTS_DIR)
    
    if not events_dir.exists():
        log.error(f"Events directory not found: {events_dir}")
        return
    
    event_files = sorted(events_dir.glob("*.json"))
    
    if not event_files:
        log.error(f"No JSON files found in: {events_dir}")
        return
    
    log.info(f"Found {len(event_files)} event files")
    
    total_events = 0
    for event_file in event_files:
        log.info(f"Reading: {event_file.name}")
        
        try:
            with open(event_file, 'r') as f:
                event_data = json.load(f)
            
            if isinstance(event_data, list):
                events = event_data
            else:
                events = [event_data]
            
            for event in events:
                publish_supplier_event(event)
                total_events += 1
            
            log.info(f"Processed {len(events)} event(s) from {event_file.name}")
            
        except Exception as e:
            log.error(f"Error processing {event_file.name}: {e}")
    
    log.info(f"Successfully published {total_events} events")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--batch":
        main()
    else:
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=8000)
