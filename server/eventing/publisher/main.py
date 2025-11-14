import os
import json
import logging
from pathlib import Path
from typing import List, Dict, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

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
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
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

@app.post("/publish")
async def publish_events(events: List[Dict[str, Any]]):
    """Publish events from the web UI"""
    try:
        count = 0
        for event in events:
            publish_supplier_event(event)
            count += 1
        
        return {
            "message": "Events published successfully",
            "count": count
        }
    except Exception as e:
        log.error(f"Error publishing events: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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
