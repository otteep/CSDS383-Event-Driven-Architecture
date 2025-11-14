import json
import logging
import time
import pika
from pathlib import Path
from typing import List, Dict, Any
from config import settings
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("event.publisher")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    """Setup topology when the app starts"""
    setup_topology()

def setup_topology():
    """Setup RabbitMQ topology once at startup"""
    try:
        parameters = pika.URLParameters(settings.RABBITMQ_URL)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        # Declare Dead Letter Exchanges
        channel.exchange_declare(exchange='dlx.init', exchange_type='fanout', durable=True)
        channel.exchange_declare(exchange='dlx.product', exchange_type='fanout', durable=True)
        
        # Declare Dead Letter Queues
        channel.queue_declare(queue='dlq.init.q', durable=True)
        channel.queue_declare(queue='dlq.product.q', durable=True)
        
        # Bind DLQs to their exchanges
        channel.queue_bind(queue='dlq.init.q', exchange='dlx.init')
        channel.queue_bind(queue='dlq.product.q', exchange='dlx.product')
        
        # Declare main initializing queue with DLX
        channel.queue_declare(
            queue='initializing_queue',
            durable=True,
            arguments={
                'x-dead-letter-exchange': 'dlx.init'
            }
        )
        
        # Declare product queue with DLX
        channel.queue_declare(
            queue='product_queue',
            durable=True,
            arguments={
                'x-dead-letter-exchange': 'dlx.product'
            }
        )
        
        connection.close()
        log.info("RabbitMQ topology setup complete with DLQ support")
    except Exception as e:
        log.error(f"Failed to setup topology: {e}")
        raise

class EventPublisher:
    def __init__(self):
        self.connection = None
        self.channel = None
        self._connect()
    
    def _connect(self):
        try:
            parameters = pika.URLParameters(settings.RABBITMQ_URL)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            log.info("Connected to RabbitMQ")
        except Exception as e:
            log.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    def publish_event(self, event: dict, filename: str = ""):
        """Publish a single event"""
        try:
            message = json.dumps(event)
            self.channel.basic_publish(
                exchange='',  
                routing_key='initializing_queue',
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/json'
                )
            )
            log.info(f"Published from {filename}")
        except Exception as e:
            log.error(f"Failed to publish event from {filename}: {e}")

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()

@app.post("/publish")
async def publish_events(events: List[Dict[str, Any]]):
    """Publish events to RabbitMQ"""
    try:
        publisher = EventPublisher()
        count = 0
        
        for event in events:
            publisher.publish_event(event, "api_request")
            count += 1
        
        publisher.close()
        
        return {
            "message": "Events published successfully",
            "count": count
        }
    except Exception as e:
        log.error(f"Error publishing events: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def main():
    """Read all JSON files from events directory and publish them"""
    events_dir = Path(settings.EVENTS_DIR)
    
    if not events_dir.exists():
        log.error(f"Events directory not found: {events_dir}")
        return
    
    if not events_dir.is_dir():
        log.error(f"Events path is not a directory: {events_dir}")
        return
    
    event_files = sorted(events_dir.glob("*.json"))
    
    if not event_files:
        log.error(f"No JSON files found in: {events_dir}")
        return
    
    log.info(f"Found {len(event_files)} event files in: {events_dir}")
    
    setup_topology()
    publisher = EventPublisher()
    total_events = 0
    
    try:
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
                    publisher.publish_event(event, event_file.name)
                    total_events += 1
                    time.sleep(0.05)  
                
                log.info(f"Processed {len(events)} event(s) from {event_file.name}")
                
            except json.JSONDecodeError as e:
                log.error(f"Invalid JSON in {event_file.name}: {e}")
            except Exception as e:
                log.error(f"Error reading {event_file.name}: {e}")
        
        log.info(f"Successfully published {total_events} events from {len(event_files)} files")
        
    except Exception as e:
        log.error(f"Error during publishing: {e}")
    finally:
        publisher.close()

if __name__ == "__main__":
    main()