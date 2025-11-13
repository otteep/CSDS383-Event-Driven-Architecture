import os
import json
import inspect
import pika
import logging
from pika.exceptions import ChannelClosedByBroker

log = logging.getLogger("broker")

# ---------- config helpers ----------

def _exchange_type() -> str:
    """Defaults to 'topic' so we don't fight with the topology container."""
    return os.getenv("EXCHANGE_TYPE", "topic").strip().lower() or "topic"

# ---------- low-level helpers ----------

def _conn_params(broker_url: str) -> pika.URLParameters:
    params = pika.URLParameters(broker_url)
    params.heartbeat = 30
    params.blocked_connection_timeout = 300
    return params

def _open_channel(broker_url: str):
    conn = pika.BlockingConnection(_conn_params(broker_url))
    ch = conn.channel()
    ch.basic_qos(prefetch_count=1)
    return conn, ch

# ---------- topology ----------

def ensure_topology(broker_url: str, exchange: str, queues: list[str] | None) -> None:

    if queues is None:
        queues = []

    conn = pika.BlockingConnection(_conn_params(broker_url))
    try:
        ch = conn.channel()

        # Respect EXCHANGE_TYPE to avoid 406 PRECONDITION_FAILED mismatches.
        ex_type = _exchange_type()
        ch.exchange_declare(exchange=exchange, exchange_type=ex_type, durable=True)

        # Assert queues exist (passive=True). If topology wasn't run, this will 404.
        for q in queues:
            try:
                ch.queue_declare(queue=q, passive=True)
            except ChannelClosedByBroker as e:
                # Reopen channel then raise a clearer error
                ch = conn.channel()
                raise RuntimeError(
                    f"Queue '{q}' not found. Did you run `docker compose up topology`?"
                ) from e
    finally:
        conn.close()

# ---------- publishing ----------

def publish(
    broker_url: str,
    routing_key: str,
    payload,
    *,
    exchange: str = "events",
    content_type: str = "application/json",
    persistent: bool = True,
) -> None:
    if isinstance(payload, (dict, list)):
        body = json.dumps(payload).encode("utf-8")
    elif isinstance(payload, str):
        body = payload.encode("utf-8")
    else:
        body = payload  # assume bytes

    conn, ch = _open_channel(broker_url)
    try:
        props = pika.BasicProperties(
            delivery_mode=2 if persistent else 1,
            content_type=content_type,
        )
        ch.basic_publish(exchange=exchange, routing_key=routing_key, body=body, properties=props)
        log.info("Published -> key=%s bytes=%d", routing_key, len(body))
    finally:
        conn.close()

# ---------- consumption ----------

def with_ack(handler):

    def _wrap(ch, method, props, body):
        try:
            ok = handler(ch, method, props, body)
            if ok is False:
                ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            else:
                ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            log.exception("Handler error; sending to DLQ")
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
    return _wrap


def _maybe_json(props, body):
    """Try to parse JSON if the content-type says so; otherwise return body as-is."""
    ct = getattr(props, "content_type", None)
    if isinstance(body, (bytes, bytearray)) and ct and "json" in ct:
        try:
            return json.loads(body.decode("utf-8"))
        except Exception:
            return body
    return body


def _to_callback(handler):

    try:
        sig = inspect.signature(handler)
        arity = len(sig.parameters)
    except Exception:
        # If we can't introspect, assume 4-arg to preserve previous behavior
        arity = 4

    if arity == 1:
        log.info("Wrapping a 1-arg handler(body) for consumption")
        def _cb(ch, method, props, body):
            try:
                # helpful: pass already-parsed dict when JSON
                normalized = _maybe_json(props, body)
                ok = handler(normalized)
                if ok is False:
                    ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
                else:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception:
                log.exception("Handler error; sending to DLQ")
                ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
        return _cb
    else:
        log.info("Using 4-arg handler(ch, method, props, body)")
        return with_ack(handler)

def consume(
    broker_url: str,
    queue: str,
    handler,
    *,
    auto_topology: bool = True,
    exchange: str = "events",
) -> None:
    if auto_topology:
        ensure_topology(broker_url, exchange=exchange, queues=[queue])
    conn, ch = _open_channel(broker_url)
    try:
        ch.basic_consume(queue=queue, on_message_callback=_to_callback(handler))
        log.info("Consuming from %s ...", queue)
        ch.start_consuming()
    finally:
        try:
            ch.stop_consuming()
        except Exception:
            pass
        conn.close()

# ---------- compatibility shim ----------

class Broker:

    def __init__(self, broker_url: str, exchange: str = "events"):
        self.url = broker_url
        self.exchange = exchange

    def ensure_topology(self, queues: list[str] | None = None):
        ensure_topology(self.url, exchange=self.exchange, queues=queues)

    def publish(self, routing_key: str, payload, *, content_type: str = "application/json", persistent: bool = True):
        publish(self.url, routing_key, payload, exchange=self.exchange, content_type=content_type, persistent=persistent)
