# processors/supplier/main.py
import os
import json
import logging
from typing import Dict, Any, List

from common.broker import ensure_topology, consume, publish
from common.events import Event
from common.http_client import post_json
from common.utils import is_email, seen, remember
from common.logging_conf import setup_logging

# NEW: strict event validation helpers
from processors.common.event_validation import (
    validate_envelope,
    validate_supplier_init_payload,
)

# -------------------------
# Logging / Config
# -------------------------
setup_logging("supplier-processor")
log = logging.getLogger("supplier-processor")

# Use %2f for the default root vhost to match other services
BROKER_URL = os.getenv("BROKER_URL", "amqp://guest:guest@rabbitmq:5672/%2f")
EXCHANGE   = os.getenv("EXCHANGE", "events")

SUPPLIER_BASE_URL = os.getenv(
    "SUPPLIER_BASE_URL",
    "http://supplier-service:8001/suppliers"
)

# Queues / routing keys (bindings are created by scripts/topology.py)
Q_SUPPLIER_INIT   = os.getenv("SUPPLIER_INIT_QUEUE",  "supplier.init.q")
RK_SUPPLIER_VALID = os.getenv("SUPPLIER_VALID_RK",    "supplier.valid")
Q_SUPPLIER_VALID  = os.getenv("SUPPLIER_VALID_QUEUE", "supplier.valid.q")  # declared for downstream consumers

# DLQ names (optional - ensure topology binds these or set in env)
DLQ_INITIALIZING = os.getenv("DLQ_INITIALIZING", "initializing.dlq")
DLQ_SUPPLIER     = os.getenv("DLQ_SUPPLIER", "supplier.dlq")


# -------------------------
# Helpers: parse / normalize / dlq
# -------------------------
def _parse_msg(body) -> Dict[str, Any]:
    """Accept bytes/str/dict and return a dict."""
    if isinstance(body, (bytes, bytearray)):
        return json.loads(body.decode("utf-8"))
    if isinstance(body, str):
        return json.loads(body)
    if isinstance(body, dict):
        return body
    raise TypeError(f"Unsupported message type: {type(body)}")


def _normalize_to_payload_shape(msg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize multiple publisher shapes into canonical envelope with 'payload'.
    """
    if "payload" in msg and isinstance(msg["payload"], dict):
        return msg

    sup = (msg.get("supplier") or {})
    if sup and ("supplier_name" in sup or "supplier_contact" in sup):
        products_in = sup.get("products") or []
        products_out: List[Dict[str, Any]] = []
        for p in products_in:
            products_out.append({
                "name": (p.get("product_name") or "").strip(),
                "description": (p.get("product_description") or "").strip(),
                "quantity": int(p.get("product_quantity") or 0),
                "price": float(p.get("product_price") or 0.0),
                "category_ids": p.get("category_ids", []),
                "image_ids": p.get("image_ids", []),
            })
        return {
            "event_id": msg.get("event_id"),
            "correlation_id": msg.get("correlation_id"),
            "payload": {
                "supplier": {
                    "name": (sup.get("supplier_name") or "").strip(),
                    "contact": (sup.get("supplier_contact") or "").strip(),
                },
                "products": products_out,
            },
            # stamp event name for downstream debugging
            "event": "supplier.init",
        }

    # last resort: pass through; envelope+payload validators will raise if wrong
    msg.setdefault("event", "supplier.init")
    return msg


def _publish_to_dlq(routing_key: str, payload: Any) -> None:
    try:
        publish(BROKER_URL, routing_key, payload, exchange=EXCHANGE)
        log.warning("Published message to DLQ %s", routing_key)
    except Exception:
        log.exception("Failed to publish to DLQ %s", routing_key)


# -------------------------
# Handler
# -------------------------
def handle_supplier_init(body) -> None:
    # 1) parse raw
    try:
        raw = _parse_msg(body)
    except Exception as e:
        log.error("Malformed message JSON: %s", e)
        _publish_to_dlq(DLQ_INITIALIZING, body)
        return

    # 2) normalize to canonical shape
    msg = _normalize_to_payload_shape(raw)

    # 3) strict envelope check: this handler only accepts supplier.init
    try:
        validate_envelope(msg, expected_event="supplier.init")
    except Exception as e:
        log.error("Envelope validation failed: %s", e)
        _publish_to_dlq(DLQ_INITIALIZING, msg)
        return

    eid  = msg.get("event_id")
    corr = msg.get("correlation_id")

    # 4) idempotency
    if seen(eid):
        return

    # 5) payload validation
    try:
        payload = validate_supplier_init_payload(msg["payload"])
    except Exception as e:
        log.error("supplier.init payload invalid: %s", e)
        _publish_to_dlq(DLQ_SUPPLIER, msg)
        return

    # 6) Create supplier via microservice
    try:
        resp = post_json(SUPPLIER_BASE_URL, {
            "name":    payload["supplier"]["name"],
            "contact": payload["supplier"]["contact"],
            "product_ids": []
        })
    except Exception as e:
        log.exception("Failed creating supplier via supplier-service: %s", e)
        _publish_to_dlq(DLQ_SUPPLIER, msg)
        return

    supplier_id = resp.get("id")
    log.info("Created supplier %s corr=%s", supplier_id, corr)

    # 7) Re-emit supplier.valid for EACH product (supplier_id + original product details)
    for p in payload["products"]:
        out = Event.new(
            RK_SUPPLIER_VALID,
            payload={
                "supplier_id": supplier_id,
                "product": {
                    "name": p["name"],
                    "description": p.get("description", ""),
                    "quantity": int(p["quantity"]),
                    "price": float(p["price"]),
                    "category_ids": p.get("category_ids", []),
                    "image_ids":  p.get("image_ids",  []),
                },
            },
            correlation_id=corr,
        )
        try:
            publish(BROKER_URL, RK_SUPPLIER_VALID, out.to_dict(), exchange=EXCHANGE)
        except Exception:
            log.exception("Failed publishing supplier.valid for supplier %s", supplier_id)
            # publish original message to DLQ to avoid message loss / reprocessing loops
            _publish_to_dlq(DLQ_SUPPLIER, msg)

    remember(eid, payload)


# -------------------------
# Main
# -------------------------
def main():
    # Declare queues locally for safety (bindings handled in topology)
    ensure_topology(
        BROKER_URL,
        exchange=EXCHANGE,
        queues=[Q_SUPPLIER_INIT, Q_SUPPLIER_VALID]
    )

    log.info("Consuming from %s ...", Q_SUPPLIER_INIT)
    consume(BROKER_URL, Q_SUPPLIER_INIT, handler=handle_supplier_init, exchange=EXCHANGE)

if __name__ == "__main__":
    main()
