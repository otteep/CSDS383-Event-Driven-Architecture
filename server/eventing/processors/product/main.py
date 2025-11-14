# processors/product/main.py
import os
import json
import logging
import threading
from typing import Dict, Any

from common.broker import ensure_topology, consume, publish
from common.http_client import post_json
from common.utils import seen, remember
from common.logging_conf import setup_logging

# NEW: strict event validation helpers
from processors.common.event_validation import (
    validate_envelope,
    validate_product_payload,
    validate_supplier_valid_payload,
)

# ---- config ----
setup_logging("product-processor")
log = logging.getLogger("product-processor")

BROKER_URL           = os.getenv("BROKER_URL", "amqp://guest:guest@rabbitmq:5672/")
EXCHANGE             = os.getenv("EXCHANGE", "events")
PRODUCT_BASE_URL     = os.getenv("PRODUCT_BASE_URL", "http://product-service:8002/products")
CONSUME_PRODUCT_INIT = os.getenv("CONSUME_PRODUCT_INIT", "true").lower() == "true"

# DLQ names (can be bound in topology)
DLQ_INITIALIZING = os.getenv("DLQ_INITIALIZING", "initializing.dlq")
DLQ_PRODUCT      = os.getenv("DLQ_PRODUCT", "product.dlq")

# queues (bindings are created by scripts/topology.py)
Q_SUPPLIER_VALID = os.getenv("SUPPLIER_VALID_QUEUE", "supplier.valid.q")
Q_PRODUCT_INIT   = os.getenv("PRODUCT_INIT_QUEUE",   "product.init.q")


def _create_product(body: Dict[str, Any]) -> Dict[str, Any]:
    """POST to product service and return the JSON response"""
    return post_json(PRODUCT_BASE_URL, body)


def _parse_msg(body) -> Dict[str, Any]:
    """Accept bytes/str/dict and return a dict."""
    if isinstance(body, (bytes, bytearray)):
        return json.loads(body.decode("utf-8"))
    if isinstance(body, str):
        return json.loads(body)
    if isinstance(body, dict):
        return body
    raise TypeError(f"Unsupported message type: {type(body)}")


def _publish_to_dlq(routing_key: str, payload: Any) -> None:
    try:
        publish(BROKER_URL, routing_key, payload, exchange=EXCHANGE)
        log.warning("Published message to DLQ %s", routing_key)
    except Exception:
        log.exception("Failed to publish to DLQ %s", routing_key)


def handle_supplier_valid(raw_body) -> None:
    try:
        msg = _parse_msg(raw_body)
    except Exception as e:
        log.error("Malformed message JSON: %s", e)
        _publish_to_dlq(DLQ_INITIALIZING, raw_body)
        return

    try:
        validate_envelope(msg, expected_event="supplier.valid")
    except Exception as e:
        log.error("Envelope validation failed: %s", e)
        _publish_to_dlq(DLQ_INITIALIZING, msg)
        return

    eid = msg.get("event_id")
    if seen(eid):
        return

    try:
        payload = validate_supplier_valid_payload(msg["payload"])
    except Exception as e:
        log.error("supplier.valid payload invalid: %s", e)
        _publish_to_dlq(DLQ_PRODUCT, msg)
        return

    supplier_id = payload["supplier_id"]
    product     = payload["product"]

    body = {
        "name":        product["name"],
        "description": product.get("description", ""),
        "quantity":    int(product["quantity"]),
        "price":       float(product["price"]),
        "supplier_ids": [supplier_id] if supplier_id else [],
        "category_ids": product.get("category_ids", []),
        "image_ids":    product.get("image_ids", []),
    }
    try:
        resp = _create_product(body)
        log.info("Created product %s from supplier.valid", resp.get("id"))
        remember(eid, payload)
    except Exception as e:
        log.exception("Failed creating product from supplier.valid: %s", e)
        _publish_to_dlq(DLQ_PRODUCT, msg)


def handle_product_init(raw_body) -> None:
    try:
        msg = _parse_msg(raw_body)
    except Exception as e:
        log.error("Malformed message JSON: %s", e)
        _publish_to_dlq(DLQ_INITIALIZING, raw_body)
        return

    try:
        validate_envelope(msg, expected_event="product.init")
    except Exception as e:
        log.error("Envelope validation failed: %s", e)
        _publish_to_dlq(DLQ_INITIALIZING, msg)
        return

    eid = msg.get("event_id")
    if seen(eid):
        return

    try:
        product = validate_product_payload(msg["payload"])
    except Exception as e:
        log.error("product.init payload invalid: %s", e)
        _publish_to_dlq(DLQ_PRODUCT, msg)
        return

    body = {
        "name":         product["name"],
        "description":  product.get("description", ""),
        "quantity":     int(product["quantity"]),
        "price":        float(product["price"]),
        "supplier_ids": product.get("supplier_ids", []),
        "category_ids": product.get("category_ids", []),
        "image_ids":    product.get("image_ids", []),
    }
    try:
        resp = _create_product(body)
        log.info("Created product %s from product.init", resp.get("id"))
        remember(eid, product)
    except Exception as e:
        log.exception("Failed creating product from product.init: %s", e)
        _publish_to_dlq(DLQ_PRODUCT, msg)


def _consume_supplier_valid():
    consume(BROKER_URL, Q_SUPPLIER_VALID, handler=handle_supplier_valid, exchange=EXCHANGE)


def _consume_product_init():
    consume(BROKER_URL, Q_PRODUCT_INIT,   handler=handle_product_init, exchange=EXCHANGE)


def main():
    queues = [Q_SUPPLIER_VALID]
    if CONSUME_PRODUCT_INIT:
        queues.append(Q_PRODUCT_INIT)

    ensure_topology(BROKER_URL, exchange=EXCHANGE, queues=queues)

    t1 = threading.Thread(target=_consume_supplier_valid, daemon=True)
    t1.start()

    if CONSUME_PRODUCT_INIT:
        t2 = threading.Thread(target=_consume_product_init, daemon=True)
        t2.start()

    t1.join()


if __name__ == "__main__":
    main()
