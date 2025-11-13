# processors/product/main.py
import os, logging, threading
from typing import Dict, Any

from common.broker import ensure_topology, consume
from common.http_client import post_json
from common.utils import seen, remember
from common.logging_conf import setup_logging

# ---- config ----
setup_logging("product-processor")
log = logging.getLogger("product-processor")

BROKER_URL           = os.getenv("BROKER_URL", "amqp://guest:guest@rabbitmq:5672/")
EXCHANGE             = os.getenv("EXCHANGE", "events")
PRODUCT_BASE_URL     = os.getenv("PRODUCT_BASE_URL", "http://product-service:8002/products")
CONSUME_PRODUCT_INIT = os.getenv("CONSUME_PRODUCT_INIT", "true").lower() == "true"

# queues (bindings are created by scripts/topology.py)
Q_SUPPLIER_VALID = os.getenv("SUPPLIER_VALID_QUEUE", "supplier.valid.q")
Q_PRODUCT_INIT   = os.getenv("PRODUCT_INIT_QUEUE",   "product.init.q")

def validate_product(doc: Dict[str, Any]) -> Dict[str, Any]:
    if not doc.get("name"):
        raise ValueError("product.name missing")
    if doc.get("quantity") is None or int(doc["quantity"]) < 0:
        raise ValueError("quantity invalid")
    if doc.get("price") is None or float(doc["price"]) <= 0:
        raise ValueError("price invalid")
    return doc

def handle_supplier_valid(msg: Dict[str, Any]) -> None:
    eid = msg.get("event_id")
    if seen(eid): return

    payload = msg.get("payload") or {}
    supplier_id = payload.get("supplier_id")
    product = validate_product(payload.get("product") or {})

    body = {
        "name": product["name"],
        "description": product.get("description", ""),
        "quantity": int(product["quantity"]),
        "price": float(product["price"]),
        "supplier_ids": [supplier_id] if supplier_id else [],
        "category_ids": product.get("category_ids", []),
        "image_ids": product.get("image_ids", []),
    }
    resp = post_json(PRODUCT_BASE_URL, body)
    log.info("Created product %s from supplier.valid", resp["id"])
    remember(eid, payload)

def handle_product_init(msg: Dict[str, Any]) -> None:
    eid = msg.get("event_id")
    if seen(eid): return

    product = validate_product(msg.get("payload") or {})
    body = {
        "name": product["name"],
        "description": product.get("description", ""),
        "quantity": int(product["quantity"]),
        "price": float(product["price"]),
        "supplier_ids": product.get("supplier_ids", []),
        "category_ids": product.get("category_ids", []),
        "image_ids": product.get("image_ids", []),
    }
    resp = post_json(PRODUCT_BASE_URL, body)
    log.info("Created product %s from product.init", resp["id"])
    remember(eid, product)

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
