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

# -------------------------
# Logging / Config
# -------------------------
setup_logging("supplier-processor")
log = logging.getLogger("supplier-processor")

# Use %2f for the default root vhost to match other services
BROKER_URL = os.getenv("BROKER_URL", "amqp://guest:guest@rabbitmq:5672/%2f")
EXCHANGE = os.getenv("EXCHANGE", "events")

SUPPLIER_BASE_URL = os.getenv(
    "SUPPLIER_BASE_URL",
    "http://supplier-service:8001/suppliers"
)

# Queues / routing keys (bindings are created by scripts/topology.py)
Q_SUPPLIER_INIT   = os.getenv("SUPPLIER_INIT_QUEUE",  "supplier.init.q")
RK_SUPPLIER_VALID = os.getenv("SUPPLIER_VALID_RK",    "supplier.valid")
Q_SUPPLIER_VALID  = os.getenv("SUPPLIER_VALID_QUEUE", "supplier.valid.q")  # declared for downstream consumers


# -------------------------
# Helpers: parse & normalize
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
    The processor expects:
      {
        "event_id": "...",
        "correlation_id": "...", (optional)
        "payload": {
          "supplier": {"name": "...", "contact": "..."},
          "products": [{"name": "...", "description": "", "quantity": 0, "price": 0.0, "category_ids": [], "image_ids": []}]
        }
      }

    Accepts both:
      A) already in 'payload' shape
      B) publisher shapes:
         - publisher/main.py -> top-level keys: supplier_name, supplier_contact, products with product_* fields
         - publisher/app.py  -> arbitrary doc; if it already has routing_key + payload, we use payload
    """
    if "payload" in msg and isinstance(msg["payload"], dict):
        # Best case: already correct.
        return msg

    # Handle publisher/main.py shape:
    # {
    #   "event_id": "...",
    #   "timestamp": ...,
    #   "source": "...",
    #   "supplier": {
    #       "supplier_name": "...",
    #       "supplier_contact": "...",
    #       "products": [
    #         {"product_name": "...", "product_description": "...", "product_quantity": 1, "product_price": 9.99}
    #       ]
    #   }
    # }
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
        }

    # Handle publisher/app.py shape if someone already included payload directly
    # or if the doc is already correct but missing 'payload' key.
    # Try to lift top-level supplier/products into payload if they look normalized.
    if "supplier" in msg and "products" in (msg.get("payload") or {}):
        # unlikely mixed shape; just return as-is and let validator throw if wrong
        return msg

    # As a last resort, pass through (validator will raise a clear error)
    return msg


# -------------------------
# Validation
# -------------------------
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



# -------------------------
# Handler
# -------------------------
def handle_supplier_init(body) -> None:
    # 1) parse
    raw = _parse_msg(body)
    # 2) normalize into expected 'payload' shape (supports both publisher formats)
    msg = _normalize_to_payload_shape(raw)

    eid = msg.get("event_id")
    corr = msg.get("correlation_id")

    # idempotency
    if seen(eid):
        return

    payload = validate_supplier_init(msg)

    # Create supplier via microservice
    resp = post_json(SUPPLIER_BASE_URL, {
        "name": payload["supplier"]["name"],
        "contact": payload["supplier"]["contact"],
        "product_ids": []
    })
    supplier_id = resp["id"]
    log.info("Created supplier %s corr=%s", supplier_id, corr)

    # Re-emit supplier.valid for EACH product (supplier_id + original product details)
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
                    "image_ids": p.get("image_ids", []),
                },
            },
            correlation_id=corr,
        )
        publish(BROKER_URL, RK_SUPPLIER_VALID, out.to_dict(), exchange=EXCHANGE)

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
    # common.consume will pass raw bytes; our handler parses/normalizes
    consume(BROKER_URL, Q_SUPPLIER_INIT, handler=handle_supplier_init, exchange=EXCHANGE)

if __name__ == "__main__":
    main()
