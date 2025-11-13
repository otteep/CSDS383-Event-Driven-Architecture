import json
import os
import time
import shutil
from pathlib import Path
from uuid import uuid4
from typing import Any, Dict, List
from contextlib import contextmanager

import pika  # direct publish

from common.logging_conf import get_logger

# -------------------------
# Config
# -------------------------
BROKER_URL = os.getenv("BROKER_URL", "amqp://guest:guest@rabbitmq:5672/%2f")
INPUT_DIR  = Path(os.getenv("INPUT_DIR", "/data/events"))
BAD_DIR    = INPUT_DIR / "_bad"
EXCHANGE   = os.getenv("EXCHANGE", "events")
ROUTING_KEY = os.getenv("ROUTING_KEY", "supplier.init")  # matches your queues

logger = get_logger("publisher")


# -------------------------
# Pika helpers
# -------------------------
def _conn_params(url: str) -> pika.URLParameters:
    params = pika.URLParameters(url)
    params.heartbeat = 30
    params.blocked_connection_timeout = 30
    return params

@contextmanager
def open_channel(url: str, exchange: str):
    conn = pika.BlockingConnection(_conn_params(url))
    ch = conn.channel()
    # declare exchange to avoid race with processors
    ch.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)
    try:
        yield ch
    finally:
        try:
            ch.close()
        finally:
            conn.close()


# -------------------------
# Coercion helpers
# -------------------------
def coerce_int(val, default=0) -> int:
    try:
        return int(val)
    except Exception:
        return default

def coerce_float(val, default=None) -> float | None:
    try:
        if val is None:
            return default
        return float(val)
    except Exception:
        return default


# -------------------------
# Event mapping (kept as-is; processor normalizes)
# -------------------------
def to_supplier_init(raw: Dict[str, Any], source_name: str) -> Dict[str, Any]:
    """Map pre-generated file -> publisher-friendly event (processor will normalize)."""
    sup = (raw or {}).get("supplier") or {}

    products_in = sup.get("products") or []
    products_out: List[Dict[str, Any]] = []

    for idx, p in enumerate(products_in):
        name = (p.get("product_name") or "").strip()
        if not name:
            logger.warning("publisher :: %s product[%s] skipped: empty name", source_name, idx)
            continue

        qty = coerce_int(p.get("product_quantity"), default=0)
        price = coerce_float(p.get("product_price"), default=None)
        if price is None:
            logger.warning(
                "publisher :: %s product[%s] skipped: bad price %r",
                source_name, idx, p.get("product_price")
            )
            continue

        desc = (p.get("product_description") or "").strip()
        products_out.append(
            {
                "product_name": name,
                "product_description": desc,
                "product_quantity": max(0, qty),
                "product_price": price,
            }
        )

    event = {
        "event_id": str(uuid4()),
        "timestamp": int(time.time() * 1000),
        "source": source_name,
        "supplier": {
            "supplier_name": (sup.get("supplier_name") or "").strip(),
            "supplier_contact": (sup.get("supplier_contact") or "").strip(),
            "products": products_out,
        },
    }
    return event


# -------------------------
# Main publish loop
# -------------------------
def publish_all():
    BAD_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted([p for p in INPUT_DIR.glob("*.json") if p.is_file()])
    if not files:
        logger.info("publisher :: no event files in %s", INPUT_DIR)
        return

    properties = pika.BasicProperties(
        content_type="application/json",
        content_encoding="utf-8",
        delivery_mode=2,  # persistent
    )

    with open_channel(BROKER_URL, EXCHANGE) as ch:
        for f in files:
            try:
                raw = json.loads(f.read_text(encoding="utf-8"))
                payload = to_supplier_init(raw, f.name)

                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                ch.basic_publish(
                    exchange=EXCHANGE,
                    routing_key=ROUTING_KEY,  # 'supplier.init'
                    body=body,
                    properties=properties,
                    mandatory=False,
                )
                logger.info(
                    "publisher :: Published %s from %s (event_id=%s)",
                    ROUTING_KEY, f.name, payload["event_id"]
                )

            except json.JSONDecodeError as e:
                logger.error("publisher :: %s invalid JSON: %s", f.name, e)
                shutil.move(str(f), BAD_DIR / f.name)
            except Exception as e:
                logger.exception("publisher :: %s failed: %s", f.name, e)
                shutil.move(str(f), BAD_DIR / f.name)


if __name__ == "__main__":
    publish_all()
