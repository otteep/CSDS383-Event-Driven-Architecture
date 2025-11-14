from __future__ import annotations
from typing import Any, Dict, Iterable

ALLOWED_EVENTS: set[str] = {
    "supplier.init",
    "supplier.valid",
    "product.init",
}

def _require_keys(obj: Dict[str, Any], keys: Iterable[str], where: str) -> None:
    missing = [k for k in keys if k not in obj]
    if missing:
        raise ValueError(f"Missing key(s) {missing} in {where}")

def validate_envelope(msg: Dict[str, Any], expected_event: str) -> Dict[str, Any]:
    """
    Validates the generic event envelope and ensures the event is what this
    handler expects. Returns the message unchanged if valid.
    """
    if expected_event not in ALLOWED_EVENTS:
        raise ValueError(f"Unknown expected_event '{expected_event}'")

    _require_keys(msg, ["event_id", "payload"], "event envelope")
    if not isinstance(msg["payload"], dict):
        raise ValueError("payload must be a dict")

    # require a non-empty event_id (publisher should provide one)
    if not msg.get("event_id"):
        raise ValueError("event_id missing or empty in envelope")

    # If a producer set an 'event' or 'routing_key', make sure it matches.
    advertised = msg.get("event") or msg.get("routing_key")
    if advertised and advertised != expected_event:
        raise ValueError(f"Unexpected event type '{advertised}', expected '{expected_event}'")

    return msg

def validate_supplier_init_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    sup = payload.get("supplier") or {}
    prods = payload.get("products") or []

    if not sup.get("name") or not str(sup.get("name")).strip():
        raise ValueError("supplier.name missing")
    contact = sup.get("contact")
    if not isinstance(contact, str) or not contact.strip():
        raise ValueError("supplier.contact invalid")

    if not isinstance(prods, list) or not prods:
        raise ValueError("products must be a non-empty list")

    # coerce and validate each product; return normalized products
    normalized = []
    for i, p in enumerate(prods):
        if not p.get("name") or not str(p.get("name")).strip():
            raise ValueError(f"product[{i}].name missing")
        try:
            qty = int(p.get("quantity"))
        except Exception:
            raise ValueError(f"product[{i}].quantity invalid")
        if qty < 0:
            raise ValueError(f"product[{i}].quantity invalid")
        try:
            price = float(p.get("price"))
        except Exception:
            raise ValueError(f"product[{i}].price invalid")
        if price <= 0:
            raise ValueError(f"product[{i}].price invalid")

        normalized.append({
            "name": str(p.get("name")).strip(),
            "description": str(p.get("description") or "").strip(),
            "quantity": qty,
            "price": price,
            "category_ids": p.get("category_ids", []),
            "image_ids": p.get("image_ids", []),
        })

    return {
        "supplier": {
            "name": str(sup.get("name")).strip(),
            "contact": str(contact).strip(),
        },
        "products": normalized,
    }

def validate_product_payload(product: Dict[str, Any]) -> Dict[str, Any]:
    # Accept product shapes with name/quantity/price and coerce types
    if not product.get("name") or not str(product.get("name")).strip():
        raise ValueError("product.name missing")
    try:
        qty = int(product.get("quantity"))
    except Exception:
        raise ValueError("quantity invalid")
    if qty < 0:
        raise ValueError("quantity invalid")
    try:
        price = float(product.get("price"))
    except Exception:
        raise ValueError("price invalid")
    if price <= 0:
        raise ValueError("price invalid")

    return {
        "name": str(product.get("name")).strip(),
        "description": str(product.get("description") or "").strip(),
        "quantity": qty,
        "price": price,
        "supplier_ids": product.get("supplier_ids", []),
        "category_ids": product.get("category_ids", []),
        "image_ids": product.get("image_ids", []),
    }

def validate_supplier_valid_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    _require_keys(payload, ["supplier_id", "product"], "supplier.valid payload")
    if not payload.get("supplier_id"):
        raise ValueError("supplier_id missing in supplier.valid payload")

    product = payload["product"]
    validated_product = validate_product_payload(product)
    return {
        "supplier_id": payload["supplier_id"],
        "product": validated_product,
    }