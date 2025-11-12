import logging
import requests
from typing import Iterable
from config import settings

log = logging.getLogger("supplier.sync")

def _safe_request(method: str, url: str, json=None):
    try:
        resp = requests.request(method=method, url=url, json=json, timeout=settings.HTTP_TIMEOUT)
        if resp.status_code >= 400:
            log.warning("Sync %s %s -> %s %s", method, url, resp.status_code, resp.text)
    except Exception as e:
        log.warning("Sync exception %s %s: %s", method, url, e)

# Product service contract used for bidirectional consistency:
#   POST   /products/{pid}/suppliers/{sid}
#   DELETE /products/{pid}/suppliers/{sid}

def sync_add_supplier_to_products(supplier_id: str, product_ids: Iterable[str]) -> None:
    for pid in product_ids or []:
        _safe_request("POST", f"{settings.PRODUCT_BASE_URL}/{pid}/suppliers/{supplier_id}")

def sync_remove_supplier_from_products(supplier_id: str, product_ids: Iterable[str]) -> None:
    for pid in product_ids or []:
        _safe_request("DELETE", f"{settings.PRODUCT_BASE_URL}/{pid}/suppliers/{supplier_id}")

def sync_replace_supplier_products(supplier_id: str, old_ids: Iterable[str], new_ids: Iterable[str]) -> None:
    old_set, new_set = set(old_ids or []), set(new_ids or [])
    to_add = new_set - old_set
    to_remove = old_set - new_set
    if to_add:
        sync_add_supplier_to_products(supplier_id, to_add)
    if to_remove:
        sync_remove_supplier_from_products(supplier_id, to_remove)
